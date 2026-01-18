"""
DAG for cleaning up Gold layer data.
Allows cleaning files from RustFS bucket and optionally dropping Gold tables.

Usage:
    Trigger with parameters:
    - table_type: Which table type to clean ('all', 'typical_day', 'gravity_mismatch', 'zone_functional_type')
    - cleanup_rustfs: If True, deletes files from RustFS bucket
    - cleanup_tables: If True, drops Gold tables (use with caution!)
    
WARNING: This DAG will permanently delete files and/or table data!
"""

from datetime import datetime, timedelta
from typing import Dict, Any

from airflow import DAG
from airflow.models import Param
from airflow.sdk import Variable
from airflow.sdk import task, task_group
from airflow.providers.standard.operators.empty import EmptyOperator
from utils.logger import get_logger

TABLE_PATTERNS = {
    'typical_day': {
        'pattern': 'gold_typical_day%',
        'tables': ['gold_typical_day_od_hourly'],
    },
    'gravity_mismatch': {
        'pattern': 'gold_gravity%',
        'tables': ['gold_gravity_mismatch'],
    },
    'zone_functional_type': {
        'pattern': 'gold_zone_functional_type',
        'tables': ['gold_zone_functional_type'],
    },
    'all': {
        'pattern': 'gold_%',
        'tables': ['gold_typical_day_od_hourly', 'gold_gravity_mismatch', 'gold_zone_functional_type'],
    },
}

@task
def list_rustfs_files(**context) -> Dict[str, Any]:
    """Lists files in RustFS bucket for gold tables matching the selected table type. Returns dict with files, counts, and sizes."""
    from airflow.providers.amazon.aws.hooks.s3 import S3Hook
    from utils.utils import get_ducklake_connection
    
    rustfs_bucket = Variable.get('RUSTFS_BUCKET', default='mitma')
    params = context.get('params', {})
    table_type = params.get('table_type', 'all')
    
    logger = get_logger(__name__, context)
    table_config = TABLE_PATTERNS.get(table_type, TABLE_PATTERNS['all'])
    table_pattern = table_config['pattern']
    
    logger.info(f"Table type: {table_type}")
    logger.info(f"Listing files in bucket '{rustfs_bucket}' for tables matching '{table_pattern}'")
    
    con = get_ducklake_connection()
    
    tables_query = f"""
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_name LIKE '{table_pattern}'
        ORDER BY table_name
    """
    
    tables_df = con.execute(tables_query).fetchdf()
    table_names = tables_df['table_name'].tolist()
    
    logger.info(f"Found {len(table_names)} matching tables: {table_names}")
    
    if not table_names:
        return {
            'bucket': rustfs_bucket,
            'files': [],
            'total_count': 0,
            'total_size_mb': 0,
            'tables': [],
            'table_type': table_type
        }
    
    s3_hook = S3Hook(aws_conn_id='rustfs_s3_conn')
    
    try:
        if not s3_hook.check_for_bucket(rustfs_bucket):
            logger.warning(f"Bucket '{rustfs_bucket}' does not exist")
            return {
                'bucket': rustfs_bucket,
                'files': [],
                'total_count': 0,
                'total_size_mb': 0,
                'tables': table_names,
                'table_type': table_type
            }
    except Exception as e:
        logger.error(f"Error checking bucket: {e}", exc_info=True)
        return {
            'bucket': rustfs_bucket,
            'files': [],
            'total_count': 0,
            'total_size_mb': 0,
            'tables': table_names,
            'table_type': table_type,
            'error': str(e)
        }
    
    s3_client = s3_hook.get_conn()
    paginator = s3_client.get_paginator('list_objects_v2')
    
    all_files = []
    total_size = 0
    files_by_table = {}
    
    for table_name in table_names:
        prefix = f"main/{table_name}/"
        
        table_files = []
        
        try:
            for page in paginator.paginate(Bucket=rustfs_bucket, Prefix=prefix, MaxKeys=1000):
                if 'Contents' not in page:
                    continue
                
                for obj in page['Contents']:
                    key = obj['Key']
                    size = obj['Size']
                    
                    if key.endswith('/'):
                        continue
                    
                    file_info = {
                        'key': key,
                        'size_bytes': size,
                        'size_mb': round(size / (1024 * 1024), 2),
                        'last_modified': obj['LastModified'].isoformat(),
                        'table_name': table_name
                    }
                    
                    table_files.append(file_info)
                    all_files.append(file_info)
                    total_size += size
                
                if not page.get('IsTruncated', False):
                    break
            
            files_by_table[table_name] = len(table_files)
            
        except Exception as e:
            logger.warning(f"Error listing files for table '{table_name}': {e}")
            files_by_table[table_name] = 0
    
    total_size_mb = round(total_size / (1024 * 1024), 2)
    logger.info(f"Found {len(all_files)} files ({total_size_mb} MB) in {len(table_names)} tables")
    
    summary = {}
    for f in all_files:
        table = f.get('table_name', 'unknown')
        if table not in summary:
            summary[table] = {'count': 0, 'size_mb': 0}
        summary[table]['count'] += 1
        summary[table]['size_mb'] += f['size_mb']
    
    return {
        'bucket': rustfs_bucket,
        'files': all_files,
        'total_count': len(all_files),
        'total_size_mb': round(total_size / (1024 * 1024), 2),
        'summary': summary,
        'tables': table_names,
        'files_by_table': files_by_table,
        'table_type': table_type
    }


@task
def delete_rustfs_files(
    file_info: Dict[str, Any] = None
) -> Dict[str, Any]:
    """Deletes files from RustFS bucket. Returns dict with deletion status and counts."""
    from airflow.providers.amazon.aws.hooks.s3 import S3Hook
    
    rustfs_bucket = Variable.get('RUSTFS_BUCKET', default='mitma')
    
    if file_info is None:
        file_info = {}
    else:
        rustfs_bucket = file_info.get('bucket', rustfs_bucket)
    
    files = file_info.get('files', [])
    total_count = file_info.get('total_count', 0)
    total_size_mb = file_info.get('total_size_mb', 0)
    table_type = file_info.get('table_type', 'unknown')
    
    if not files:
        return {
            'status': 'skipped',
            'reason': 'no_files',
            'deleted': 0,
            'table_type': table_type
        }
    
    logger = get_logger(__name__, context)
    logger.info(f"Deleting {total_count} files ({total_size_mb} MB) for table type '{table_type}'")
    
    s3_hook = S3Hook(aws_conn_id='rustfs_s3_conn')
    s3_client = s3_hook.get_conn()
    
    deleted = 0
    errors = []
    
    batch_size = 1000
    for i in range(0, len(files), batch_size):
        batch = files[i:i + batch_size]
        
        try:
            delete_objects = [{'Key': f['key']} for f in batch]
            
            response = s3_client.delete_objects(
                Bucket=rustfs_bucket,
                Delete={'Objects': delete_objects}
            )
            
            deleted += len(response.get('Deleted', []))
            
            if 'Errors' in response:
                for err in response['Errors']:
                    errors.append({
                        'key': err['Key'],
                        'error': err['Message']
                    })
            
        except Exception as e:
            logger.error(f"Error deleting batch {i // batch_size}: {e}", exc_info=True)
            errors.append({'batch': i // batch_size, 'error': str(e)})
    
    logger.info(f"Deleted {deleted}/{total_count} files")
    
    if errors:
        logger.warning(f"{len(errors)} errors occurred during deletion")
    
    return {
        'status': 'success' if not errors else 'partial',
        'deleted': deleted,
        'freed_mb': total_size_mb,
        'errors': errors[:10] if errors else [],
        'table_type': table_type
    }


@task
def list_gold_tables(**context) -> Dict[str, Any]:
    """Lists gold tables matching the selected table type. Returns dict with table names and counts."""
    from utils.utils import get_ducklake_connection
    
    params = context.get('params', {})
    table_type = params.get('table_type', 'all')
    
    logger = get_logger(__name__, context)
    table_config = TABLE_PATTERNS.get(table_type, TABLE_PATTERNS['all'])
    table_pattern = table_config['pattern']
    
    logger.info(f"Table type: {table_type}")
    logger.info(f"Listing tables matching '{table_pattern}'")
    
    con = get_ducklake_connection()
    
    tables_query = f"""
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_name LIKE '{table_pattern}'
        ORDER BY table_name
    """
    
    tables_df = con.execute(tables_query).fetchdf()
    table_names = tables_df['table_name'].tolist()
    
    logger.info(f"Found {len(table_names)} matching tables: {table_names}")
    
    return {
        'tables': table_names,
        'total_tables': len(table_names),
        'table_type': table_type
    }


@task
def drop_gold_tables(
    table_info: Dict[str, Any] = None,
    **context
) -> Dict[str, Any]:
    """Drops Gold tables permanently. Returns dict with drop status and counts."""
    from utils.gcp import execute_sql_or_cloud_run
    
    if table_info is None:
        table_info = {}
    
    tables = table_info.get('tables', [])
    table_type = table_info.get('table_type', 'unknown')
    
    if not tables:
        return {
            'status': 'skipped',
            'reason': 'no_tables',
            'dropped': 0,
            'table_type': table_type
        }
    
    logger = get_logger(__name__, context)
    table_names = tables if isinstance(tables, list) else []
    drop_statements = [f"DROP TABLE IF EXISTS {table_name};" for table_name in table_names]
    sql_query = "\n".join(drop_statements)
    
    logger.warning(f"⚠️  DROPPING {len(table_names)} {table_type} tables (complete deletion)")
    for t in table_names:
        logger.warning(f"  - {t}")
    
    try:
        result = execute_sql_or_cloud_run(sql_query=sql_query, **context)
        
        logger.info(f"✅ Dropped {len(table_names)} tables")
        
        return {
            'status': 'success',
            'dropped': len(table_names),
            'tables_dropped': table_names,
            'execution_time_seconds': result.get('execution_time_seconds', 0),
            'execution_name': result.get('execution_name', 'unknown'),
            'table_type': table_type
        }
        
    except Exception as e:
        logger.error(f"❌ Error dropping tables: {e}", exc_info=True)
        return {
            'status': 'error',
            'dropped': 0,
            'error': str(e),
            'table_type': table_type
        }

with DAG(
    dag_id="gold_cleanup",
    start_date=datetime(2025, 12, 1),
    schedule=None,
    catchup=False,
    tags=["gold", "cleanup", "maintenance"],
    params={
        "table_type": Param(
            type="string",
            default="all",
            enum=["all", "typical_day", "gravity_mismatch", "zone_functional_type"],
            description="Which table type to clean (all, typical_day, gravity_mismatch, zone_functional_type)"
        ),
        "cleanup_rustfs": Param(
            type="boolean",
            default=True,
            description="Delete files from RustFS bucket"
        ),
        "cleanup_tables": Param(
            type="boolean",
            default=False,
            description="⚠️ Drop Gold tables (DANGER: deletes tables completely!)"
        ),
    },
    description="⚠️ Cleanup DAG for Gold layer - Manual trigger only",
    default_args={
        "retries": 1,
        "retry_delay": timedelta(seconds=30),
    },
    max_active_runs=1,
) as dag:
    
    start = EmptyOperator(task_id="start")
    
    @task_group(group_id="rustfs_cleanup")
    def rustfs_cleanup_group():
        @task.branch
        def check_rustfs_enabled(**context):
            if context['params'].get('cleanup_rustfs', True):
                return "rustfs_cleanup.list_files"
            return "rustfs_cleanup.skipped"
        
        branch = check_rustfs_enabled()
        
        list_files = list_rustfs_files.override(task_id="list_files")()
        
        delete_files = delete_rustfs_files.override(task_id="delete_files")(
            file_info=list_files
        )
        
        skipped = EmptyOperator(task_id="skipped")
        
        branch >> list_files >> delete_files
        branch >> skipped
    
    @task_group(group_id="tables_cleanup")
    def tables_cleanup_group():
        @task.branch
        def check_tables_enabled(**context):
            if context['params'].get('cleanup_tables', False):
                return "tables_cleanup.list_tables"
            return "tables_cleanup.skipped"
        
        branch = check_tables_enabled()
        
        list_tables = list_gold_tables.override(task_id="list_tables")()
        
        drop_tables = drop_gold_tables.override(task_id="drop_tables")(
            table_info=list_tables
        )
        
        skipped = EmptyOperator(task_id="skipped")
        
        branch >> list_tables >> drop_tables
        branch >> skipped
    
    rustfs_group = rustfs_cleanup_group()
    tables_group = tables_cleanup_group()
    
    done = EmptyOperator(
        task_id="done",
        trigger_rule="none_failed"
    )
    
    start >> [rustfs_group, tables_group]
    rustfs_group >> done
    tables_group >> done
