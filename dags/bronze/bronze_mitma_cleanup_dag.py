"""
DAG for cleaning up MITMA Bronze layer data.
Allows cleaning files from RustFS bucket and optionally truncating Bronze tables.

Usage:
    Trigger with parameters:
    - cleanup_rustfs: If True, deletes files from RustFS bucket
    - cleanup_tables: If True, truncates Bronze tables (use with caution!)
    - dataset: Which dataset to clean ('all', 'od', 'people_day', 'overnight_stay')
    - zone_type: Which zone type to clean ('all', 'municipios', 'distritos', 'gau')
    
WARNING: This DAG will permanently delete files and/or table data!
"""

from datetime import datetime, timedelta
from typing import List, Dict, Any

from airflow import DAG
from airflow.models import Param, Variable
from airflow.decorators import task, task_group
from airflow.providers.standard.operators.empty import EmptyOperator


@task
def list_rustfs_files(**context) -> Dict[str, Any]:
    """
    Lists files in RustFS bucket based on bronze_mitma* tables.
    For each table, searches for files in main/TABLE_NAME/ prefix.
    """
    from airflow.providers.amazon.aws.hooks.s3 import S3Hook
    from utils.utils import get_ducklake_connection
    
    # Get bucket name from Airflow Variable
    rustfs_bucket = Variable.get('RUSTFS_BUCKET', default_var='mitma')
    
    # Get params from context
    params = context.get('params', {})
    dataset = params.get('dataset', 'all')
    zone_type = params.get('zone_type', 'all')
    
    print(f"[CLEANUP] Listing files in bucket '{rustfs_bucket}'")
    
    con = get_ducklake_connection()
    
    # Get all bronze_mitma tables (bronze_mitma_loquesea)
    tables_query = """
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_name LIKE 'bronze_mitma_%'
           OR table_name = 'bronze_mitma'
        ORDER BY table_name
    """
    
    tables_df = con.execute(tables_query).fetchdf()
    table_names = tables_df['table_name'].tolist()
    
    # Apply filters if needed
    if dataset != 'all':
        table_names = [t for t in table_names if f'_{dataset}_' in t or t.endswith(f'_{dataset}')]
    
    if zone_type != 'all':
        table_names = [t for t in table_names if t.endswith(f'_{zone_type}')]
    
    if not table_names:
        return {
            'bucket': rustfs_bucket,
            'files': [],
            'total_count': 0,
            'total_size_mb': 0,
            'tables': []
        }
    
    # Now list files for each table in main/TABLE_NAME/
    s3_hook = S3Hook(aws_conn_id='rustfs_s3_conn')
    
    try:
        if not s3_hook.check_for_bucket(rustfs_bucket):
            print(f"[CLEANUP] Bucket '{rustfs_bucket}' does not exist")
            return {
                'bucket': rustfs_bucket,
                'files': [],
                'total_count': 0,
                'total_size_mb': 0,
                'tables': table_names
            }
    except Exception as e:
        print(f"[CLEANUP] Error checking bucket: {e}")
        return {
            'bucket': rustfs_bucket,
            'files': [],
            'total_count': 0,
            'total_size_mb': 0,
            'tables': table_names,
            'error': str(e)
        }
    
    s3_client = s3_hook.get_conn()
    paginator = s3_client.get_paginator('list_objects_v2')
    
    all_files = []
    total_size = 0
    files_by_table = {}
    
    # For each table, list files in main/TABLE_NAME/
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
                    
                    # Skip directories
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
            print(f"[CLEANUP] Error listing files for table '{table_name}': {e}")
            files_by_table[table_name] = 0
    
    print(f"[CLEANUP] Found {len(all_files)} files ({round(total_size / (1024 * 1024), 2)} MB) in {len(table_names)} tables")
    
    # Group by table for summary
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
        'files_by_table': files_by_table
    }


@task
def delete_rustfs_files(
    file_info: Dict[str, Any] = None
) -> Dict[str, Any]:
    """
    Deletes files from RustFS bucket.
    """
    from airflow.providers.amazon.aws.hooks.s3 import S3Hook
    
    # Get bucket name from Airflow Variable or file_info
    rustfs_bucket = Variable.get('RUSTFS_BUCKET', default_var='mitma')
    
    if file_info is None:
        file_info = {}
    else:
        # Use bucket from file_info if available (more accurate)
        rustfs_bucket = file_info.get('bucket', rustfs_bucket)
    
    files = file_info.get('files', [])
    total_count = file_info.get('total_count', 0)
    total_size_mb = file_info.get('total_size_mb', 0)
    
    if not files:
        return {
            'status': 'skipped',
            'reason': 'no_files',
            'deleted': 0
        }
    
    print(f"[CLEANUP] Deleting {total_count} files ({total_size_mb} MB)")
    
    s3_hook = S3Hook(aws_conn_id='rustfs_s3_conn')
    s3_client = s3_hook.get_conn()
    
    deleted = 0
    errors = []
    
    # Delete in batches of 1000 (S3 limit)
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
            print(f"[CLEANUP] Error deleting batch: {e}")
            errors.append({'batch': i // batch_size, 'error': str(e)})
    
    print(f"[CLEANUP] Deleted {deleted}/{total_count} files")
    
    if errors:
        print(f"[CLEANUP] {len(errors)} errors occurred")
    
    return {
        'status': 'success' if not errors else 'partial',
        'deleted': deleted,
        'freed_mb': total_size_mb,
        'errors': errors[:10] if errors else []
    }


@task
def list_bronze_tables(**context) -> Dict[str, Any]:
    """
    Lists all bronze_mitma_* tables and their row counts.
    """
    from utils.utils import get_ducklake_connection
    
    # Get params from context
    params = context.get('params', {})
    dataset = params.get('dataset', 'all')
    zone_type = params.get('zone_type', 'all')
    
    con = get_ducklake_connection()
    
    # Get all bronze_mitma_* tables (bronze_mitma_loquesea)
    tables_query = """
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_name LIKE 'bronze_mitma_%'
           OR table_name = 'bronze_mitma'
        ORDER BY table_name
    """
    
    tables_df = con.execute(tables_query).fetchdf()
    table_names = tables_df['table_name'].tolist()
    
    # Apply filters if needed
    if dataset != 'all':
        table_names = [t for t in table_names if f'_{dataset}_' in t or t.endswith(f'_{dataset}')]
    
    if zone_type != 'all':
        table_names = [t for t in table_names if t.endswith(f'_{zone_type}')]
    
    # Get row counts for each table
    tables = []
    for table_name in table_names:
        try:
            count = con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
            tables.append({
                'table_name': table_name,
                'row_count': count
            })
        except Exception as e:
            print(f"[CLEANUP] Error getting count for {table_name}: {e}")
            tables.append({
                'table_name': table_name,
                'row_count': 0
            })
    
    total_rows = sum(t['row_count'] for t in tables)
    
    print(f"[CLEANUP] Found {len(tables)} tables with {total_rows:,} total rows")
    
    return {
        'tables': tables,
        'total_tables': len(tables),
        'total_rows': total_rows
    }


@task
def truncate_bronze_tables(
    table_info: Dict[str, Any] = None,
    **context
) -> Dict[str, Any]:
    """
    Truncates Bronze MITMA tables using a single SQL statement executed via Cloud Run.
    
    WARNING: This permanently deletes all data in the tables!
    """
    from utils.gcp import execute_sql_or_cloud_run
    
    if table_info is None:
        table_info = {}
    
    tables = table_info.get('tables', [])
    total_rows = table_info.get('total_rows', 0)
    
    if not tables:
        return {
            'status': 'skipped',
            'reason': 'no_tables',
            'truncated': 0
        }
    
    # Build single SQL statement with all DELETE statements
    table_names = [t['table_name'] for t in tables]
    delete_statements = [f"DELETE FROM {table_name};" for table_name in table_names]
    sql_query = "\n".join(delete_statements)
    
    print(f"[CLEANUP] ⚠️  TRUNCATING {len(tables)} tables ({total_rows:,} rows)")
    
    try:
        # Execute all DELETE statements in a single call via Cloud Run
        result = execute_sql_or_cloud_run(sql_query=sql_query, **context)
        
        print(f"[CLEANUP] ✅ Truncated {len(tables)} tables")
        
        return {
            'status': 'success',
            'truncated': len(tables),
            'deleted_rows': total_rows,
            'execution_time_seconds': result.get('execution_time_seconds', 0),
            'execution_name': result.get('execution_name', 'unknown')
        }
        
    except Exception as e:
        print(f"[CLEANUP] ❌ Error truncating tables: {e}")
        return {
            'status': 'error',
            'truncated': 0,
            'deleted_rows': 0,
            'error': str(e)
        }


# ============================================================================
# DAG DEFINITION
# ============================================================================

with DAG(
    dag_id="bronze_mitma_cleanup",
    start_date=datetime(2025, 12, 1),
    schedule=None,
    catchup=False,
    tags=["bronze", "mitma", "cleanup", "maintenance"],
    params={
        "cleanup_rustfs": Param(
            type="boolean",
            default=True,
            description="Delete files from RustFS bucket"
        ),
        "cleanup_tables": Param(
            type="boolean",
            default=False,
            description="⚠️ Truncate Bronze tables (DANGER: deletes all data!)"
        ),
        "dataset": Param(
            type="string",
            default="all",
            enum=["all", "od", "people_day", "overnight_stay"],
            description="Which dataset to clean"
        ),
        "zone_type": Param(
            type="string",
            default="all",
            enum=["all", "municipios", "distritos", "gau"],
            description="Which zone type to clean"
        ),
    },
    description="Cleanup DAG for MITMA Bronze layer - RustFS files and tables",
    default_args={
        "retries": 1,
        "retry_delay": timedelta(seconds=30),
    },
    max_active_runs=1,
) as dag:
    
    start = EmptyOperator(task_id="start")
    
    # RustFS cleanup branch
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
    
    # Tables cleanup branch
    @task_group(group_id="tables_cleanup")
    def tables_cleanup_group():
        @task.branch
        def check_tables_enabled(**context):
            if context['params'].get('cleanup_tables', False):
                return "tables_cleanup.list_tables"
            return "tables_cleanup.skipped"
        
        branch = check_tables_enabled()
        
        list_tables = list_bronze_tables.override(task_id="list_tables")()
        
        truncate_tables = truncate_bronze_tables.override(task_id="truncate_tables")(
            table_info=list_tables
        )
        
        skipped = EmptyOperator(task_id="skipped")
        
        branch >> list_tables >> truncate_tables
        branch >> skipped
    
    rustfs_group = rustfs_cleanup_group()
    tables_group = tables_cleanup_group()
    
    done = EmptyOperator(
        task_id="done",
        trigger_rule="none_failed"
    )
    
    # Dependencies
    start >> [rustfs_group, tables_group]
    rustfs_group >> done
    tables_group >> done

