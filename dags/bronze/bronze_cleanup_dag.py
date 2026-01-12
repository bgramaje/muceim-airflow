"""
DAG for cleaning up Bronze layer data.
Allows cleaning files from RustFS bucket and optionally dropping Bronze tables.

Usage:
    Trigger with parameters:
    - source: Which source to clean ('mitma', 'ine', 'holidays')
    - cleanup_rustfs: If True, deletes files from RustFS bucket
    - cleanup_tables: If True, drops Bronze tables (use with caution!)
    - dataset: Which dataset to clean (options depend on source)
    - zone_type: Which zone type to clean (only for MITMA)
    
WARNING: This DAG will permanently delete files and/or table data!
"""

from datetime import datetime, timedelta
from typing import List, Dict, Any

from airflow import DAG
from airflow.models import Param
from airflow.sdk import Variable
from airflow.sdk import task, task_group
from airflow.providers.standard.operators.empty import EmptyOperator


# ============================================================================
# TABLE PATTERNS BY SOURCE
# ============================================================================

SOURCE_TABLE_PATTERNS = {
    'mitma': {
        'pattern': 'bronze_mitma%',
        'datasets': ['all', 'od', 'people_day', 'overnight_stay', 'zonification', 'distances'],
        'has_zone_types': True,
        'zone_types': ['all', 'municipios', 'distritos', 'gau'],
    },
    'ine': {
        'pattern': 'bronze_ine%',
        'datasets': ['all', 'empresas', 'poblacion', 'renta', 'municipios'],
        'has_zone_types': False,
        'zone_types': ['all'],
    },
    'holidays': {
        'pattern': 'bronze_holidays%',
        'datasets': ['all'],
        'has_zone_types': False,
        'zone_types': ['all'],
    },
}


# ============================================================================
# TASK DEFINITIONS
# ============================================================================

@task
def list_rustfs_files(**context) -> Dict[str, Any]:
    """
    Lists files in RustFS bucket based on bronze tables for the selected source.
    For each table, searches for files in main/TABLE_NAME/ prefix.
    """
    from airflow.providers.amazon.aws.hooks.s3 import S3Hook
    from utils.utils import get_ducklake_connection
    
    # Get bucket name from Airflow Variable
    rustfs_bucket = Variable.get('RUSTFS_BUCKET', default='mitma')
    
    # Get params from context
    params = context.get('params', {})
    source = params.get('source', 'mitma')
    dataset = params.get('dataset', 'all')
    zone_type = params.get('zone_type', 'all')
    
    source_config = SOURCE_TABLE_PATTERNS.get(source, SOURCE_TABLE_PATTERNS['mitma'])
    table_pattern = source_config['pattern']
    
    print(f"[CLEANUP] Source: {source}")
    print(f"[CLEANUP] Listing files in bucket '{rustfs_bucket}' for tables matching '{table_pattern}'")
    
    con = get_ducklake_connection()
    
    # Get all tables matching the pattern
    tables_query = f"""
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_name LIKE '{table_pattern}'
        ORDER BY table_name
    """
    
    tables_df = con.execute(tables_query).fetchdf()
    table_names = tables_df['table_name'].tolist()
    
    # Apply dataset filter if needed
    if dataset != 'all':
        table_names = [t for t in table_names if f'_{dataset}' in t]
    
    # Apply zone_type filter if applicable (only for MITMA)
    if source_config.get('has_zone_types', False) and zone_type != 'all':
        table_names = [t for t in table_names if t.endswith(f'_{zone_type}')]
    
    print(f"[CLEANUP] Found {len(table_names)} matching tables: {table_names}")
    
    if not table_names:
        return {
            'bucket': rustfs_bucket,
            'files': [],
            'total_count': 0,
            'total_size_mb': 0,
            'tables': [],
            'source': source
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
                'tables': table_names,
                'source': source
            }
    except Exception as e:
        print(f"[CLEANUP] Error checking bucket: {e}")
        return {
            'bucket': rustfs_bucket,
            'files': [],
            'total_count': 0,
            'total_size_mb': 0,
            'tables': table_names,
            'source': source,
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
        'files_by_table': files_by_table,
        'source': source
    }


@task
def delete_rustfs_files(
    file_info: Dict[str, Any] = None
) -> Dict[str, Any]:
    """
    Deletes files from RustFS bucket.
    """
    from airflow.providers.amazon.aws.hooks.s3 import S3Hook
    
    # Get bucket name from Airflow Variable
    rustfs_bucket = Variable.get('RUSTFS_BUCKET', default='mitma')
    
    if file_info is None:
        file_info = {}
    else:
        # Use bucket from file_info if available (more accurate)
        rustfs_bucket = file_info.get('bucket', rustfs_bucket)
    
    files = file_info.get('files', [])
    total_count = file_info.get('total_count', 0)
    total_size_mb = file_info.get('total_size_mb', 0)
    source = file_info.get('source', 'unknown')
    
    if not files:
        return {
            'status': 'skipped',
            'reason': 'no_files',
            'deleted': 0,
            'source': source
        }
    
    print(f"[CLEANUP] Deleting {total_count} files ({total_size_mb} MB) for source '{source}'")
    
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
        'errors': errors[:10] if errors else [],
        'source': source
    }


@task
def list_bronze_tables(**context) -> Dict[str, Any]:
    """
    Lists all bronze tables for the selected source and their row counts.
    """
    from utils.utils import get_ducklake_connection
    
    # Get params from context
    params = context.get('params', {})
    source = params.get('source', 'mitma')
    dataset = params.get('dataset', 'all')
    zone_type = params.get('zone_type', 'all')
    
    source_config = SOURCE_TABLE_PATTERNS.get(source, SOURCE_TABLE_PATTERNS['mitma'])
    table_pattern = source_config['pattern']
    
    print(f"[CLEANUP] Source: {source}")
    print(f"[CLEANUP] Listing tables matching '{table_pattern}'")
    
    con = get_ducklake_connection()
    
    # Get all tables matching the pattern
    tables_query = f"""
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_name LIKE '{table_pattern}'
        ORDER BY table_name
    """
    
    tables_df = con.execute(tables_query).fetchdf()
    table_names = tables_df['table_name'].tolist()
    
    # Apply dataset filter if needed
    if dataset != 'all':
        table_names = [t for t in table_names if f'_{dataset}' in t]
    
    # Apply zone_type filter if applicable (only for MITMA)
    if source_config.get('has_zone_types', False) and zone_type != 'all':
        table_names = [t for t in table_names if t.endswith(f'_{zone_type}')]
    
    print(f"[CLEANUP] Found {len(table_names)} matching tables: {table_names}")
    
    return {
        'tables': table_names,
        'total_tables': len(table_names),
        'source': source
    }


@task
def drop_bronze_tables(
    table_info: Dict[str, Any] = None,
    **context
) -> Dict[str, Any]:
    """
    Drops Bronze tables using a single SQL statement executed via Cloud Run.
    
    WARNING: This permanently deletes the tables completely (structure and data)!
    """
    from utils.gcp import execute_sql_or_cloud_run
    
    if table_info is None:
        table_info = {}
    
    tables = table_info.get('tables', [])
    source = table_info.get('source', 'unknown')
    
    if not tables:
        return {
            'status': 'skipped',
            'reason': 'no_tables',
            'dropped': 0,
            'source': source
        }
    
    # Build single SQL statement with all DROP TABLE statements
    table_names = tables if isinstance(tables, list) else []
    drop_statements = [f"DROP TABLE IF EXISTS {table_name};" for table_name in table_names]
    sql_query = "\n".join(drop_statements)
    
    print(f"[CLEANUP] ⚠️  DROPPING {len(table_names)} {source} tables (complete deletion)")
    for t in table_names:
        print(f"[CLEANUP]   - {t}")
    
    try:
        # Execute all DROP TABLE statements in a single call via Cloud Run
        result = execute_sql_or_cloud_run(sql_query=sql_query, **context)
        
        print(f"[CLEANUP] ✅ Dropped {len(table_names)} tables")
        
        return {
            'status': 'success',
            'dropped': len(table_names),
            'tables_dropped': table_names,
            'execution_time_seconds': result.get('execution_time_seconds', 0),
            'execution_name': result.get('execution_name', 'unknown'),
            'source': source
        }
        
    except Exception as e:
        print(f"[CLEANUP] ❌ Error dropping tables: {e}")
        return {
            'status': 'error',
            'dropped': 0,
            'error': str(e),
            'source': source
        }


# ============================================================================
# DAG DEFINITION
# ============================================================================

with DAG(
    dag_id="bronze_cleanup",
    start_date=datetime(2025, 12, 1),
    schedule=None,
    catchup=False,
    tags=["bronze", "cleanup", "maintenance"],
    params={
        "source": Param(
            type="string",
            default="mitma",
            enum=["mitma", "ine", "holidays"],
            description="Which data source to clean (mitma, ine, holidays)"
        ),
        "cleanup_rustfs": Param(
            type="boolean",
            default=True,
            description="Delete files from RustFS bucket"
        ),
        "cleanup_tables": Param(
            type="boolean",
            default=False,
            description="⚠️ Drop Bronze tables (DANGER: deletes tables completely!)"
        ),
        "dataset": Param(
            type="string",
            default="all",
            enum=["all", "od", "people_day", "overnight_stay", "zonification", "distances", "empresas", "poblacion", "renta", "municipios"],
            description="Which dataset to clean. 'all' cleans everything for the selected source. MITMA: od, people_day, overnight_stay, zonification, distances. INE: empresas, poblacion, renta, municipios"
        ),
        "zone_type": Param(
            type="string",
            default="all",
            enum=["all", "municipios", "distritos", "gau"],
            description="Which zone type to clean (only applicable for MITMA source)"
        ),
    },
    description="Cleanup DAG for Bronze layer - supports MITMA, INE, and Holidays sources",
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
        
        drop_tables = drop_bronze_tables.override(task_id="drop_tables")(
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
    
    # Dependencies
    start >> [rustfs_group, tables_group]
    rustfs_group >> done
    tables_group >> done
