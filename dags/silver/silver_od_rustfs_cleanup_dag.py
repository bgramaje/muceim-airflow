"""
DAG for cleaning up RustFS files matching main/silver_od* pattern.
Deletes files from RustFS bucket that match the pattern main/silver_od*

Usage:
    Trigger manually - this DAG will permanently delete files matching main/silver_od*
    
WARNING: This DAG will permanently delete files from RustFS bucket!
"""

from datetime import datetime, timedelta
from typing import Dict, Any

from airflow import DAG
from airflow.sdk import Variable
from airflow.sdk import task
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook


@task
def list_silver_od_files(**context) -> Dict[str, Any]:
    """
    Lists files in RustFS bucket matching the pattern main/silver_od*
    """
    # Get bucket name from Airflow Variable
    rustfs_bucket = Variable.get('RUSTFS_BUCKET', default='mitma')
    
    print(f"[CLEANUP] Listing files in bucket '{rustfs_bucket}' matching pattern 'main/silver_od*'")
    
    s3_hook = S3Hook(aws_conn_id='rustfs_s3_conn')
    
    try:
        if not s3_hook.check_for_bucket(rustfs_bucket):
            print(f"[CLEANUP] Bucket '{rustfs_bucket}' does not exist")
            return {
                'bucket': rustfs_bucket,
                'files': [],
                'total_count': 0,
                'total_size_mb': 0
            }
    except Exception as e:
        print(f"[CLEANUP] Error checking bucket: {e}")
        return {
            'bucket': rustfs_bucket,
            'files': [],
            'total_count': 0,
            'total_size_mb': 0,
            'error': str(e)
        }
    
    s3_client = s3_hook.get_conn()
    paginator = s3_client.get_paginator('list_objects_v2')
    
    all_files = []
    total_size = 0
    prefix = 'main/silver_od'
    
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
                    'last_modified': obj['LastModified'].isoformat()
                }
                
                all_files.append(file_info)
                total_size += size
            
            if not page.get('IsTruncated', False):
                break
        
    except Exception as e:
        print(f"[CLEANUP] Error listing files: {e}")
        return {
            'bucket': rustfs_bucket,
            'files': [],
            'total_count': 0,
            'total_size_mb': 0,
            'error': str(e)
        }
    
    print(f"[CLEANUP] Found {len(all_files)} files ({round(total_size / (1024 * 1024), 2)} MB) matching pattern 'main/silver_od*'")
    
    return {
        'bucket': rustfs_bucket,
        'files': all_files,
        'total_count': len(all_files),
        'total_size_mb': round(total_size / (1024 * 1024), 2)
    }


@task
def delete_silver_od_files(
    file_info: Dict[str, Any] = None
) -> Dict[str, Any]:
    """
    Deletes files from RustFS bucket matching main/silver_od* pattern.
    """
    # Get bucket name from Airflow Variable or file_info
    rustfs_bucket = Variable.get('RUSTFS_BUCKET', default='mitma')
    
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
    
    print(f"[CLEANUP] Deleting {total_count} files ({total_size_mb} MB) matching pattern 'main/silver_od*'")
    
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
        for error in errors[:10]:  # Print first 10 errors
            print(f"[CLEANUP] Error: {error}")
    
    return {
        'status': 'success' if not errors else 'partial',
        'deleted': deleted,
        'freed_mb': total_size_mb,
        'errors': errors[:10] if errors else []
    }


# ============================================================================
# DAG DEFINITION
# ============================================================================

with DAG(
    dag_id="silver_od_rustfs_cleanup",
    start_date=datetime(2025, 12, 1),
    schedule=None,  # Manual trigger only
    catchup=False,
    tags=["silver", "cleanup", "rustfs", "maintenance"],
    description="⚠️ Cleanup DAG - Deletes RustFS files matching main/silver_od* pattern. Manual trigger only.",
    default_args={
        "retries": 1,
        "retry_delay": timedelta(seconds=30),
    },
    max_active_runs=1,
) as dag:
    
    start = EmptyOperator(
        task_id="start",
        doc_md="""
        ## Start Task
        
        ⚠️ **WARNING**: This DAG will permanently delete files from RustFS bucket 
        matching the pattern `main/silver_od*`.
        
        Make sure you have backups if needed before proceeding.
        """
    )
    
    list_files = list_silver_od_files()
    
    delete_files = delete_silver_od_files(file_info=list_files)
    
    done = EmptyOperator(
        task_id="done",
        trigger_rule="none_failed",
        doc_md="""
        ## Done
        
        Files matching pattern `main/silver_od*` have been deleted from RustFS bucket.
        """
    )
    
    # Dependencies
    start >> list_files >> delete_files >> done

