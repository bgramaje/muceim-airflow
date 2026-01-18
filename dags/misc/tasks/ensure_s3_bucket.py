"""
Airflow task for ensuring RustFS bucket exists before data ingestion.
Creates the bucket if it doesn't exist.
"""

from airflow.sdk import task


@task
def PRE_s3_bucket(bucket_name: str):
    """
    Airflow task to ensure S3 bucket exists.
    Creates the bucket if it doesn't exist.
    
    Args:
        bucket_name: Name of the bucket to check/create
    
    Returns:
    - Dict with status information
    """
    print(f"[TASK] Checking if bucket '{bucket_name}' exists...")
    
    try:
        from airflow.providers.amazon.aws.hooks.s3 import S3Hook
        
        # Use S3Hook to connect to RustFS
        s3_hook = S3Hook(aws_conn_id='rustfs_s3_conn')
        
        # Check if bucket exists
        if s3_hook.check_for_bucket(bucket_name):
            print(f"[TASK] Bucket '{bucket_name}' already exists")
            return {
                'status': 'exists',
                'bucket': bucket_name,
                'message': f"Bucket '{bucket_name}' already exists"
            }
        else:
            # Create bucket
            print(f"[TASK] Creating bucket '{bucket_name}'...")
            s3_hook.create_bucket(bucket_name=bucket_name)
            print(f"[TASK] Bucket '{bucket_name}' created successfully")
            return {
                'status': 'created',
                'bucket': bucket_name,
                'message': f"Bucket '{bucket_name}' created successfully"
            }
            
    except Exception as e:
        print(f"[TASK] Error with bucket: {str(e)}")
        raise
