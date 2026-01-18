"""
Airflow task for ensuring RustFS bucket exists before data ingestion.
Creates the bucket if it doesn't exist.
"""

from airflow.sdk import task
from utils.logger import get_logger


@task
def PRE_s3_bucket(bucket_name: str, **context):
    """
    Airflow task to ensure S3 bucket exists.
    Creates the bucket if it doesn't exist.
    
    Args:
        bucket_name: Name of the bucket to check/create
    
    Returns:
    - Dict with status information
    """
    logger = get_logger(__name__, context)
    logger.info(f"Checking if bucket '{bucket_name}' exists...")
    
    try:
        from airflow.providers.amazon.aws.hooks.s3 import S3Hook
        
        # Use S3Hook to connect to RustFS
        s3_hook = S3Hook(aws_conn_id='rustfs_s3_conn')
        
        # Check if bucket exists
        if s3_hook.check_for_bucket(bucket_name):
            logger.info(f"Bucket '{bucket_name}' already exists")
            return {
                'status': 'exists',
                'bucket': bucket_name,
                'message': f"Bucket '{bucket_name}' already exists"
            }
        else:
            # Create bucket
            logger.info(f"Creating bucket '{bucket_name}'...")
            s3_hook.create_bucket(bucket_name=bucket_name)
            logger.info(f"Bucket '{bucket_name}' created successfully")
            return {
                'status': 'created',
                'bucket': bucket_name,
                'message': f"Bucket '{bucket_name}' created successfully"
            }
            
    except Exception as e:
        logger.error(f"Error with bucket: {str(e)}", exc_info=True)
        raise
