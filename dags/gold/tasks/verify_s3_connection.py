"""
S3 Connection Verification Task for Gold Layer

This module contains a task to verify S3 connection.
It simply verifies bucket access and tests file upload capability.
"""

from airflow.sdk import task  # type: ignore
from airflow.sdk import Variable
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from datetime import datetime


@task
def GOLD_verify_s3_connection(**context):
    """
    Verify S3 connection by testing bucket access and file upload.
    
    This task:
    1. Verifies bucket access
    2. Tests file upload by creating a verification file
    
    Returns:
    - Dict with verification results
    """
    print("[TASK] Verifying S3 connection")
    
    try:
        # Get bucket name from Airflow Variables
        bucket_name = Variable.get('RUSTFS_BUCKET', default='mitma')
        
        # Use S3Hook to connect to RustFS
        s3_hook = S3Hook(aws_conn_id='rustfs_s3_conn')
        
        # Test 1: Verify bucket access
        print(f"[TEST] Verifying access to bucket '{bucket_name}'...")
        if not s3_hook.check_for_bucket(bucket_name):
            raise Exception(f"Bucket '{bucket_name}' does not exist or is not accessible")
        print(f"[SUCCESS] Bucket '{bucket_name}' is accessible")
        
        # Test 2: Upload a simple verification file
        print("[TEST] Testing file upload...")
        test_key = f"gold/_verification/test_connection_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
        test_content = f"S3 connection verification - {datetime.now().isoformat()}"
        
        s3_hook.load_string(
            string_data=test_content,
            key=test_key,
            bucket_name=bucket_name,
            replace=True
        )
        print(f"[SUCCESS] Verification file uploaded to s3://{bucket_name}/{test_key}")
        
        result = {
            'status': 'success',
            'bucket': bucket_name,
            'message': 'S3 connection verified successfully',
            'verification_file': test_key
        }
        
        print("[TASK] S3 verification completed successfully")
        return result
        
    except Exception as e:
        error_msg = f"S3 connection verification failed: {str(e)}"
        print(f"[ERROR] {error_msg}")
        raise Exception(error_msg) from e
