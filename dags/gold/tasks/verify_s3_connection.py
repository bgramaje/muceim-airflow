"""
S3 Connection Verification Task for Gold Layer

This module contains a task to verify S3 connection in Cloud Run.
It performs a simple SELECT query and then tests S3 connectivity using boto3.
"""

import sys
import os
from airflow.sdk import task  # type: ignore

# Add parent directory to path to import utils
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))))


from utils.gcp import execute_sql_or_cloud_run


def _post_process_verify_s3(df, con, result_dict):
    """
    Post-processing function to verify S3 connection using boto3.
    This function runs in Cloud Run and tests S3 connectivity.
    """
    import os
    import boto3
    from botocore.config import Config
    from datetime import datetime
    
    # Get S3 credentials from environment variables (available in Cloud Run)
    s3_endpoint = os.environ.get("S3_ENDPOINT", "rustfs:9000")
    rustfs_user = os.environ.get("RUSTFS_USER")
    rustfs_password = os.environ.get("RUSTFS_PASSWORD")
    rustfs_ssl = os.environ.get("RUSTFS_SSL", "false").lower() == "true"
    bucket_name = os.environ.get("RUSTFS_BUCKET", "mitma")
    
    print(f"[INFO] Testing S3 connection to endpoint: {s3_endpoint}")
    print(f"[INFO] Using bucket: {bucket_name}")
    
    try:
        # Configure boto3 S3 client
        endpoint_url = f"{'https' if rustfs_ssl else 'http'}://{s3_endpoint}"
        s3_client = boto3.client(
            's3',
            endpoint_url=endpoint_url,
            aws_access_key_id=rustfs_user,
            aws_secret_access_key=rustfs_password,
            config=Config(signature_version='s3v4')
        )
        
        # Test 1: List buckets (or head bucket)
        print("[TEST] Testing bucket access...")
        try:
            s3_client.head_bucket(Bucket=bucket_name)
            print(f"[SUCCESS] Bucket '{bucket_name}' is accessible")
        except Exception as e:
            # If head_bucket fails, try list_objects_v2
            try:
                s3_client.list_objects_v2(Bucket=bucket_name, MaxKeys=1)
                print(f"[SUCCESS] Bucket '{bucket_name}' is accessible (via list_objects)")
            except Exception as e2:
                raise Exception(f"Cannot access bucket '{bucket_name}': {str(e2)}")
        
        # Test 2: Upload a test file
        print("[TEST] Testing file upload...")
        test_key = f"gold/_verification/test_connection_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
        test_content = f"S3 connection test - {datetime.now().isoformat()}"
        
        s3_client.put_object(
            Bucket=bucket_name,
            Key=test_key,
            Body=test_content.encode('utf-8'),
            ContentType='text/plain'
        )
        print(f"[SUCCESS] Test file uploaded to s3://{bucket_name}/{test_key}")
        
        # Test 3: Read the test file back
        print("[TEST] Testing file read...")
        response = s3_client.get_object(Bucket=bucket_name, Key=test_key)
        read_content = response['Body'].read().decode('utf-8')
        if read_content == test_content:
            print(f"[SUCCESS] Test file read successfully from s3://{bucket_name}/{test_key}")
        else:
            raise Exception("Read content does not match written content")
        
        # Test 4: Delete the test file (cleanup)
        print("[TEST] Cleaning up test file...")
        s3_client.delete_object(Bucket=bucket_name, Key=test_key)
        print(f"[SUCCESS] Test file deleted from s3://{bucket_name}/{test_key}")
        
        s3_path = f"s3://{bucket_name}/{test_key}"
        print(f"[SUCCESS] All S3 connection tests passed!")
        
        return {
            'status': 'success',
            's3_endpoint': endpoint_url,
            'bucket': bucket_name,
            'message': 'S3 connection verified successfully',
            'test_file_uploaded': test_key,
            'test_file_deleted': True
        }
        
    except Exception as e:
        error_msg = f"S3 connection test failed: {str(e)}"
        print(f"[ERROR] {error_msg}")
        raise Exception(error_msg) from e


@task
def GOLD_verify_s3_connection(**context):
    """
    Verify S3 connection in Cloud Run.
    Performs a simple SELECT query and then tests S3 connectivity using boto3.
    
    This task:
    1. Executes a simple SELECT query (low cost) to verify database connection
    2. Tests S3 connection using boto3:
       - Verifies bucket access
       - Tests file upload
       - Tests file read
       - Cleans up test file
    
    Returns:
    - Dict with verification results
    """
    print("[TASK] Verifying S3 connection in Cloud Run")
    
    # Simple SELECT query with low cost - just get a few rows from silver_zones
    sql_query = """
        SELECT id, nombre 
        FROM silver_zones 
        LIMIT 5;
    """
    
    # Pass the function directly - parameters will be read from env vars inside the function
    result = execute_sql_or_cloud_run(sql_query=sql_query, post_process_func=_post_process_verify_s3, **context)
    
    print(f"[TASK] S3 verification completed: {result.get('status', 'unknown')}")
    return result

