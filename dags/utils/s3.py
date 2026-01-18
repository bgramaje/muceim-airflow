"""
S3 utilities for uploading files to RustFS using Airflow connection.
"""


def upload_to_s3_rustfs(
    content: bytes | str,
    s3_key: str,
    content_type: str = 'application/octet-stream',
    bucket_name: str = None
) -> str:
    """
    Upload content to RustFS S3 using Airflow connection 'rustfs_s3_conn'.
    
    This function tries to use S3Hook when running in Airflow context,
    otherwise falls back to boto3 using credentials from environment variables
    (which come from the Airflow connection when running in Cloud Run).
    
    Args:
        content: Content to upload (bytes or string)
        s3_key: S3 key (path) where to upload the file
        content_type: MIME type of the content (default: 'application/octet-stream')
        bucket_name: Bucket name (default: from RUSTFS_BUCKET variable or 'mitma')
    
    Returns:
        S3 path in format 's3://bucket/key'
    
    Raises:
        RuntimeError: If upload fails or credentials are not available
    """
    import os
    
    if isinstance(content, str):
        content_bytes = content.encode('utf-8')
    else:
        content_bytes = content
    
    try:
        from airflow.providers.amazon.aws.hooks.s3 import S3Hook

        s3_hook = S3Hook(aws_conn_id='rustfs_s3_conn')
        
        s3_hook.load_bytes(
            bytes_data=content_bytes,
            key=s3_key,
            bucket_name=bucket_name,
            replace=True
        )
        
        s3_path = f"s3://{bucket_name}/{s3_key}"
        print(f"[S3_UPLOAD] Uploaded to {s3_path} using S3Hook")
        return s3_path
        
    except (ImportError, Exception) as e:
        print(f"Executing fallback to boto3 using environment variables (Cloud Run)")
        import boto3
        from botocore.config import Config
        
        s3_endpoint = os.environ.get("S3_ENDPOINT", "rustfs:9000")
        rustfs_user = os.environ.get("RUSTFS_USER")
        rustfs_password = os.environ.get("RUSTFS_PASSWORD")
        rustfs_ssl = os.environ.get("RUSTFS_SSL", "false").lower() == "true"
        
        print(s3_endpoint, rustfs_user, bucket_name)
        
        if not rustfs_user or not rustfs_password:
            raise RuntimeError(
                "S3 credentials not available. "
                "Make sure Airflow connection 'rustfs_s3_conn' is configured, "
                "or environment variables RUSTFS_USER and RUSTFS_PASSWORD are set."
            ) from e
        
        # Configure boto3 S3 client
        endpoint_url = f"{'https' if rustfs_ssl else 'http'}://{s3_endpoint}"
        s3_client = boto3.client(
            's3',
            endpoint_url=endpoint_url,
            aws_access_key_id=rustfs_user,
            aws_secret_access_key=rustfs_password,
            config=Config(signature_version='s3v4')
        )
        
        # Upload to S3
        s3_client.put_object(
            Bucket=bucket_name,
            Key=s3_key,
            Body=content_bytes,
            ContentType=content_type
        )
        
        s3_path = f"s3://{bucket_name}/{s3_key}"
        print(f"[S3_UPLOAD] Uploaded to {s3_path} using boto3 (Cloud Run)")
        return s3_path
