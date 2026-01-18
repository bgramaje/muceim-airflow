"""
Airflow task to verify PostgreSQL and RustFS connections.
"""

from airflow.sdk import task
from utils.logger import get_logger


@task
def PRE_verify_connections(**context):
    """
    Airflow task to verify PostgreSQL and RustFS connections.
    
    Returns:
    - Dict with connection status
    """
    logger = get_logger(__name__, context)
    logger.info("Verifying connections...")
    
    results = {
        'postgres': False,
        'rustfs': False,
        'errors': []
    }
    
    try:
        from airflow.providers.postgres.hooks.postgres import PostgresHook
        
        logger.info("Testing PostgreSQL connection...")
        pg_hook = PostgresHook(postgres_conn_id='postgres_datos_externos')
        
        connection = pg_hook.get_conn()
        cursor = connection.cursor()
        cursor.execute("SELECT version();")
        version = cursor.fetchone()
        cursor.close()
        connection.close()
        
        logger.info(f"PostgreSQL OK: {version[0][:50]}...")
        results['postgres'] = True
        
    except Exception as e:
        error_msg = f"PostgreSQL connection failed: {str(e)}"
        logger.error(error_msg, exc_info=True)
        results['errors'].append(error_msg)
    
    try:
        from airflow.providers.amazon.aws.hooks.s3 import S3Hook
        
        logger.info("Testing RustFS connection...")
        s3_hook = S3Hook(aws_conn_id='rustfs_s3_conn')
        
        s3_client = s3_hook.get_conn()
        response = s3_client.list_buckets()
        buckets = [bucket['Name'] for bucket in response.get('Buckets', [])]
        
        logger.info(f"RustFS OK. Buckets: {buckets}")
        results['rustfs'] = True
        
    except Exception as e:
        error_msg = f"RustFS connection failed: {str(e)}"
        logger.error(error_msg, exc_info=True)
        results['errors'].append(error_msg)
    
    if results['postgres'] and results['rustfs']:
        logger.info("All connections verified successfully")
        results['status'] = 'success'
    else:
        logger.error("Some connections failed")
        results['status'] = 'failed'
        raise Exception(f"Connection verification failed: {results['errors']}")
    
    return results
