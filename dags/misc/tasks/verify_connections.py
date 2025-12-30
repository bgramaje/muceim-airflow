"""
Airflow task to verify PostgreSQL and RustFS connections.
"""

from airflow.sdk import task


@task
def PRE_verify_connections():
    """
    Airflow task to verify PostgreSQL and RustFS connections.
    
    Returns:
    - Dict with connection status
    """
    print("[TASK] Verifying connections...")
    
    results = {
        'postgres': False,
        'rustfs': False,
        'errors': []
    }
    
    try:
        from airflow.providers.postgres.hooks.postgres import PostgresHook
        
        print("[TASK] 🔍 Testing PostgreSQL connection...")
        pg_hook = PostgresHook(postgres_conn_id='postgres_datos_externos')
        
        connection = pg_hook.get_conn()
        cursor = connection.cursor()
        cursor.execute("SELECT version();")
        version = cursor.fetchone()
        cursor.close()
        connection.close()
        
        print(f"[TASK] ✅ PostgreSQL OK: {version[0][:50]}...")
        results['postgres'] = True
        
    except Exception as e:
        error_msg = f"PostgreSQL connection failed: {str(e)}"
        print(f"[TASK] ❌ {error_msg}")
        results['errors'].append(error_msg)
    
    try:
        from airflow.providers.amazon.aws.hooks.s3 import S3Hook
        
        print("[TASK] 🔍 Testing RustFS connection...")
        s3_hook = S3Hook(aws_conn_id='rustfs_s3_conn')
        
        s3_client = s3_hook.get_conn()
        response = s3_client.list_buckets()
        buckets = [bucket['Name'] for bucket in response.get('Buckets', [])]
        
        print(f"[TASK] ✅ RustFS OK. Buckets: {buckets}")
        results['rustfs'] = True
        
    except Exception as e:
        error_msg = f"RustFS connection failed: {str(e)}"
        print(f"[TASK] ❌ {error_msg}")
        results['errors'].append(error_msg)
    
    if results['postgres'] and results['rustfs']:
        print("[TASK] ✅ All connections verified successfully")
        results['status'] = 'success'
    else:
        print("[TASK] ❌ Some connections failed")
        results['status'] = 'failed'
        raise Exception(f"Connection verification failed: {results['errors']}")
    
    return results
