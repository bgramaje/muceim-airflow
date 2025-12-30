#!/usr/bin/env python3
"""
Script para probar las conexiones desde el contenedor de Airflow
"""
import sys

def test_postgres():
    """Prueba la conexión a PostgreSQL"""
    try:
        from airflow.providers.postgres.hooks.postgres import PostgresHook
        
        print("🔍 Probando conexión a PostgreSQL...")
        pg_hook = PostgresHook(postgres_conn_id='postgres_datos_externos')
        
        connection = pg_hook.get_conn()
        cursor = connection.cursor()
        
        cursor.execute("SELECT version();")
        version = cursor.fetchone()
        print(f"✅ PostgreSQL conectado correctamente!")
        print(f"   Versión: {version[0][:50]}...")
        
        cursor.execute("SELECT current_database();")
        db = cursor.fetchone()
        print(f"   Base de datos actual: {db[0]}")
        
        cursor.close()
        connection.close()
        return True
        
    except Exception as e:
        print(f"❌ Error conectando a PostgreSQL: {str(e)}")
        return False


def test_rustfs():
    """Prueba la conexión a RustFS"""
    try:
        from airflow.providers.amazon.aws.hooks.s3 import S3Hook
        
        print("\n🔍 Probando conexión a RustFS...")
        s3_hook = S3Hook(aws_conn_id='rustfs_s3_conn')
        
        buckets = s3_hook.list_buckets()
        print(f"✅ RustFS conectado correctamente!")
        print(f"   Buckets disponibles: {buckets}")
        return True
        
    except Exception as e:
        print(f"❌ Error conectando a RustFS: {str(e)}")
        return False


if __name__ == "__main__":
    print("="*60)
    print("PRUEBA DE CONEXIONES AIRFLOW")
    print("="*60)
    
    postgres_ok = test_postgres()
    rustfs_ok = test_rustfs()
    
    print("\n" + "="*60)
    print("RESUMEN")
    print("="*60)
    print(f"PostgreSQL: {'✅ OK' if postgres_ok else '❌ FAILED'}")
    print(f"RustFS:     {'✅ OK' if rustfs_ok else '❌ FAILED'}")
    print("="*60)
    
    sys.exit(0 if (postgres_ok and rustfs_ok) else 1)
