"""
Generic Utility functions for Data Ingestion Pipeline.
Contains helper functions for DuckDB operations and connection management.
"""

import duckdb
from contextlib import contextmanager

class DuckLakeConnectionManager:
    """
    Singleton manager for DuckLake connections.
    Ensures only one connection is created and reused.
    """
    _instance = None
    _connection = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(DuckLakeConnectionManager, cls).__new__(cls)
        return cls._instance
    
    def get_connection(self, force_new=False):
        """
        Get or create a DuckLake connection.
        
        Parameters:
        - force_new: If True, close existing connection and create new one
        
        Returns:
        - DuckDB connection object
        """
        if force_new and self._connection is not None:
            try:
                self._connection.close()
            except:
                pass
            self._connection = None
        
        if self._connection is None:
            self._connection = self._create_connection()
        
        return self._connection
    
    def _create_connection(self):
        """
        Create a new DuckLake connection with RustFS and Postgres.
        Uses Airflow connections solely.
        """
        from airflow.hooks.base import BaseHook # type: ignore
        from airflow.models import Variable # type: ignore
        
        print("🔗 Usando conexiones de Airflow...")
        
        # Obtener configuración de PostgreSQL desde Airflow
        pg_conn = BaseHook.get_connection('postgres_datos_externos')
        POSTGRES_HOST = pg_conn.host
        POSTGRES_PORT = pg_conn.port or 5432
        POSTGRES_DB = pg_conn.schema
        POSTGRES_USER = pg_conn.login
        POSTGRES_PASSWORD = pg_conn.password
        
        # Obtener configuración de RustFS desde Airflow
        s3_conn = BaseHook.get_connection('rustfs_s3_conn')
        s3_extra = s3_conn.extra_dejson
        endpoint_url = s3_extra.get('endpoint_url', 'http://rustfs:9000')
        S3_ENDPOINT = endpoint_url.replace('http://', '').replace('https://', '')
        
        # Las credenciales AWS están en extra_dejson
        RUSTFS_USER = s3_extra.get('aws_access_key_id', 'admin')
        RUSTFS_PASSWORD = s3_extra.get('aws_secret_access_key', 'muceim-duckduck.2025!')
        RUSTFS_SSL = 'true' if 'https' in endpoint_url else 'false'
        
        # Obtener bucket desde Variables de Airflow
        RUSTFS_BUCKET = Variable.get('RUSTFS_BUCKET', default_var='mitma')
        
        print(f"   ✅ PostgreSQL: {POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}")
        print(f"   ✅ RustFS: {S3_ENDPOINT}")
        print(f"   ✅ Bucket: {RUSTFS_BUCKET}")
    
        # Create connection
        con = duckdb.connect()
        
        # Install and load critical extensions first
        critical_extensions = ['ducklake', 'postgres', 'httpfs']
        for ext in critical_extensions:
            try:
                con.execute(f"INSTALL {ext};")
                con.execute(f"LOAD {ext};")
                print(f"   ✅ Extension {ext} loaded")
            except Exception as e:
                print(f"   ⚠️ Warning loading {ext}: {e}")
                # Try to load anyway in case it's already installed
                try:
                    con.execute(f"LOAD {ext};")
                    print(f"   ✅ Extension {ext} loaded (already installed)")
                except Exception as e2:
                    print(f"   ❌ Failed to load {ext}: {e2}")
                    raise  # Critical extensions must load
        
        # Try to load spatial extension (optional for some tasks)
        try:
            con.execute("INSTALL spatial;")
            con.execute("LOAD spatial;")
            print(f"   ✅ Extension spatial loaded")
        except Exception as e:
            print(f"   ⚠️ Spatial extension not loaded: {e}")
            print(f"   ℹ️  Spatial functions will not be available")
        
        # Configure S3 for RustFS
        con.execute(f"SET s3_endpoint='{S3_ENDPOINT}';")
        con.execute(f"SET s3_access_key_id='{RUSTFS_USER}';")
        con.execute(f"SET s3_secret_access_key='{RUSTFS_PASSWORD}';")
        con.execute(f"SET s3_use_ssl={RUSTFS_SSL};")
        con.execute("SET s3_url_style='path';")
        con.execute("SET preserve_insertion_order=false;")
        
        # OPTIMIZACION DE RECURSOS PARA AIRFLOW - Evitar que se mate por recursos
        # Limitar memoria para evitar OOM killer
        con.execute("SET memory_limit='4GB';")  # Ajusta según RAM disponible (2GB si <8GB RAM)
        
        # Limitar threads para no saturar CPU
        con.execute("SET threads=4;")  # Reduce según cores (1-2 si problemas persisten)
        con.execute("SET worker_threads=2;")  # Threads para I/O asíncrono
        
        # Optimizar spillfiles en disco cuando se agota RAM
        con.execute("SET max_temp_directory_size='40GiB';")
        con.execute("SET temp_directory='/tmp/duckdb';")  # Directorio para spillover
        
        # Optimizaciones de cache y objeto
        con.execute("SET enable_object_cache=true;")  # Cache de metadatos (bajo overhead)
        
        # Optimizaciones de S3/RustFS para reducir memory peaks
        con.execute("SET force_download=false;")  # Stream en vez de descargar todo
        
        # Check if ducklake is already attached
        databases = con.execute("SELECT database_name FROM duckdb_databases();").fetchdf()
        if 'ducklake' not in databases['database_name'].values:
            # Attach DuckLake with Postgres Catalog
            postgres_connection_string = f"""
                dbname={POSTGRES_DB} host={POSTGRES_HOST} user={POSTGRES_USER} password={POSTGRES_PASSWORD} port={POSTGRES_PORT} 
                sslmode=require connect_timeout=30 keepalives=1 keepalives_idle=30 keepalives_interval=10 keepalives_count=5 tcp_user_timeout=30000
            """
            attach_query = f"""
                ATTACH 'ducklake:postgres:{postgres_connection_string}' AS ducklake (DATA_PATH 's3://{RUSTFS_BUCKET}/');
            """
            con.execute(attach_query)
        
        con.execute("USE ducklake;")
        
        return con
    
    def close(self):
        """Close the connection if it exists."""
        if self._connection is not None:
            try:
                self._connection.close()
            except:
                pass
            self._connection = None


_connection_manager = DuckLakeConnectionManager()


def get_ducklake_connection(force_new=False):
    """
    Get a reusable DuckLake connection (Singleton pattern).
    
    This is the recommended way to get a connection in your tasks.
    The same connection is reused across calls to avoid duplicate ATTACH errors.
    
    Parameters:
    - force_new: If True, close existing connection and create new one
    
    Returns:
    - DuckDB connection object
    """
    return _connection_manager.get_connection(force_new=force_new)


@contextmanager
def ducklake_connection():
    """
    Context manager for DuckLake connection.
    
    Use this when you want automatic cleanup, but be aware it will
    close the connection when exiting the context.
    """
    con = get_ducklake_connection()
    try:
        yield con
    finally:
        pass


def close_ducklake_connection():
    """
    Explicitly close the DuckLake connection.
    Only use this at the very end of your pipeline.
    """
    _connection_manager.close()


def get_default_pool_slots() -> int:
    """
    Get the number of slots in the default_pool.
    
    Returns:
    - int: Number of slots in the default_pool, or 128 if pool cannot be accessed
    """
    try:
        from airflow.models import Pool  # type: ignore
        pool = Pool.get_pool(pool_name="default_pool")
        return pool.slots if pool else 128  # Default is 128 if pool not found
    except Exception:
        # Fallback to default value if pool cannot be accessed
        return 128