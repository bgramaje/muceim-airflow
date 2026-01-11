"""
Generic Utility functions for Data Ingestion Pipeline.
Contains helper functions for DuckDB operations and connection management.
"""

import duckdb
from contextlib import contextmanager

def load_extension(con: duckdb.DuckDBPyConnection, extension: str):
    """Carga una extensión de DuckDB."""
    try:
        con.execute(f"INSTALL {extension};")
        con.execute(f"LOAD {extension};")
        print(f"Extension {extension} loaded")
    except Exception as e:
        print(f"Warning loading {extension}: {e}")
        try:
            con.execute(f"LOAD {extension};")
            print(f"Extension {extension} loaded (already installed)")
        except:
            print(f"Failed to load {extension}")
            raise

def _get_default_duckdb_config():
    """
    Get default DuckDB configuration parameters.
    
    Default values:
    - memory_limit: 8GB
    - threads: 6
    - worker_threads: 6
    - max_temp_directory_size: 80GiB
    - temp_directory: /tmp/duckdb
    - enable_object_cache: true
    
    Returns:
    - dict: Default configuration parameters for DuckDB
    """
    return {
        'memory_limit': '8GB',
        'threads': 6,
        'worker_threads': 6,
        'max_temp_directory_size': '80GiB',
        'temp_directory': '/tmp/duckdb',
        'enable_object_cache': True,
    }


def get_dynamic_duckdb_config(
    batch_size: int = 10,
    estimated_file_size_mb: float = 100,
    max_memory_gb: int = 28,
    max_threads: int = 8
) -> dict:
    """
    Calculates optimal DuckDB configuration based on batch size and workload.
    
    This function dynamically adjusts DuckDB resources based on the expected
    workload, preventing OOM errors and optimizing performance.
    
    Parameters:
    - batch_size: Number of files to process in parallel
    - estimated_file_size_mb: Average file size in MB (default: 100MB for MITMA files)
    - max_memory_gb: Maximum memory limit in GB (default: 28GB)
    - max_threads: Maximum thread count (default: 8)
    
    Returns:
    - dict: Optimized DuckDB configuration
    
    Example:
        # For a small batch (5 files)
        config = get_dynamic_duckdb_config(batch_size=5)
        # Returns: {'memory_limit': '4GB', 'threads': 4, ...}
        
        # For a large batch (20 files)
        config = get_dynamic_duckdb_config(batch_size=20)
        # Returns: {'memory_limit': '16GB', 'threads': 8, ...}
    """
    # Estimate memory needed: 2x the data size for processing overhead
    estimated_data_gb = (batch_size * estimated_file_size_mb) / 1000
    estimated_memory_gb = min(
        max(4, int(estimated_data_gb * 2)),  # At least 4GB, 2x data size
        max_memory_gb
    )
    
    # Threads scale with batch size but have limits
    threads = min(
        max(4, batch_size // 2),  # At least 4 threads
        max_threads
    )
    
    # Temp directory size should be 2x memory for spillover
    temp_size_gb = estimated_memory_gb * 2
    
    config = {
        'memory_limit': f'{estimated_memory_gb}GB',
        'threads': threads,
        'worker_threads': threads,
        'max_temp_directory_size': f'{temp_size_gb}GiB',
        'temp_directory': '/tmp/duckdb',
        'enable_object_cache': True,
    }
    
    print(f"[DUCKDB_CONFIG] Dynamic config for batch_size={batch_size}: "
          f"memory={estimated_memory_gb}GB, threads={threads}")
    
    return config


def get_config_for_workload(workload: str = 'default') -> dict:
    """
    Returns predefined DuckDB configurations for common workloads.
    
    Parameters:
    - workload: One of 'small', 'medium', 'large', 'heavy', 'default'
    
    Workload profiles:
    - small: 1-5 files, light processing (4GB RAM, 4 threads)
    - medium: 5-15 files, moderate processing (8GB RAM, 6 threads)
    - large: 15-30 files, heavy processing (16GB RAM, 8 threads)
    - heavy: 30+ files or very large files (28GB RAM, 8 threads)
    - default: Standard configuration (8GB RAM, 6 threads)
    
    Returns:
    - dict: DuckDB configuration for the workload
    """
    profiles = {
        'small': {
            'memory_limit': '4GB',
            'threads': 4,
            'worker_threads': 4,
            'max_temp_directory_size': '10GiB',
            'temp_directory': '/tmp/duckdb',
            'enable_object_cache': True,
        },
        'medium': {
            'memory_limit': '8GB',
            'threads': 6,
            'worker_threads': 6,
            'max_temp_directory_size': '20GiB',
            'temp_directory': '/tmp/duckdb',
            'enable_object_cache': True,
        },
        'large': {
            'memory_limit': '16GB',
            'threads': 8,
            'worker_threads': 8,
            'max_temp_directory_size': '40GiB',
            'temp_directory': '/tmp/duckdb',
            'enable_object_cache': True,
        },
        'heavy': {
            'memory_limit': '28GB',
            'threads': 8,
            'worker_threads': 8,
            'max_temp_directory_size': '80GiB',
            'temp_directory': '/tmp/duckdb',
            'enable_object_cache': True,
        },
        'default': _get_default_duckdb_config(),
    }
    
    if workload not in profiles:
        print(f"[DUCKDB_CONFIG] Unknown workload '{workload}', using default")
        return profiles['default']
    
    print(f"[DUCKDB_CONFIG] Using '{workload}' workload profile")
    return profiles[workload]


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
    
    def get_connection(self, force_new=False, duckdb_config=None):
        """
        Get or create a DuckLake connection.
        
        Parameters:
        - force_new: If True, close existing connection and create new one
        - duckdb_config: Optional dict with DuckDB configuration parameters to override defaults.
                        If None, uses hardcoded defaults.
                        Keys: memory_limit, threads, worker_threads, max_temp_directory_size,
                              temp_directory, enable_object_cache
                        Defaults: memory_limit='4GB', threads=4, worker_threads=4,
                                 max_temp_directory_size='40GiB', temp_directory='/tmp/duckdb',
                                 enable_object_cache=True
        
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
            self._connection = self._create_connection(duckdb_config=duckdb_config)
        
        return self._connection
    
    def _create_connection(self, duckdb_config=None):
        """
        Create a new DuckLake connection with RustFS and Postgres.
        Uses Airflow connections if available, otherwise uses environment variables (for Cloud Run).
        """
        import os
        
        # Try to use Airflow connections first, fallback to environment variables
        try:
            from airflow.sdk import Connection, Variable # type: ignore
            
            print("🔗 Usando conexiones de Airflow...")
            
            # Obtener configuración de PostgreSQL desde Airflow
            pg_conn = Connection.get('postgres_datos_externos')
            POSTGRES_HOST = pg_conn.host
            POSTGRES_PORT = pg_conn.port or 5432
            POSTGRES_DB = pg_conn.schema
            POSTGRES_USER = pg_conn.login
            POSTGRES_PASSWORD = pg_conn.password
            
            s3_conn = Connection.get('rustfs_s3_conn')
            s3_extra = s3_conn.extra_dejson
            endpoint_url = s3_extra.get('endpoint_url', 'http://rustfs:9000')
            S3_ENDPOINT = endpoint_url.replace('http://', '').replace('https://', '')
            
            # Las credenciales AWS están en extra_dejson
            RUSTFS_USER = s3_extra.get('aws_access_key_id', 'admin')
            RUSTFS_PASSWORD = s3_extra.get('aws_secret_access_key', 'muceim-duckduck.2025!')
            RUSTFS_SSL = 'true' if 'https' in endpoint_url else 'false'
            
            # Obtener bucket desde Variables de Airflow
            RUSTFS_BUCKET = Variable.get('RUSTFS_BUCKET', default='mitma')
            
        except (ImportError, Exception):
            # Fallback to environment variables (Cloud Run scenario)
            print("🔗 Usando variables de entorno (Cloud Run)...")
            
            POSTGRES_HOST = os.environ.get("POSTGRES_HOST")
            POSTGRES_PORT = os.environ.get("POSTGRES_PORT", "5432")
            POSTGRES_DB = os.environ.get("POSTGRES_DB")
            POSTGRES_USER = os.environ.get("POSTGRES_USER")
            POSTGRES_PASSWORD = os.environ.get("POSTGRES_PASSWORD")
            
            S3_ENDPOINT = os.environ.get("S3_ENDPOINT", "rustfs:9000")
            RUSTFS_USER = os.environ.get("RUSTFS_USER")
            RUSTFS_PASSWORD = os.environ.get("RUSTFS_PASSWORD")
            RUSTFS_BUCKET = os.environ.get("RUSTFS_BUCKET", "mitma")
            RUSTFS_SSL = os.environ.get("RUSTFS_SSL", "false")
            
            if not all([POSTGRES_HOST, POSTGRES_DB, POSTGRES_USER, POSTGRES_PASSWORD]):
                raise ValueError("Missing required PostgreSQL environment variables")
            
            if not all([RUSTFS_USER, RUSTFS_PASSWORD]):
                raise ValueError("Missing required RustFS environment variables")
        
        print(f"   PostgreSQL: {POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}")
        print(f"   RustFS: {S3_ENDPOINT}")
        print(f"   Bucket: {RUSTFS_BUCKET}")
    
        # Get DuckDB configuration (use provided config or defaults)
        if duckdb_config is None:
            duckdb_config = _get_default_duckdb_config()
        else:
            # Merge provided config with defaults to allow partial overrides
            default_config = _get_default_duckdb_config()
            duckdb_config = {**default_config, **duckdb_config}
        
        print(f"   DuckDB Config: memory_limit={duckdb_config['memory_limit']}, "
              f"threads={duckdb_config['threads']}, "
              f"worker_threads={duckdb_config['worker_threads']}")
    
        # Create connection
        con = duckdb.connect()
        
        # IMPORTANT: Set custom_user_agent BEFORE loading httpfs extension
        # Once httpfs is loaded, this setting cannot be changed
        con.execute(f"SET custom_user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64)';")
        
        # Apply other configurable DuckDB parameters early (before extensions)
        con.execute(f"SET memory_limit='{duckdb_config['memory_limit']}';")
        con.execute(f"SET threads={duckdb_config['threads']};")
        con.execute(f"SET worker_threads={duckdb_config['worker_threads']};")
        con.execute(f"SET max_temp_directory_size='{duckdb_config['max_temp_directory_size']}';")
        con.execute(f"SET temp_directory='{duckdb_config['temp_directory']}';")
        con.execute(f"SET enable_object_cache={str(duckdb_config['enable_object_cache']).lower()};")
        con.execute(f"SET preserve_insertion_order=false;")
        con.execute(f"SET force_download=false;")
        
        # Install and load critical extensions
        # ducklake: Install from core_nightly with FORCE INSTALL
        try:
            con.execute("FORCE INSTALL ducklake FROM core_nightly;")
            con.execute("LOAD ducklake;")
            print("Extension ducklake loaded from core_nightly")
        except Exception as e:
            print(f"Warning loading ducklake from core_nightly: {e}")
            try:
                # Fallback to standard install
                con.execute("INSTALL ducklake;")
                con.execute("LOAD ducklake;")
                print("Extension ducklake loaded (standard install)")
            except Exception as e2:
                print(f"Failed to load ducklake: {e2}")
                raise
        
        # postgres and httpfs: Always installed by default
        # Note: custom_user_agent must be set BEFORE loading httpfs
        critical_extensions = ['postgres', 'httpfs']
        for ext in critical_extensions:
            load_extension(con, ext)
        
        # spatial: Only load when needed (not in bronze layer)
        # Will be loaded manually in silver layer tasks if needed
        
        # Configure S3/RustFS settings (after httpfs is loaded)
        con.execute(f"SET s3_endpoint='{S3_ENDPOINT}';")
        con.execute(f"SET s3_access_key_id='{RUSTFS_USER}';")
        con.execute(f"SET s3_secret_access_key='{RUSTFS_PASSWORD}';")
        con.execute(f"SET s3_use_ssl={RUSTFS_SSL};")
        con.execute("SET s3_url_style='path';")

        databases = con.execute("SELECT database_name FROM duckdb_databases();").fetchdf()
        if 'ducklake' not in databases['database_name'].values:
            # Configuración mejorada de PostgreSQL para evitar cierres inesperados de SSL
            # sslmode=disable: deshabilita SSL completamente para evitar problemas de conexión
            # keepalives_idle: tiempo antes de enviar el primer keepalive (60s, muy largo para evitar timeouts)
            # keepalives_interval: intervalo entre keepalives (30s, muy largo)
            # keepalives_count: número de keepalives antes de considerar la conexión muerta (10, muchos intentos)
            # tcp_user_timeout: timeout total de TCP (300s, muy largo)
            # connect_timeout: timeout para establecer conexión (60s, más largo)
            postgres_connection_string = f"""
                dbname={POSTGRES_DB} host={POSTGRES_HOST} user={POSTGRES_USER} password={POSTGRES_PASSWORD} port={POSTGRES_PORT} 
                sslmode=prefer 
                connect_timeout=60 
                keepalives=1 
                keepalives_idle=60 
                keepalives_interval=30 
                keepalives_count=10 
                tcp_user_timeout=300000
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


def get_ducklake_connection(force_new=False, duckdb_config=None):
    """
    Get a reusable DuckLake connection (Singleton pattern).
    
    This is the recommended way to get a connection in your tasks.
    The same connection is reused across calls to avoid duplicate ATTACH errors.
    
    Parameters:
    - force_new: If True, close existing connection and create new one
    - duckdb_config: Optional dict with DuckDB configuration parameters to override defaults.
                    If None, uses hardcoded defaults.
                    Keys: memory_limit, threads, worker_threads, max_temp_directory_size,
                          temp_directory, enable_object_cache
                    Defaults: memory_limit='4GB', threads=4, worker_threads=4,
                             max_temp_directory_size='40GiB', temp_directory='/tmp/duckdb',
                             enable_object_cache=True
                    Example: {'memory_limit': '28GB', 'threads': 8}  # Partial override for 32GiB RAM / 8 CPUs
    
    Returns:
    - DuckDB connection object
    """
    return _connection_manager.get_connection(force_new=force_new, duckdb_config=duckdb_config)


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