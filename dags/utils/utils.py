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
            self._connection = self._create_connection(
                duckdb_config=duckdb_config)

        return self._connection

    def _create_connection(self, duckdb_config=None):
        """
        Create a new DuckLake connection with RustFS and Postgres.
        Optimized implementation using SECRETS and correct extension loading order.
        """
        import os

        try:
            from airflow.sdk import Connection, Variable  # type: ignore
            print("🔗 Usando conexiones de Airflow...")

            pg_conn = Connection.get('postgres_datos_externos')
            POSTGRES_HOST = pg_conn.host
            POSTGRES_PORT = pg_conn.port or 5432
            POSTGRES_DB = pg_conn.schema
            POSTGRES_USER = pg_conn.login
            POSTGRES_PASSWORD = pg_conn.password

            s3_conn = Connection.get('rustfs_s3_conn')
            s3_extra = s3_conn.extra_dejson
            endpoint_url = s3_extra.get('endpoint_url', 'http://rustfs:9000')
            S3_ENDPOINT = endpoint_url.replace(
                'http://', '').replace('https://', '')
            RUSTFS_USER = s3_extra.get('aws_access_key_id', 'admin')
            RUSTFS_PASSWORD = s3_extra.get(
                'aws_secret_access_key', 'admin')
            RUSTFS_SSL = 'true' if 'https' in endpoint_url else 'false'
            RUSTFS_BUCKET = Variable.get('RUSTFS_BUCKET', default='mitma')

        except (ImportError, Exception):
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

        # --- 2. Configuración Inicial DuckDB ---
        default_config = _get_default_duckdb_config()
        if duckdb_config:
            duckdb_config = {**default_config, **duckdb_config}
        else:
            duckdb_config = default_config

        USER_AGENT_STR = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        duckdb_config['custom_user_agent'] = USER_AGENT_STR

        print(
            f"DuckDB Config: RAM={duckdb_config['memory_limit']}, UA=Windows/Chrome")

        con = duckdb.connect(config=duckdb_config)

        con.execute(f"""
            CREATE OR REPLACE SECRET rustfs_secret (
                TYPE S3,
                KEY_ID '{RUSTFS_USER}',
                SECRET '{RUSTFS_PASSWORD}',
                ENDPOINT '{S3_ENDPOINT}',
                REGION 'eu-west-1', -- Requerido sintácticamente aunque usemos MinIO
                URL_STYLE 'path',
                USE_SSL {RUSTFS_SSL}
            );
        """)

        con.execute(f"""
            CREATE OR REPLACE SECRET http (
                TYPE HTTP,
                EXTRA_HTTP_HEADERS MAP {
                    'User-Agent': '{USER_AGENT_STR}'
                }
            );
        """)

        con.execute(f"SET memory_limit='{duckdb_config['memory_limit']}';")
        con.execute(f"SET threads={duckdb_config['threads']};")
        con.execute(f"SET worker_threads={duckdb_config['worker_threads']};")
        con.execute(
            f"SET max_temp_directory_size='{duckdb_config['max_temp_directory_size']}';")
        con.execute(f"SET temp_directory='{duckdb_config['temp_directory']}';")
        con.execute(
            f"SET enable_object_cache={str(duckdb_config['enable_object_cache']).lower()};")

        # Primero httpfs, ya que DuckLake y S3 dependen de ella
        load_extension(con, 'httpfs')
        load_extension(con, 'postgres')

        try:
            con.execute("FORCE INSTALL ducklake FROM core_nightly;")
            con.execute("LOAD ducklake;")
        except Exception as e:
            print(f"⚠️ DuckLake nightly failed ({e}), trying standard...")
            load_extension(con, 'ducklake')

        # --- 6. Conexión a DuckLake ---
        databases = con.execute(
            "SELECT database_name FROM duckdb_databases();").fetchdf()
        if 'ducklake' not in databases['database_name'].values:
            print("🔌 Attaching DuckLake...")
            postgres_connection_string = f"""
                dbname={POSTGRES_DB} host={POSTGRES_HOST} user={POSTGRES_USER} password={POSTGRES_PASSWORD} port={POSTGRES_PORT} 
                sslmode=prefer connect_timeout=60 keepalives=1 keepalives_idle=60 
                keepalives_interval=30 keepalives_count=10 tcp_user_timeout=300000
            """

            # Nota: Al usar SECRETS arriba, el ATTACH ya tiene acceso autenticado al bucket
            attach_query = f"""
                ATTACH 'ducklake:postgres:{postgres_connection_string}' 
                AS ducklake (DATA_PATH 's3://{RUSTFS_BUCKET}/');
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
