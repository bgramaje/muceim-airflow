"""
Generic Utility functions for Data Ingestion Pipeline.
Contains helper functions for DuckDB operations and connection management.
"""

import duckdb
from contextlib import contextmanager
from utils.logger import get_logger


def load_extension(con: duckdb.DuckDBPyConnection, extension: str):
    """Loads a DuckDB extension with fallback if already installed."""
    logger = get_logger(__name__)
    try:
        con.execute(f"INSTALL {extension};")
        con.execute(f"LOAD {extension};")
        logger.info(f"Extension {extension} loaded")
    except Exception as e:
        logger.warning(f"Warning loading {extension}: {e}")
        try:
            con.execute(f"LOAD {extension};")
            logger.info(f"Extension {extension} loaded (already installed)")
        except:
            logger.error(f"Failed to load {extension}")
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

        logger = get_logger(__name__)
        
        try:
            from airflow.sdk import Connection, Variable  # type: ignore
            logger.info("🔗 Usando conexiones de Airflow...")

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
            logger.info("🔗 Usando variables de entorno (Cloud Run)...")

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

        default_config = _get_default_duckdb_config()
        if duckdb_config:
            duckdb_config = {**default_config, **duckdb_config}
        else:
            duckdb_config = default_config

        USER_AGENT_STR = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        duckdb_config['custom_user_agent'] = USER_AGENT_STR

        logger.debug(f"DuckDB Config: RAM={duckdb_config['memory_limit']}, UA=Windows/Chrome")

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

        con.execute("""
            CREATE OR REPLACE SECRET mitma_conf (
                TYPE HTTP,
                EXTRA_HTTP_HEADERS MAP {
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36',
                    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
                    'Accept-Language': 'es-ES,es;q=0.9',
                    'Connection': 'keep-alive'
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

        load_extension(con, 'httpfs')
        load_extension(con, 'postgres')

        try:
            load_extension(con, 'ducklake')  # Use stable, not core_nightly
        except Exception as e:
            logger.warning(f"DuckLake nightly failed: {e}, trying standard...") 
            try:
                con.execute("FORCE INSTALL ducklake FROM stable;")
                con.execute("LOAD ducklake;")
                logger.info("DuckLake standard loaded")
            except Exception as e:
                logger.error(f"DuckLake standard failed: {e}")
                raise

        databases = con.execute(
            "SELECT database_name FROM duckdb_databases();").fetchdf()
        database_names = databases['database_name'].values if not databases.empty else []
        
        # Build PostgreSQL connection string (single line, no extra whitespace)
        postgres_connection_string = (
            f"dbname={POSTGRES_DB} host={POSTGRES_HOST} user={POSTGRES_USER} "
            f"password={POSTGRES_PASSWORD} port={POSTGRES_PORT} "
            f"sslmode=prefer connect_timeout=60 keepalives=1 keepalives_idle=60 "
            f"keepalives_interval=30 keepalives_count=10 tcp_user_timeout=300000"
        )
        
        # Attach DuckLake for main operations
        if 'ducklake' not in database_names:
            logger.info("🔌 Attaching DuckLake...")
            attach_query = f"""
                ATTACH 'ducklake:postgres:{postgres_connection_string}' 
                AS ducklake (DATA_PATH 's3://{RUSTFS_BUCKET}/');
            """
            con.execute(attach_query)
            logger.info("✅ DuckLake attached")

        con.execute("USE ducklake;")
        
        # Verify DuckLake connection works (this also validates PostgreSQL connectivity)
        try:
            con.execute("SELECT 1")
            logger.info("✅ DuckLake connection verified")
        except Exception as e:
            logger.error(f"⚠️ DuckLake connection verification failed: {e}", exc_info=True)
            raise

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
    """Context manager for DuckLake connection (currently unused)."""
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
