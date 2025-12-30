"""
Cloud Run Job para insertar datos CSV en DuckDB.

Este job recibe parámetros de entorno:
- TABLE_NAME: Nombre de la tabla (sin prefijo 'bronze_')
- URL: URL del CSV a descargar
- ZONE_TYPE: Tipo de zona (opcional, default: 'distritos')

El job descarga el CSV usando el ancho de banda de GCP y lo inserta en DuckDB.
"""

import os
import sys
import duckdb


def get_duckdb_connection():
    """
    Crea una conexión a DuckDB con DuckLake, PostgreSQL y RustFS.
    Usa variables de entorno para las credenciales.
    """
    # PostgreSQL credentials
    POSTGRES_HOST = os.environ.get("POSTGRES_HOST")
    POSTGRES_PORT = os.environ.get("POSTGRES_PORT", "5432")
    POSTGRES_DB = os.environ.get("POSTGRES_DB")
    POSTGRES_USER = os.environ.get("POSTGRES_USER")
    POSTGRES_PASSWORD = os.environ.get("POSTGRES_PASSWORD")
    
    # RustFS/S3 credentials
    S3_ENDPOINT = os.environ.get("S3_ENDPOINT", "rustfs:9000")
    RUSTFS_USER = os.environ.get("RUSTFS_USER")
    RUSTFS_PASSWORD = os.environ.get("RUSTFS_PASSWORD")
    RUSTFS_BUCKET = os.environ.get("RUSTFS_BUCKET", "mitma")
    RUSTFS_SSL = os.environ.get("RUSTFS_SSL", "false")
    
    if not all([POSTGRES_HOST, POSTGRES_DB, POSTGRES_USER, POSTGRES_PASSWORD]):
        raise ValueError("Missing required PostgreSQL environment variables")
    
    if not all([RUSTFS_USER, RUSTFS_PASSWORD]):
        raise ValueError("Missing required RustFS environment variables")
    
    # Crear conexión DuckDB
    con = duckdb.connect()
    
    # Instalar y cargar extensiones
    extensions = ['ducklake', 'postgres', 'httpfs']
    for ext in extensions:
        try:
            con.execute(f"INSTALL {ext};")
            con.execute(f"LOAD {ext};")
            print(f"✅ Extension {ext} loaded")
        except Exception as e:
            print(f"⚠️ Warning loading {ext}: {e}")
            try:
                con.execute(f"LOAD {ext};")
                print(f"✅ Extension {ext} loaded (already installed)")
            except:
                print(f"❌ Failed to load {ext}")
                raise
    
    # Configurar S3/RustFS
    con.execute(f"SET s3_endpoint='{S3_ENDPOINT}';")
    con.execute(f"SET s3_access_key_id='{RUSTFS_USER}';")
    con.execute(f"SET s3_secret_access_key='{RUSTFS_PASSWORD}';")
    con.execute(f"SET s3_use_ssl={RUSTFS_SSL};")
    con.execute("SET s3_url_style='path';")
    con.execute("SET force_download=false;")  # Stream en vez de descargar todo
    
    # Optimizaciones de memoria
    con.execute("SET memory_limit='4GB';")
    con.execute("SET threads=4;")
    con.execute("SET max_temp_directory_size='40GiB';")
    
    # Attach DuckLake con PostgreSQL Catalog
    postgres_connection_string = f"""
        dbname={POSTGRES_DB} host={POSTGRES_HOST} user={POSTGRES_USER} password={POSTGRES_PASSWORD} port={POSTGRES_PORT} 
        sslmode=require connect_timeout=30 keepalives=1 keepalives_idle=30 keepalives_interval=10 keepalives_count=5 tcp_user_timeout=30000
    """
    
    databases = con.execute("SELECT database_name FROM duckdb_databases();").fetchdf()
    if 'ducklake' not in databases['database_name'].values:
        attach_query = f"""
            ATTACH 'ducklake:postgres:{postgres_connection_string}' AS ducklake (DATA_PATH 's3://{RUSTFS_BUCKET}/');
        """
        con.execute(attach_query)
        print("✅ DuckLake attached")
    
    con.execute("USE ducklake;")
    return con


def _get_data_columns(con, full_table_name):
    """Obtiene las columnas de negocio (excluyendo auditoría)."""
    audit_cols = "('loaded_at', 'source_file', 'source_url')"
    
    df = con.execute(f"""
        SELECT column_name 
        FROM information_schema.columns 
        WHERE table_name = '{full_table_name}'
        AND column_name NOT IN {audit_cols}
        ORDER BY ordinal_position;
    """).fetchdf()
    return df['column_name'].tolist()


def _build_merge_condition(columns):
    """Construye una cláusula ON robusta que maneja NULLs correctamente."""
    return " AND ".join([
        f"target.{col} IS NOT DISTINCT FROM source.{col}" 
        for col in columns
    ])


def _get_csv_source_query(url):
    """Genera la subconsulta SELECT para leer el CSV."""
    return f"""
        SELECT 
            * EXCLUDE (filename),
            CURRENT_TIMESTAMP AS loaded_at,
            filename AS source_file
        FROM read_csv(
            ['{url}'],
            filename = true,
            all_varchar = true
        )
    """


def main():
    """Función principal del Cloud Run Job."""
    try:
        # Obtener parámetros desde variables de entorno
        table_name = os.environ.get("TABLE_NAME")
        url = os.environ.get("URL")
        zone_type = os.environ.get("ZONE_TYPE", "distritos")
        
        if not table_name or not url:
            print("❌ ERROR: Missing required environment variables: TABLE_NAME and URL")
            sys.exit(1)
        
        # Construir nombre completo de la tabla
        full_table_name = f'bronze_{table_name}'
        
        print(f"[CLOUD_RUN_JOB] Processing: {url} -> {full_table_name}")
        print(f"[CLOUD_RUN_JOB] Zone type: {zone_type}")
        
        # Conectar a DuckDB
        con = get_duckdb_connection()
        
        # Obtener columnas para merge
        merge_keys = _get_data_columns(con, full_table_name)
        on_clause = _build_merge_condition(merge_keys)
        
        # Generar query de merge
        source_sql = _get_csv_source_query(url)
        
        # Ejecutar merge
        print(f"[CLOUD_RUN_JOB] Executing merge into {full_table_name}...")
        con.execute(f"""
            MERGE INTO {full_table_name} AS target
            USING ({source_sql}) AS source
            ON {on_clause}
            WHEN NOT MATCHED THEN
                INSERT *;
        """)
        
        print(f"[CLOUD_RUN_JOB] ✅ Merged successfully into {full_table_name}")
        
        # Actualizar estadísticas
        try:
            con.execute(f"ANALYZE {full_table_name};")
            print(f"[CLOUD_RUN_JOB] ✅ Updated statistics for {full_table_name}")
        except Exception as analyze_error:
            print(f"[CLOUD_RUN_JOB] ⚠️ Could not analyze table (non-critical): {analyze_error}")
        
        con.close()
        print("[CLOUD_RUN_JOB] ✅ Job completed successfully")
        sys.exit(0)
        
    except Exception as e:
        error_msg = f"❌ Error processing job: {str(e)}"
        print(f"[CLOUD_RUN_JOB] {error_msg}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()

