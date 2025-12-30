"""
Google Cloud Function para insertar datos CSV en DuckDB.

Esta función recibe:
- table_name: Nombre de la tabla (sin prefijo 'bronze_')
- url: URL del CSV a descargar
- zone_type: Tipo de zona (opcional)

La función descarga el CSV usando el ancho de banda de GCP y lo inserta en DuckDB.
"""

import json
import os
import duckdb
from google.cloud import secretmanager


def get_secret(secret_id: str, project_id: str) -> str:
    """Obtiene un secreto desde Secret Manager."""
    client = secretmanager.SecretManagerServiceClient()
    name = f"projects/{project_id}/secrets/{secret_id}/versions/latest"
    response = client.access_secret_version(request={"name": name})
    return response.payload.data.decode("UTF-8")


def get_duckdb_connection():
    """
    Crea una conexión a DuckDB con DuckLake, PostgreSQL y RustFS.
    Usa variables de entorno o Secret Manager para las credenciales.
    """
    # Obtener credenciales desde variables de entorno o Secret Manager
    project_id = os.environ.get("GCP_PROJECT_ID", "muceim-bigdata")
    
    # PostgreSQL credentials
    POSTGRES_HOST = os.environ.get("POSTGRES_HOST") or get_secret("postgres-host", project_id)
    POSTGRES_PORT = os.environ.get("POSTGRES_PORT", "5432")
    POSTGRES_DB = os.environ.get("POSTGRES_DB") or get_secret("postgres-db", project_id)
    POSTGRES_USER = os.environ.get("POSTGRES_USER") or get_secret("postgres-user", project_id)
    POSTGRES_PASSWORD = os.environ.get("POSTGRES_PASSWORD") or get_secret("postgres-password", project_id)
    
    # RustFS/S3 credentials
    S3_ENDPOINT = os.environ.get("S3_ENDPOINT", "rustfs:9000")
    RUSTFS_USER = os.environ.get("RUSTFS_USER") or get_secret("rustfs-user", project_id)
    RUSTFS_PASSWORD = os.environ.get("RUSTFS_PASSWORD") or get_secret("rustfs-password", project_id)
    RUSTFS_BUCKET = os.environ.get("RUSTFS_BUCKET", "mitma")
    RUSTFS_SSL = os.environ.get("RUSTFS_SSL", "false")
    
    # Crear conexión DuckDB
    con = duckdb.connect()
    
    # Instalar y cargar extensiones
    extensions = ['ducklake', 'postgres', 'httpfs']
    for ext in extensions:
        try:
            con.execute(f"INSTALL {ext};")
            con.execute(f"LOAD {ext};")
        except Exception as e:
            print(f"⚠️ Warning loading {ext}: {e}")
            try:
                con.execute(f"LOAD {ext};")
            except:
                pass
    
    # Configurar S3/RustFS
    con.execute(f"SET s3_endpoint='{S3_ENDPOINT}';")
    con.execute(f"SET s3_access_key_id='{RUSTFS_USER}';")
    con.execute(f"SET s3_secret_access_key='{RUSTFS_PASSWORD}';")
    con.execute(f"SET s3_use_ssl={RUSTFS_SSL};")
    con.execute("SET force_download=false;")  # Stream en vez de descargar todo
    
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


def merge_csv_to_duckdb(request):
    """
    Cloud Function entry point.
    
    Recibe un JSON con:
    {
        "table_name": "mitma_od_municipios",
        "url": "https://...",
        "zone_type": "municipios"
    }
    
    Retorna:
    {
        "status": "success",
        "message": "Data merged successfully"
    }
    """
    try:
        # Parsear request
        if isinstance(request, str):
            data = json.loads(request)
        else:
            data = request.get_json() if hasattr(request, 'get_json') else request
        
        table_name = data.get('table_name')
        url = data.get('url')
        zone_type = data.get('zone_type', 'distritos')
        
        if not table_name or not url:
            return {
                'status': 'error',
                'error': 'Missing required parameters: table_name and url'
            }, 400
        
        # Construir nombre completo de la tabla
        full_table_name = f'bronze_{table_name}'
        
        print(f"[CLOUD_FUNCTION] Processing: {url} -> {full_table_name}")
        
        # Conectar a DuckDB
        con = get_duckdb_connection()
        
        # Obtener columnas para merge
        merge_keys = _get_data_columns(con, full_table_name)
        on_clause = _build_merge_condition(merge_keys)
        
        # Generar query de merge
        source_sql = _get_csv_source_query(url)
        
        # Ejecutar merge
        con.execute(f"""
            MERGE INTO {full_table_name} AS target
            USING ({source_sql}) AS source
            ON {on_clause}
            WHEN NOT MATCHED THEN
                INSERT *;
        """)
        
        print(f"[CLOUD_FUNCTION] ✅ Merged successfully")
        
        return {
            'status': 'success',
            'message': f'Data merged successfully into {full_table_name}',
            'table_name': full_table_name,
            'url': url
        }, 200
        
    except Exception as e:
        error_msg = f"Error processing request: {str(e)}"
        print(f"[CLOUD_FUNCTION] ❌ {error_msg}")
        return {
            'status': 'error',
            'error': error_msg
        }, 500

