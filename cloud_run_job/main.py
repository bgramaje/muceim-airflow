"""
Cloud Run Job para insertar datos CSV en DuckDB.

Este job recibe parámetros de entorno:
- TABLE_NAME: Nombre de la tabla (sin prefijo 'bronze_')
- URL: URL del CSV a leer (HTTP/HTTPS)
- ZONE_TYPE: Tipo de zona (opcional, default: 'distritos')

El job lee el CSV directamente desde HTTP(S) mediante la extensión httpfs de DuckDB
y lo inserta en DuckDB (DuckLake).
"""

import os
import sys
import subprocess
import duckdb


def load_extension(con, extension):
    """Carga una extensión de DuckDB."""
    try:
        con.execute(f"INSTALL {extension};")
        con.execute(f"LOAD {extension};")
        print(f"✅ Extension {extension} loaded")
    except Exception as e:
        print(f"⚠️ Warning loading {extension}: {e}")
        try:
            con.execute(f"LOAD {extension};")
            print(f"✅ Extension {extension} loaded (already installed)")
        except Exception:
            print(f"❌ Failed to load {extension}")
            raise


def get_duckdb_connection():
    """
    Crea una conexión a DuckDB con DuckLake, PostgreSQL y RustFS.
    Usa variables de entorno para las credenciales.
    """
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

    con = duckdb.connect()

    # httpfs es clave para leer HTTP(S) y S3 desde DuckDB
    extensions = ["ducklake", "postgres", "httpfs", "spatial"]
    for ext in extensions:
        load_extension(con, ext)

    # Config S3 (RustFS/MinIO)
    con.execute(f"SET s3_endpoint='{S3_ENDPOINT}';")
    con.execute(f"SET s3_access_key_id='{RUSTFS_USER}';")
    con.execute(f"SET s3_secret_access_key='{RUSTFS_PASSWORD}';")
    con.execute(f"SET s3_use_ssl={RUSTFS_SSL};")
    con.execute("SET s3_url_style='path';")

    # Performance / recursos
    con.execute("SET preserve_insertion_order=false;")
    con.execute("SET memory_limit='4GB';")
    con.execute("SET threads=4;")
    con.execute("SET worker_threads=4;")
    con.execute("SET max_temp_directory_size='40GiB';")
    con.execute("SET temp_directory='/tmp/duckdb';")
    con.execute("SET enable_object_cache=true;")

    # DuckDB httpfs: evitar descargas forzadas si no hace falta
    con.execute("SET force_download=false;")

    postgres_connection_string = f"""
        dbname={POSTGRES_DB} host={POSTGRES_HOST} user={POSTGRES_USER} password={POSTGRES_PASSWORD} port={POSTGRES_PORT}
        sslmode=require connect_timeout=30 keepalives=1 keepalives_idle=30 keepalives_interval=10 keepalives_count=5 tcp_user_timeout=30000
    """

    databases = con.execute(
        "SELECT database_name FROM duckdb_databases();").fetchdf()
    if "ducklake" not in databases["database_name"].values:
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
    return df["column_name"].tolist()


def _build_merge_condition(columns):
    """
    Construye una cláusula ON robusta que maneja NULLs correctamente.
    """
    return " AND ".join([
        f"target.{col} IS NOT DISTINCT FROM source.{col}"
        for col in columns
    ])


def _get_csv_source_query(urls):
    """
    Genera la subconsulta SELECT para leer los CSVs.
    Centraliza la configuración de read_csv.
    """
    url_list_str = "[" + ", ".join([f"'{u}'" for u in urls]) + "]"

    return f"""
        SELECT 
            * EXCLUDE (filename),
            CURRENT_TIMESTAMP AS loaded_at,
            filename AS source_file
        FROM read_csv(
            {url_list_str},
            filename = true,
            all_varchar = true
        )
    """


def check_url_headers(url):
    """
    Ejecuta curl -I a la URL y loguea el resultado por consola.
    Valida que la respuesta sea exitosa (2xx o 3xx).
    """
    print(f"[CLOUD_RUN_JOB] Checking URL headers with curl -I: {url}")
    try:
        result = subprocess.run(
            ["curl", "-I", "-s", "-w", "\nHTTP_CODE:%{http_code}", url],
            capture_output=True,
            text=True,
            timeout=30
        )
        
        print(f"[CLOUD_RUN_JOB] curl exit code: {result.returncode}")
        print(f"[CLOUD_RUN_JOB] curl output:")
        print(result.stdout)
        
        if result.stderr:
            print(f"[CLOUD_RUN_JOB] curl stderr:")
            print(result.stderr)
        
        # Extraer código HTTP si está disponible
        if "HTTP_CODE:" in result.stdout:
            http_code = result.stdout.split("HTTP_CODE:")[-1].strip()
            http_code_int = int(http_code) if http_code.isdigit() else None
            
            if http_code_int:
                if 200 <= http_code_int < 300:
                    print(f"[CLOUD_RUN_JOB] ✅ URL is accessible (HTTP {http_code_int})")
                elif 300 <= http_code_int < 400:
                    print(f"[CLOUD_RUN_JOB] ⚠️ URL redirects (HTTP {http_code_int})")
                elif 400 <= http_code_int < 500:
                    print(f"[CLOUD_RUN_JOB] ❌ Client error: HTTP {http_code_int} - Check the URL")
                elif http_code_int >= 500:
                    print(f"[CLOUD_RUN_JOB] ❌ Server error: HTTP {http_code_int} - The remote server is failing")
            
    except subprocess.TimeoutExpired:
        print(f"[CLOUD_RUN_JOB] ⚠️ curl -I timed out after 30 seconds")
    except Exception as e:
        print(f"[CLOUD_RUN_JOB] ⚠️ Error executing curl -I: {e}")


def main():
    """Función principal del Cloud Run Job."""
    try:
        # Verificar acceso a internet primero
        print("[CLOUD_RUN_JOB] Testing internet connectivity with Google...")
        test_result = subprocess.run(
            ["curl", "-I", "-s", "-w", "\nHTTP_CODE:%{http_code}", "https://www.google.com"],
            capture_output=True,
            text=True,
            timeout=10
        )
        
        print(f"[CLOUD_RUN_JOB] Google test - exit code: {test_result.returncode}")
        print(f"[CLOUD_RUN_JOB] Google test - output:")
        print(test_result.stdout)
        
        if "HTTP_CODE:200" in test_result.stdout:
            print("[CLOUD_RUN_JOB] ✅ Internet access confirmed - Google returned HTTP 200")
        elif "HTTP_CODE:" in test_result.stdout:
            http_code = test_result.stdout.split("HTTP_CODE:")[-1].strip()
            print(f"[CLOUD_RUN_JOB] ⚠️ Google returned HTTP {http_code}")
        else:
            print("[CLOUD_RUN_JOB] ⚠️ Could not determine HTTP status from Google test")
        
        if test_result.stderr:
            print(f"[CLOUD_RUN_JOB] Google test - stderr: {test_result.stderr}")
        
        print("\n" + "="*60 + "\n")
        
        table_name = os.environ.get("TABLE_NAME")
        url = os.environ.get("URL")
        zone_type = os.environ.get("ZONE_TYPE", "distritos")

        if not table_name or not url:
            print("❌ ERROR: Missing required environment variables: TABLE_NAME and URL")
            sys.exit(1)

        full_table_name = f"bronze_{table_name}"

        check_url_headers(url)

        print(f"[CLOUD_RUN_JOB] Processing: {url} -> {full_table_name}")
        print(f"[CLOUD_RUN_JOB] Zone type: {zone_type}")

        con = get_duckdb_connection()

        merge_keys = _get_data_columns(con, full_table_name)
        if not merge_keys:
            raise Exception(
                f"No merge keys found for {full_table_name}. "
                "Check that the table exists and has non-audit columns."
            )

        print(f"[CLOUD_RUN_JOB] Executing merge into {full_table_name}...")
        con.execute(f"""
            MERGE INTO {full_table_name} AS target
            USING ({_get_csv_source_query([url])}) AS source
            ON {_build_merge_condition(merge_keys)}
            WHEN NOT MATCHED THEN
                INSERT *;
        """)

        print(f"[CLOUD_RUN_JOB] ✅ Merged successfully into {full_table_name}")

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
