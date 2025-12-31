"""
Cloud Run Job para insertar datos CSV en DuckDB.

Este job recibe parámetros de entorno:
- TABLE_NAME: Nombre de la tabla (sin prefijo 'bronze_')
- URL: URL del CSV a leer (HTTP/HTTPS)
- ZONE_TYPE: Tipo de zona (opcional, default: 'distritos')

El job lee el CSV directamente desde HTTP(S) mediante la extensión httpfs de DuckDB
y lo inserta en DuckDB (DuckLake).

NOTA: Si el servidor remoto (AWS S3) bloquea las IPs de Google Cloud, el job
descargará el archivo primero con curl (que permite headers personalizados) y luego
lo leerá desde archivo local.
"""

import os
import sys
import subprocess
import tempfile
from utils import get_ducklake_connection

def check_internet_connectivity(url):
    """
    Verifica si hay conexión a internet.
    """
    print("[CLOUD_RUN_JOB] Testing internet connectivity with Google...")
    check_url_headers(url)


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


def download_file_with_user_agent(url, output_path):
    """
    Descarga un archivo desde URL usando curl con User-Agent personalizado.
    Útil cuando el servidor (ej: AWS S3) bloquea peticiones sin User-Agent o desde ciertas IPs.
    """
    print(f"[CLOUD_RUN_JOB] Downloading file from URL to local file: {output_path}")
    result = subprocess.run(
        [
            "curl", "-L", "-s", "-f",
            "-H", "User-Agent: MITMA-DuckLake-Loader",
            "-o", output_path,
            url
        ],
        capture_output=True,
        text=True,
        timeout=300  # 5 minutos para archivos grandes
    )
    
    if result.returncode != 0:
        error_msg = f"Failed to download file: {result.stderr}"
        print(f"[CLOUD_RUN_JOB] ❌ {error_msg}")
        raise RuntimeError(error_msg)
    
    print(f"[CLOUD_RUN_JOB] ✅ File downloaded successfully to {output_path}")
    return output_path


def _get_csv_source_query(source_path, use_local_file=False, local_file_path=None, original_url=None):
    """
    Genera la subconsulta SELECT para leer los CSVs.
    Centraliza la configuración de read_csv.
    
    Args:
        source_path: URL HTTP o ruta S3 (s3://...) del archivo
        use_local_file: Si True, lee desde local_file_path en lugar de source_path
        local_file_path: Ruta al archivo local descargado
        original_url: URL original para logging (si se lee desde S3)
    """
    csv_source = f"'{local_file_path}'" if use_local_file else f"'{source_path}'"
    source_file_value = original_url if original_url else source_path
    
    return f"""
        SELECT 
            * EXCLUDE (filename),
            CURRENT_TIMESTAMP AS loaded_at,
            '{source_file_value}' AS source_file
        FROM read_csv(
            {csv_source},
            filename = true,
            all_varchar = true,
            compression = 'gzip'
        )
    """


def check_url_headers(url):
    """
    Ejecuta curl -I a la URL y loguea el resultado por consola.
    Valida que la respuesta sea exitosa (2xx o 3xx).
    Incluye User-Agent para evitar bloqueos de servidores que requieren headers.
    """
    print(f"[CLOUD_RUN_JOB] Checking URL headers with curl -I: {url}")
    try:
        # Usar User-Agent similar al que se usa en utils.py para MITMA
        result = subprocess.run(
            [
                "curl", "-I", "-s", "-w", "\nHTTP_CODE:%{http_code}", 
                "-H", "User-Agent: MITMA-DuckLake-Loader",
                url
            ],
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
        table_name = os.environ.get("TABLE_NAME")
        url = os.environ.get("URL")
        is_s3_path = os.environ.get("IS_S3_PATH", "false").lower() == "true"

        if not table_name or not url:
            print("❌ ERROR: Missing required environment variables: TABLE_NAME and URL")
            sys.exit(1)

        con = get_ducklake_connection()

        merge_keys = _get_data_columns(con, table_name)
        if not merge_keys:
            raise Exception(
                f"No merge keys found for {table_name}. "
                "Check that the table exists and has non-audit columns."
            )

        print(f"[CLOUD_RUN_JOB] Executing merge into {table_name}...")
        print(f"[CLOUD_RUN_JOB] Source: {url} (is_s3_path={is_s3_path})")
        
        # Si es una ruta S3, leer directamente desde S3
        # DuckDB ya tiene configurado S3 en get_ducklake_connection()
        if is_s3_path:
            print(f"[CLOUD_RUN_JOB] Reading CSV from RustFS S3: {url}")
            source_query = _get_csv_source_query(url, use_local_file=False, original_url=url)
            con.execute(f"""
                MERGE INTO {table_name} AS target
                USING ({source_query}) AS source
                ON {_build_merge_condition(merge_keys)}
                WHEN NOT MATCHED THEN
                    INSERT *;
            """)
            print(f"[CLOUD_RUN_JOB] ✅ Successfully read CSV from RustFS S3")
        else:
            # Si es una URL HTTP, intentar leer directamente
            # Si falla, descargar primero con curl (permite User-Agent)
            local_file = None
            
            try:
                # Intentar leer directamente desde URL
                print(f"[CLOUD_RUN_JOB] Attempting to read CSV directly from URL...")
                source_query = _get_csv_source_query(url, use_local_file=False)
                con.execute(f"""
                    MERGE INTO {table_name} AS target
                    USING ({source_query}) AS source
                    ON {_build_merge_condition(merge_keys)}
                    WHEN NOT MATCHED THEN
                        INSERT *;
                """)
                print(f"[CLOUD_RUN_JOB] ✅ Successfully read CSV directly from URL")
            except Exception as direct_error:
                error_str = str(direct_error).lower()
                # Si falla, intentar descargar primero con curl (permite User-Agent)
                if any(keyword in error_str for keyword in ['403', '500', 'access denied', 'forbidden', 'could not open', 'http']):
                    print(f"[CLOUD_RUN_JOB] ⚠️ Direct URL read failed (likely server blocking): {direct_error}")
                    print(f"[CLOUD_RUN_JOB] Falling back to download-first approach...")
                    
                    # Crear archivo temporal
                    with tempfile.NamedTemporaryFile(delete=False, suffix='.csv.gz') as tmp_file:
                        local_file = tmp_file.name
                    
                    try:
                        # Descargar con curl (permite User-Agent)
                        download_file_with_user_agent(url, local_file)
                        
                        # Leer desde archivo local
                        source_query = _get_csv_source_query(url, use_local_file=True, local_file_path=local_file)
                        con.execute(f"""
                            MERGE INTO {table_name} AS target
                            USING ({source_query}) AS source
                            ON {_build_merge_condition(merge_keys)}
                            WHEN NOT MATCHED THEN
                                INSERT *;
                        """)
                        print(f"[CLOUD_RUN_JOB] ✅ Successfully read CSV from downloaded local file")
                    finally:
                        # Limpiar archivo temporal
                        if local_file and os.path.exists(local_file):
                            os.unlink(local_file)
                            print(f"[CLOUD_RUN_JOB] Cleaned up temporary file: {local_file}")
                else:
                    raise

        print(f"[CLOUD_RUN_JOB] ✅ Merged successfully into {table_name}")

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
