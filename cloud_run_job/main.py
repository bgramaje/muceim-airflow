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


def _get_csv_source_query(url):
    """
    Genera la subconsulta SELECT para leer los CSVs.
    Centraliza la configuración de read_csv.
    """

    return f"""
        SELECT 
            * EXCLUDE (filename),
            CURRENT_TIMESTAMP AS loaded_at,
            filename AS source_file
        FROM read_csv(
            {url},
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

        full_table_name = f"bronze_{table_name}"

        check_internet_connectivity("https://www.google.com")
        check_internet_connectivity("https://movilidad-opendata.mitma.es")

        con = get_ducklake_connection()

        merge_keys = _get_data_columns(con, full_table_name)
        if not merge_keys:
            raise Exception(
                f"No merge keys found for {full_table_name}. "
                "Check that the table exists and has non-audit columns."
            )

        print(f"[CLOUD_RUN_JOB] Executing merge into {full_table_name}...")
        con.execute(f"""
            MERGE INTO {full_table_name} AS target
            USING ({_get_csv_source_query(url)}) AS source
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
