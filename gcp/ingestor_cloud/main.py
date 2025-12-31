"""
Cloud Run Job para insertar datos CSV en DuckDB.

Este job recibe parámetros de entorno:
- TABLE_NAME: Nombre de la tabla (sin prefijo 'bronze_')
- URL: Ruta S3 del CSV a leer (formato: s3://bucket/key)
- ZONE_TYPE: Tipo de zona (opcional, default: 'distritos')

El job lee el CSV directamente desde RustFS S3 mediante DuckDB y lo inserta en DuckDB (DuckLake).
"""

import os
import sys
from utils import get_ducklake_connection


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


def _get_csv_source_query(s3_path, original_url=None):
    """
    Genera la subconsulta SELECT para leer los CSVs desde S3.
    Centraliza la configuración de read_csv.
    
    Args:
        s3_path: Ruta S3 del archivo (s3://bucket/key)
        original_url: URL original para logging (opcional)
    """
    source_file_value = original_url if original_url else s3_path
    
    return f"""
        SELECT 
            * EXCLUDE (filename),
            CURRENT_TIMESTAMP AS loaded_at,
            '{source_file_value}' AS source_file
        FROM read_csv(
            '{s3_path}',
            filename = true,
            all_varchar = true,
            compression = 'gzip'
        )
    """


def main():
    """Función principal del Cloud Run Job."""
    try:
        table_name = os.environ.get("TABLE_NAME")
        s3_path = os.environ.get("URL")  # Ahora siempre es una ruta S3
        original_url = os.environ.get("ORIGINAL_URL")  # URL original para logging (opcional)

        if not table_name or not s3_path:
            print("❌ ERROR: Missing required environment variables: TABLE_NAME and URL (S3 path)")
            sys.exit(1)

        # Validar que la URL es una ruta S3
        if not s3_path.startswith("s3://"):
            raise ValueError(
                f"Invalid S3 path format: {s3_path}. "
                "Expected format: s3://bucket/key"
            )

        print(f"[CLOUD_RUN_JOB] Processing S3 path: {s3_path}")
        if original_url:
            print(f"[CLOUD_RUN_JOB] Original URL: {original_url}")

        con = get_ducklake_connection()

        merge_keys = _get_data_columns(con, table_name)
        if not merge_keys:
            raise Exception(
                f"No merge keys found for {table_name}. "
                "Check that the table exists and has non-audit columns."
            )

        print(f"[CLOUD_RUN_JOB] Executing merge into {table_name}...")
        print(f"[CLOUD_RUN_JOB] Reading CSV from RustFS S3: {s3_path}")
        
        # Leer directamente desde S3
        # DuckDB ya tiene configurado S3 en get_ducklake_connection()
        source_query = _get_csv_source_query(s3_path, original_url=original_url)
        con.execute(f"""
            MERGE INTO {table_name} AS target
            USING ({source_query}) AS source
            ON {_build_merge_condition(merge_keys)}
            WHEN NOT MATCHED THEN
                INSERT *;
        """)
        
        print(f"[CLOUD_RUN_JOB] ✅ Successfully read CSV from RustFS S3")
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
