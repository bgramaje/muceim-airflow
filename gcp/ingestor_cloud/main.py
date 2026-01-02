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
    Usa IS NOT DISTINCT FROM para igualdad null-safe.
    """
    return " AND ".join([
        f"target.{col} IS NOT DISTINCT FROM source.{col}"
        for col in columns
    ])


def _get_csv_source_query(s3_path, original_url=None):
    """
    Genera la subconsulta SELECT para leer los CSVs desde S3.
    Mantiene all_varchar = true para un bronze schema flexible.
    
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
            header = true,
            all_varchar = true,
            -- Si siempre son gzip, mantenlo; si no, quita esta línea
            compression = 'gzip'
        )
    """


def main():
    """Función principal del Cloud Run Job."""
    try:
        table_name = os.environ.get("TABLE_NAME")
        s3_path = os.environ.get("URL")  # Ruta S3
        original_url = os.environ.get("ORIGINAL_URL")  # URL original para logging (opcional)

        if not table_name or not s3_path:
            print("ERROR: Missing required environment variables: TABLE_NAME and URL (S3 path)")
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

        # DuckDB configuration optimized for 32 GiB RAM and 8 CPUs
        config = {
            'memory_limit': '28GB',
            'threads': 8,
            'worker_threads': 8,
            'max_temp_directory_size': '200GiB'
        }
        con = get_ducklake_connection(duckdb_config=config)

        # Validar que la tabla existe
        table_exists = con.execute(f"""
            SELECT COUNT(*) AS cnt
            FROM information_schema.tables
            WHERE table_name = '{table_name}'
        """).fetchone()[0]

        if table_exists == 0:
            raise Exception(f"Target table {table_name} does not exist in DuckLake.")

        merge_keys = _get_data_columns(con, table_name)
        if not merge_keys:
            raise Exception(
                f"No merge keys found for {table_name}. "
                "Check that the table exists and has non-audit columns."
            )

        print(f"[CLOUD_RUN_JOB] Merge keys: {merge_keys}")

        print(f"[CLOUD_RUN_JOB] Reading CSV from RustFS S3 into staging: {s3_path}")
        source_query = _get_csv_source_query(s3_path, original_url=original_url)

        # Staging table (TEMP) para evitar re-leer S3 durante el MERGE
        staging_table = f"tmp_{table_name}_staging"

        # Asegurar que no queda basura de ejecuciones anteriores en la misma conexión
        con.execute(f"DROP TABLE IF EXISTS {staging_table}")

        con.execute(f"""
            CREATE TEMP TABLE {staging_table} AS
            {source_query}
        """)

        # Log de filas en staging
        staging_count = con.execute(f"SELECT COUNT(*) FROM {staging_table}").fetchone()[0]
        print(f"[CLOUD_RUN_JOB] Staging rows loaded from CSV: {staging_count}")

        if staging_count == 0:
            print("[CLOUD_RUN_JOB] No rows to merge; exiting early.")
            con.close()
            sys.exit(0)

        print(f"[CLOUD_RUN_JOB] Executing MERGE into {table_name}...")

        # MERGE usando la tabla staging; todas las columnas siguen siendo VARCHAR + auditoría
        con.execute(f"""
            MERGE INTO {table_name} AS target
            USING {staging_table} AS source
            ON {_build_merge_condition(merge_keys)}
            WHEN NOT MATCHED THEN
                INSERT *;
        """)

        print(f"[CLOUD_RUN_JOB] Successfully read CSV from RustFS S3 into staging")
        print(f"[CLOUD_RUN_JOB] Merged successfully into {table_name}")

        con.close()
        print("[CLOUD_RUN_JOB] Job completed successfully")
        sys.exit(0)

    except Exception as e:
        error_msg = f"Error processing job: {str(e)}"
        print(f"[CLOUD_RUN_JOB] {error_msg}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
