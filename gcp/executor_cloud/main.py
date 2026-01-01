import os
import sys
from utils import get_ducklake_connection


def main():
    """Función principal del Cloud Run Job para ejecutar consultas SQL."""
    try:
        sql_query = os.environ.get("SQL_QUERY")

        if not sql_query:
            print("❌ ERROR: Missing required environment variable: SQL_QUERY")
            sys.exit(1)

        print(f"[CLOUD_RUN_JOB] Executing SQL query...")
        print(f"[CLOUD_RUN_JOB] {sql_query[:200]}")

        # DuckDB configuration optimized for 32 GiB RAM and 8 CPUs
        config = {
            'memory_limit': '28GB',
            'threads': 8,
            'worker_threads': 8,
            'max_temp_directory_size': '200GiB'
        }
        con = get_ducklake_connection(duckdb_config=config)

        try:
            df =  con.execute(sql_query).fetchdf()
            row_count = len(df)
            print(f"[CLOUD_RUN_JOB] ✅ Query executed successfully")
            print(f"[CLOUD_RUN_JOB] ✅ Rows affected/returned: {row_count}")
            if row_count > 0:
                print(df.head(5).to_string())
        except Exception as query_error:
            error_msg = f"❌ Error executing SQL query: {str(query_error)}"
            print(f"[CLOUD_RUN_JOB] {error_msg}")
            raise

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

