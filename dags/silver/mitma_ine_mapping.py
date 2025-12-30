
import sys
import os
from airflow.sdk import task # type: ignore

# Add parent directory to path to import utils
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils import get_ducklake_connection

@task
def SILVER_mitma_ine_mapping():
    """
    Airflow task to create silver_mitma_ine_mapping table.
    """
    print("[TASK] Building silver_mitma_ine_mapping table")

    con = get_ducklake_connection()

    # User provided SQL from notebook
    query = """
    CREATE OR REPLACE TABLE silver_mitma_ine_mapping AS (
        SELECT DISTINCT
            replace(lower(strip_accents(
                TRIM(
                    COALESCE(
                        NULLIF(split_part(split_part(m.Nombre, '.', 1), '/', 2), ''),
                        split_part(split_part(m.Nombre, '.', 1), '/', 1)
                    )
                )
            )), '-', ' ') AS nombre,
            m.Codigo as codigo_ine,
            r.municipio_mitma
        FROM bronze_ine_municipios m
        JOIN bronze_mitma_ine_relations r
            ON m.Codigo = r.municipio_ine
        WHERE r.municipio_mitma IS NOT NULL
            AND m.Codigo IS NOT NULL
            AND nombre IS NOT NULL
        ORDER BY m.Codigo
    )
    """
    
    # Executing the query
    con.execute(query)

    # Verification
    count = con.execute("SELECT COUNT(*) as count FROM silver_mitma_ine_mapping").fetchdf()
    record_count = int(count['count'].iloc[0])
    
    print(f"[TASK] Created silver_mitma_ine_mapping with {record_count:,} records")
    print(con.execute("SELECT * FROM silver_mitma_ine_mapping LIMIT 10").fetchdf())

    return {
        "status": "success",
        "records": record_count,
        "table": "silver_mitma_ine_mapping"
    }
