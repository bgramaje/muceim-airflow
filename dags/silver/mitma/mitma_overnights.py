"""
Airflow task for loading MITMA overnight stay data into Silver layer.
Handles overnight stay counts for distritos, municipios, and GAU zone types,
with proper type casting, zone_level labeling, and filtering of incomplete rows.
"""

import sys
import os
from airflow.sdk import task  # type: ignore

# Add parent directory to path to import utils
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))


from utils import get_ducklake_connection


@task
def SILVER_mitma_overnight_stay():
    """
    Airflow task to transform and standardize overnight stay data into DuckDB Silver layer.

    Returns:
    - Dict with task status and info
    """
    print("[TASK] Building unified silver_overnight_stay table")

    con = get_ducklake_connection()

    con.execute("""
        CREATE OR REPLACE TABLE silver_overnight_stay AS
        WITH base AS (
            SELECT
                strptime(CAST(fecha AS VARCHAR), '%Y%m%d')::DATE as fecha,
                zona_pernoctacion,
                zona_residencia,
                CAST(personas AS DOUBLE) as personas
            FROM bronze_mitma_overnight_stay_municipios
        )
        SELECT *
        FROM base
        WHERE fecha IS NOT NULL
          AND zona_pernoctacion IS NOT NULL
          AND zona_residencia IS NOT NULL
          AND personas IS NOT NULL
    """)

    count = con.execute("SELECT COUNT(*) AS count FROM silver_overnight_stay").fetchdf()
    print(f"[TASK] Created silver_overnight_stay with {count.iloc[0]['count']:,} records")

    print(con.execute("SELECT * FROM silver_overnight_stay LIMIT 10").fetchdf())

    return {
        "status": "success",
        "records": int(count.iloc[0]['count']),
        "table": "silver_overnight_stay"
    }
