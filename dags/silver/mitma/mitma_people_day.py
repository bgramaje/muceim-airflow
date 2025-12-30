"""
Airflow task for loading MITMA People Day data into Silver layer.
Handles daily person-trip counts (personas) by zone, age, sex, and travel type,
for distritos, municipios, and GAU zone types.
"""

import sys
import os
from airflow.sdk import task  # type: ignore

# Add parent directory to path to import utils
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))


from utils.utils import get_ducklake_connection


@task
def SILVER_mitma_people_day():
    """
    Airflow task to transform and standardize MITMA People Day data
    into DuckDB Silver layer.

    Returns:
    - Dict with task status and info
    """
    print("[TASK] Building unified silver_people_day table")

    con = get_ducklake_connection()

    con.execute(f"""
        CREATE OR REPLACE TABLE silver_people_day AS
        WITH base AS (
            SELECT
                strptime(CAST(fecha AS VARCHAR), '%Y%m%d')::DATE as fecha,
                zona_pernoctacion,
                edad,
                sexo,
                numero_viajes,
                CAST(personas AS DOUBLE) as personas
            FROM bronze_mitma_people_day_municipios
        )
        SELECT *
        FROM base
        WHERE fecha IS NOT NULL
          AND zona_pernoctacion IS NOT NULL
          AND edad IS NOT NULL
          AND sexo IS NOT NULL
          AND numero_viajes IS NOT NULL
          AND personas IS NOT NULL
    """)

    count = con.execute("SELECT COUNT(*) AS count FROM silver_people_day").fetchdf()
    print(f"[TASK] Created silver_people_day with {count.iloc[0]['count']:,} records")

    print(con.execute("SELECT * FROM silver_people_day LIMIT 10").fetchdf())

    return {
        "status": "success",
        "records": int(count.iloc[0]['count']),
        "table": "silver_people_day"
    }
