"""
Airflow task for building the unified MITMA OD Silver table.
Includes:
- Type casting
- Weekend / holiday flags
- Null filtering of critical fields
- zone_level standardization
"""

import sys
import os
from airflow.sdk import task  # type: ignore

# Add parent directory to path to import utils
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from utils import get_ducklake_connection


@task
def SILVER_mitma_od():
    """
    Creates the unified silver_od table from all Bronze MITMA OD tables.
    Requires that the TEMP table `bronze_spanish_holidays` already exists in the session.

    Returns:
    - Dict with task status and metadata
    """
    print("[TASK] Building silver_od unified OD table")

    con = get_ducklake_connection()

    con.execute("""
        CREATE TABLE IF NOT EXISTS silver_od AS
        WITH base AS (
            SELECT
                strptime(fecha::VARCHAR || LPAD(periodo::VARCHAR, 2, '0'), '%Y%m%d%H') as fecha,
                origen AS origen_zone_id,
                destino AS destino_zone_id,
                CAST(viajes AS DOUBLE)    AS viajes,
                CAST(viajes_km AS DOUBLE) AS viajes_km,
                distancia,
                residencia,
            FROM bronze_mitma_od_municipios
        ),
        filtered AS (
            SELECT *
            FROM base
            WHERE 
                fecha IS NOT NULL
                AND (origen_zone_id IS NOT NULL AND origen_zone_id != 'externo')
                AND (destino_zone_id IS NOT NULL AND destino_zone_id != 'externo')
                AND viajes IS NOT NULL
                AND viajes_km IS NOT NULL
                AND distancia IS NOT NULL
                AND residencia IS NOT NULL
        )
        SELECT DISTINCT(*) FROM filtered;
    """)

    # Count & preview
    df = con.execute("SELECT COUNT(*) AS count FROM silver_od_all").fetchdf()
    print(f"[TASK] Created silver_od_all with {df.iloc[0]['count']:,} records")

    print("[TASK] Sample preview:")
    print(con.execute("SELECT * FROM silver_od LIMIT 10").fetchdf())

    return {
        "status": "success",
        "records": int(df.iloc[0]["count"]),
        "table": "silver_od"
    }
