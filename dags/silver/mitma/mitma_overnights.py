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


from utils.utils import get_ducklake_connection


@task
def SILVER_mitma_overnight_stay_create_table(**context):
    """
    Crea la tabla silver_overnight_stay si no existe.
    silver_overnight_stay está particionada por fecha para optimizar queries por fecha.
    
    Returns:
    - Dict con status de la creación de la tabla
    """
    print("[TASK] Creating silver_overnight_stay table if it doesn't exist")
    
    con = get_ducklake_connection()
    
    # Crear tabla primero
    con.execute("""
        CREATE TABLE IF NOT EXISTS silver_overnight_stay (
            fecha DATE,
            zona_pernoctacion VARCHAR,
            zona_residencia VARCHAR,
            personas DOUBLE
        );
    """)
    
    # Configurar particionado por fecha (columna DATE - usar identity)
    # Para columnas DATE, DuckLake recomienda usar identity (nombre de columna directamente)
    # Las funciones year/month/day pueden causar problemas con columnas DATE
    con.execute("""
        ALTER TABLE silver_overnight_stay SET PARTITIONED BY (fecha);
    """)
    
    print("[TASK] Table silver_overnight_stay created/verified successfully")
    
    return {
        "status": "success",
        "table": "silver_overnight_stay"
    }


@task
def SILVER_mitma_overnight_stay_insert(**context):
    """
    Inserta datos transformados desde bronze_mitma_overnight_stay_municipios a silver_overnight_stay.
    
    Returns:
    - Dict with task status and info
    """
    print("[TASK] Inserting data into silver_overnight_stay table")

    con = get_ducklake_connection()
    
    # Insertar datos
    con.execute("""
        INSERT INTO silver_overnight_stay
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
          AND personas IS NOT NULL;
    """)

    count = con.execute("SELECT COUNT(*) AS count FROM silver_overnight_stay").fetchdf()
    print(f"[TASK] Inserted {count.iloc[0]['count']:,} records into silver_overnight_stay")

    print(con.execute("SELECT * FROM silver_overnight_stay LIMIT 10").fetchdf())

    return {
        "status": "success",
        "records": int(count.iloc[0]['count']),
        "table": "silver_overnight_stay"
    }
