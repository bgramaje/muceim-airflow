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
def SILVER_mitma_people_day_create_table(**context):
    """
    Crea la tabla silver_people_day si no existe.
    silver_people_day está particionada por fecha para optimizar queries por fecha.
    
    Returns:
    - Dict con status de la creación de la tabla
    """
    print("[TASK] Creating silver_people_day table if it doesn't exist")
    
    con = get_ducklake_connection()
    
    # Crear tabla primero
    con.execute("""
        CREATE TABLE IF NOT EXISTS silver_people_day (
            fecha DATE,
            zona_pernoctacion VARCHAR,
            edad VARCHAR,
            sexo VARCHAR,
            numero_viajes VARCHAR,
            personas DOUBLE
        );
    """)
    
    # Configurar particionado por fecha (columna DATE - usar identity)
    # Para columnas DATE, DuckLake recomienda usar identity (nombre de columna directamente)
    # Las funciones year/month/day pueden causar problemas con columnas DATE
    con.execute("""
        ALTER TABLE silver_people_day SET PARTITIONED BY (fecha);
    """)
    
    print("[TASK] Table silver_people_day created/verified successfully")
    
    return {
        "status": "success",
        "table": "silver_people_day"
    }


@task
def SILVER_mitma_people_day_insert(**context):
    """
    Inserta datos transformados desde bronze_mitma_people_day_municipios a silver_people_day.
    
    Returns:
    - Dict with task status and info
    """
    print("[TASK] Inserting data into silver_people_day table")

    con = get_ducklake_connection()
    
    # Insertar datos
    con.execute("""
        INSERT INTO silver_people_day
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
          AND personas IS NOT NULL;
    """)

    count = con.execute("SELECT COUNT(*) AS count FROM silver_people_day").fetchdf()
    print(f"[TASK] Inserted {count.iloc[0]['count']:,} records into silver_people_day")

    print(con.execute("SELECT * FROM silver_people_day LIMIT 10").fetchdf())

    return {
        "status": "success",
        "records": int(count.iloc[0]['count']),
        "table": "silver_people_day"
    }
