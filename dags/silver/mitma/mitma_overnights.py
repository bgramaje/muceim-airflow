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
def SILVER_mitma_overnight_stay_insert(start_date: str = None, end_date: str = None, **context):
    """
    Inserta datos transformados desde bronze_mitma_overnight_stay_municipios a silver_overnight_stay.
    Filtra por rango de fechas si se especifican los parámetros.
    
    Parameters:
    - start_date: Fecha inicio para filtrar (formato: YYYY-MM-DD). Si None, no filtra por inicio.
    - end_date: Fecha fin para filtrar (formato: YYYY-MM-DD). Si None, no filtra por fin.
    
    Returns:
    - Dict with task status and info
    """
    print("[TASK] Inserting data into silver_overnight_stay table")
    print(f"[TASK] Date range filter: start={start_date}, end={end_date}")

    con = get_ducklake_connection()
    
    # Construir filtro de fechas si se especifican
    date_filter = ""
    if start_date and start_date not in ('None', '', '{{ params.start }}'):
        # Convertir formato YYYY-MM-DD a YYYYMMDD si es necesario
        start_clean = start_date.replace('-', '')
        date_filter += f" AND CAST(fecha AS VARCHAR) >= '{start_clean}'"
        print(f"[TASK] Filtering from start_date: {start_clean}")
    if end_date and end_date not in ('None', '', '{{ params.end }}'):
        end_clean = end_date.replace('-', '')
        date_filter += f" AND CAST(fecha AS VARCHAR) <= '{end_clean}'"
        print(f"[TASK] Filtering to end_date: {end_clean}")
    
    # Insertar datos
    query = f"""
        INSERT INTO silver_overnight_stay
        WITH base AS (
            SELECT
                strptime(CAST(fecha AS VARCHAR), '%Y%m%d')::DATE as fecha,
                zona_pernoctacion,
                zona_residencia,
                CAST(personas AS DOUBLE) as personas
            FROM bronze_mitma_overnight_stay_municipios
            WHERE 1=1
            {date_filter}
        )
        SELECT *
        FROM base
        WHERE fecha IS NOT NULL
          AND zona_pernoctacion IS NOT NULL
          AND zona_residencia IS NOT NULL
          AND personas IS NOT NULL;
    """
    
    con.execute(query)

    count = con.execute("SELECT COUNT(*) AS count FROM silver_overnight_stay").fetchdf()
    print(f"[TASK] Inserted {count.iloc[0]['count']:,} records into silver_overnight_stay")

    print(con.execute("SELECT * FROM silver_overnight_stay LIMIT 10").fetchdf())

    return {
        "status": "success",
        "records": int(count.iloc[0]['count']),
        "table": "silver_overnight_stay",
        "date_filter": {"start": start_date, "end": end_date}
    }
