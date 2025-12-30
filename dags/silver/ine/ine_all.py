
import sys
import os
from airflow.sdk import task # type: ignore

from utils.utils import get_ducklake_connection

@task
def SILVER_ine_all(**context):
    """
    Airflow task to create silver_ine_all table by joining zones with INE data.
    Adds year column extracted from params.start (YYYY-MM-DD format).
    """
    print("[TASK] Building silver_ine_all table")

    con = get_ducklake_connection()
    
    # Extract year from DAG params
    year = context['params'].get('start', '')[:4] if context['params'].get('start') else ''
    print(f"[TASK] Using year: {year}")

    # User provided SQL from notebook
    query = f"""
    CREATE OR REPLACE TABLE silver_ine_all AS (
        SELECT 
            z.id,
            z.nombre,
            COALESCE(m.empresas, 0) AS empresas,
            COALESCE(r.renta_media, 0) AS renta_media,
            COALESCE(p.poblacion_total, 0) AS poblacion_total,
            COALESCE(p.poblacion_hombres, 0) AS poblacion_hombres,
            COALESCE(p.poblacion_mujeres, 0) AS poblacion_mujeres,
            '{year}' AS year
        FROM silver_zones z
        LEFT JOIN silver_ine_empresas_municipio m
            ON z.id = m.zone_id
        LEFT JOIN silver_ine_renta_municipio r
            ON z.id = r.zone_id
        LEFT JOIN silver_ine_poblacion_municipio p
            ON z.id = p.zone_id
        ORDER BY z.id
    )
    """
    
    con.execute(query)

    # Verification
    count = con.execute("SELECT COUNT(*) as count FROM silver_ine_all").fetchdf()
    record_count = int(count['count'].iloc[0])
    
    print(f"[TASK] Created silver_ine_all with {record_count:,} records")
    print(con.execute("SELECT * FROM silver_ine_all LIMIT 10").fetchdf())

    return {
        "status": "success",
        "records": record_count,
        "table": "silver_ine_all",
        "year": year
    }

@task
def CLEANUP_intermediate_ine_tables():
    """
    Airflow task to drop intermediate INE tables.
    """
    print("[TASK] Dropping intermediate INE tables")

    con = get_ducklake_connection()

    tables_to_drop = [
        "silver_ine_empresas_municipio",
        "silver_ine_poblacion_municipio",
        "silver_ine_renta_municipio"
    ]

    for table in tables_to_drop:
        print(f"[TASK] Dropping table {table}")
        con.execute(f"DROP TABLE IF EXISTS {table}")

    return {
        "status": "success",
        "dropped_tables": tables_to_drop
    }
