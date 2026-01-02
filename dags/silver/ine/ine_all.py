
import sys
import os
from airflow.sdk import task # type: ignore

# Add parent directory to path to import utils
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))))

from utils.gcp import execute_sql_or_cloud_run
from utils.utils import get_ducklake_connection

@task
def SILVER_ine_all(**context):
    """
    Airflow task to create silver_ine_all table by joining zones with INE data.
    Adds year column extracted from params.start (YYYY-MM-DD format).
    Executes using Cloud Run Job (ducklake-executor).
    """
    print("[TASK] Building silver_ine_all table using Cloud Run")
    
    # Extract year from DAG params
    year = context['params'].get('start', '')[:4] if context['params'].get('start') else ''
    print(f"[TASK] Using year: {year}")

    # User provided SQL from notebook
    sql_query = f"""
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
    
    result = execute_sql_or_cloud_run(sql_query=sql_query, **context)
    
    print(f"[TASK] silver_ine_all table built successfully")
    print(f"[TASK] Execution time: {result.get('execution_time_seconds', 0)} seconds")

    return {
        "status": "success",
        "table": "silver_ine_all",
        "year": year,
        "execution_name": result.get("execution_name"),
        "execution_time_seconds": result.get("execution_time_seconds")
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
