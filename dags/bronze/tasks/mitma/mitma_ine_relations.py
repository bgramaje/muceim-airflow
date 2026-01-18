"""
Airflow task for loading MITMA-INE relations data into Bronze layer.
Reads directly from URL using DuckDB (lightweight operation, no Cloud Run needed).
"""

from airflow.sdk import task
from typing import Dict, Any


MITMA_INE_RELATIONS_URL = "https://movilidad-opendata.mitma.es/zonificacion/relacion_ine_zonificacionMitma.csv"
TABLE_NAME = 'bronze_mitma_ine_relations'


@task
def BRONZE_mitma_ine_relations():
    """
    Load MITMA-INE relations data directly from URL into Bronze layer.
    Creates or replaces the table with fresh data from the source.
    
    This is a lightweight operation that runs locally without Cloud Run.
    """
    from utils.utils import get_ducklake_connection
    
    print(f"[TASK] Loading MITMA-INE relations from: {MITMA_INE_RELATIONS_URL}")
    
    con = get_ducklake_connection()
    
    # Create or replace table reading directly from CSV URL
    sql_query = f"""
        CREATE OR REPLACE TABLE {TABLE_NAME} AS
        SELECT 
            seccion_ine,
            distrito_ine,
            municipio_ine,
            distrito_mitma,
            municipio_mitma,
            gau_mitma,
            CURRENT_TIMESTAMP AS loaded_at,
            '{MITMA_INE_RELATIONS_URL}' AS source_file
        FROM read_csv(
            '{MITMA_INE_RELATIONS_URL}',
            auto_detect=true,
            header=true,
            delim='|'
        );
    """
    
    con.execute(sql_query)
    
    # Verify the result
    count_result = con.execute(f"SELECT COUNT(*) as count FROM {TABLE_NAME}").fetchdf()
    record_count = int(count_result['count'].iloc[0])
    
    print(f"[TASK] Loaded {record_count:,} records into {TABLE_NAME}")
    print(con.execute(f"SELECT * FROM {TABLE_NAME} LIMIT 5").fetchdf())
    
    return {
        'status': 'success',
        'table_name': TABLE_NAME,
        'records': record_count,
        'source_url': MITMA_INE_RELATIONS_URL
    }


