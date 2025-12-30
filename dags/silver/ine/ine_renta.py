import sys
import os
from airflow.sdk import task  # type: ignore

from utils.utils import get_ducklake_connection


@task
def SILVER_ine_renta():
    """
    Airflow task to create silver_ine_renta_municipio table.
    """
    print("[TASK] Building silver_ine_renta_municipio table")

    con = get_ducklake_connection()

    # User provided SQL from notebook
    query = f"""
        CREATE OR REPLACE TABLE silver_ine_renta_municipio AS (
        WITH renta_parsed AS (
            SELECT 
                replace(lower(strip_accents(
                    TRIM(
                        COALESCE(
                            NULLIF(split_part(split_part(Nombre, '.', 1), '/', 2), ''),
                            split_part(split_part(Nombre, '.', 1), '/', 1)
                        )
                    )
                )), '-', ' ') AS nombre,
                LOWER(TRIM(split_part(Nombre, '.', 3))) AS tipo,
                CAST(data_item.Valor AS DOUBLE) AS valor
            FROM bronze_ine_renta_municipio,
                UNNEST(Data) AS t(data_item)
            WHERE tipo = 'renta neta media por persona'
            AND nombre IS NOT NULL
            AND nombre NOT ILIKE '%sección%' 
            AND nombre NOT ILIKE '%distrito%'
        ), 
        renta_mitma AS (
            SELECT DISTINCT ON (m.municipio_mitma)
                m.municipio_mitma AS zone_id,
                ep.tipo,
                ep.valor
            FROM silver_mitma_ine_mapping m
            LEFT JOIN renta_parsed ep
                ON (ep.nombre ILIKE m.nombre)
        )
        SELECT 
            zone_id,
            tipo,
            COALESCE(ROUND(AVG(valor), 2), 0) AS renta_media
        FROM renta_mitma
        GROUP BY zone_id, tipo    
    )
    """
    
    con.execute(query)

    # Verification
    count = con.execute("SELECT COUNT(*) as count FROM silver_ine_renta_municipio").fetchdf()
    record_count = int(count['count'].iloc[0])
    
    print(f"[TASK] Created silver_ine_renta_municipio with {record_count:,} records")

    return {
        "status": "success",
        "records": record_count,
        "table": "silver_ine_renta_municipio"
    }

