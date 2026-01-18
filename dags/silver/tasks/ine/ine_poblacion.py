from airflow.sdk import task

from utils.utils import get_ducklake_connection
from utils.logger import get_logger


@task
def SILVER_ine_poblacion_municipio(**context):
    """
    Airflow task to create silver_ine_poblacion_municipio table.
    Adds year column extracted from params.start (YYYY-MM-DD format).
    """
    con = get_ducklake_connection()

    logger = get_logger(__name__, context)
    year = context['params'].get('start', '')[
        :4] if context['params'].get('start') else ''
    logger.info(f"Using year: {year}")

    query = f"""
        CREATE OR REPLACE TABLE silver_ine_poblacion_municipio AS (
        WITH poblacion_parsed AS (
            SELECT 
                replace(lower(strip_accents(
                    TRIM(
                        COALESCE(
                            NULLIF(split_part(split_part(p.Nombre, '.', 1), '/', 2), ''),
                            split_part(split_part(p.Nombre, '.', 1), '/', 1)
                        )
                    )
                )), '-', ' ') AS nombre,
                LOWER(TRIM(split_part(p.Nombre, '.', 2))) AS tipo,
                COALESCE(CAST(data_item.Valor AS DOUBLE), 0) AS valor
            FROM bronze_ine_poblacion_municipio p,
                UNNEST(p.Data) AS t(data_item)
            WHERE nombre IS NOT NULL
        ), 
        poblacion_mitma AS ( 
            SELECT 
                m.municipio_mitma AS zone_id,
                ep.nombre,
                ep.tipo,
                MAX(ep.valor) AS valor
            FROM silver_mitma_ine_mapping m
            LEFT JOIN poblacion_parsed ep
                ON (ep.nombre ILIKE m.nombre)
            GROUP BY ep.nombre, ep.tipo, m.municipio_mitma
        )
        SELECT 
            zone_id,
            COALESCE(SUM(CASE WHEN tipo = 'total' THEN valor ELSE 0 END), 0) AS poblacion_total,
            COALESCE(SUM(CASE WHEN tipo = 'hombres' THEN valor ELSE 0 END), 0) AS poblacion_hombres,
            COALESCE(SUM(CASE WHEN tipo = 'mujeres' THEN valor ELSE 0 END), 0) AS poblacion_mujeres,
            '{year}' AS year
        FROM poblacion_mitma
        GROUP BY zone_id
    )
    """

    try:
        con.execute(query)
        return {
            "status": "success",
            "table": "silver_ine_poblacion_municipio"
        }
    except Exception as e:
        raise e
    finally:
        con.close()
