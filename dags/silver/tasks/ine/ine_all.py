from airflow.sdk import task  # type: ignore

from utils.gcp import execute_sql_or_cloud_run
from utils.utils import get_ducklake_connection
from utils.logger import get_logger


@task
def SILVER_ine_all(**context):
    """
    Airflow task to create silver_ine_all table by joining zones with INE data.
    Adds year column extracted from params.start (YYYY-MM-DD format).
    """
    con = get_ducklake_connection()

    logger = get_logger(__name__, context)
    year = context['params'].get('start', '')[
        :4] if context['params'].get('start') else ''
    logger.info(f"Using year: {year}")

    query = f"""
        CREATE OR REPLACE TABLE silver_ine_all AS
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
        LEFT JOIN silver_ine_empresas_municipio m ON z.id = m.zone_id
        LEFT JOIN silver_ine_renta_municipio r ON z.id = r.zone_id
        LEFT JOIN silver_ine_poblacion_municipio p ON z.id = p.zone_id
    """

    try:
        con.execute(query)
        return {
            "status": "success",
            "table": "silver_ine_all"
        }
    except Exception as e:
        raise e
    finally:
        con.close()


@task
def CLEANUP_intermediate_ine_tables(**context):
    """
    Airflow task to drop intermediate INE tables.
    """
    logger = get_logger(__name__, context)
    logger.info("Dropping intermediate INE tables")

    con = get_ducklake_connection()

    tables_to_drop = [
        "silver_ine_empresas_municipio",
        "silver_ine_poblacion_municipio",
        "silver_ine_renta_municipio"
    ]

    for table in tables_to_drop:
        logger.info(f"Dropping table {table}")
        con.execute(f"DROP TABLE IF EXISTS {table}")

    return {
        "status": "success",
        "dropped_tables": tables_to_drop
    }
