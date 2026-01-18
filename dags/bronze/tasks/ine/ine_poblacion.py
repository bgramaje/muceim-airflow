"""
Airflow task for loading INE Poblacion per Municipio data into Bronze layer.
Uses Cloud Run executor to run SQL directly against INE API URLs.
"""

from airflow.sdk import task
from utils.logger import get_logger


@task
def BRONZE_ine_poblacion_municipio_urls(year: int = 2023):
    """
    Generate the list of URLs for INE Poblacion data.
    Returns a list with a single URL for the specified year.

    Parameters:
    - year: Year to fetch data for (default: 2023)
    """
    url = f'https://servicios.ine.es/wstempus/js/ES/DATOS_TABLA/29005?date={year}0101:{year}1231&nult=1&det=2'
    return [url]


@task
def BRONZE_ine_poblacion_municipio_create_table(urls: list[str], **context):
    """
    Create the table for INE Poblacion data if it doesn't exist.
    Uses Cloud Run executor to run CREATE TABLE directly from INE API URL.

    Parameters:
    - urls: List of URLs to process
    """
    from utils.gcp import execute_sql_or_cloud_run

    table_name = 'bronze_ine_poblacion_municipio'

    if not urls or len(urls) == 0:
        raise ValueError(f"No URLs provided to create table {table_name}")

    logger = get_logger(__name__, context)
    logger.info(f"Creating table {table_name} if not exists")

    # SQL to create table from JSON URL with year column as INTEGER
    sql_query = f"""
        CREATE TABLE IF NOT EXISTS {table_name} AS
        SELECT 
            *,
            CURRENT_TIMESTAMP AS loaded_at,
            '{urls[0]}' AS source_url,
            0 AS year
        FROM read_json('{urls[0]}', format='array')
        LIMIT 0;
    """

    result = execute_sql_or_cloud_run(sql_query=sql_query, **context)

    return {'status': 'success', 'table_name': table_name, **result}


@task
def BRONZE_ine_poblacion_municipio_filter_urls(urls: list[str]):
    """
    Filter URLs to only include those not already ingested.
    Queries the bronze table for existing source_url values and returns only new URLs.
    """
    from bronze.utils import filter_json_urls

    table_name = 'bronze_ine_poblacion_municipio'
    return filter_json_urls(table_name, urls)


@task
def BRONZE_ine_poblacion_municipio_insert(url: str, year: int = None, **context):
    """
    Insert data from a single URL using Cloud Run executor.
    Runs MERGE directly against the INE API URL.

    Parameters:
    - url: URL to process
    - year: Year to add to each row
    """
    from utils.gcp import execute_sql_or_cloud_run

    table_name = 'bronze_ine_poblacion_municipio'

    sql_query = f"""
        MERGE INTO {table_name} AS target
        USING (
            SELECT 
                *,
                CURRENT_TIMESTAMP AS loaded_at,
                '{url}' AS source_url,
                {year} AS year
            FROM read_json('{url}', format='array')
        ) AS source
        ON target.COD = source.COD AND target.year = source.year
        WHEN NOT MATCHED THEN
            INSERT *;
    """

    result = execute_sql_or_cloud_run(sql_query=sql_query, **context)

    return {
        'status': 'success',
        'url': url,
        'year': year,
        **result
    }
