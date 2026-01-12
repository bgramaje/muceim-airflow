"""
Airflow task for loading INE Empresas per Municipio data into Bronze layer.
Uses Cloud Run executor to run SQL directly against INE API URLs.
"""

from airflow.sdk import task


@task
def BRONZE_ine_empresas_municipio_urls(year: int = 2023):
    """
    Generate the list of URLs for INE Empresas data.
    Returns a list with a single URL for the specified year.
    
    Parameters:
    - year: Year to fetch data for (default: 2023)
    """
    url = f'https://servicios.ine.es/wstempus/js/ES/DATOS_TABLA/4721?date={year}0101:{year}1231&Tv=40621:248341&Tv=selCri_2:on'
    urls = [url]
    print(f"[TASK] Generated {len(urls)} URL(s) for INE Empresas data (year {year})")
    return urls


@task
def BRONZE_ine_empresas_municipio_create_table(urls: list[str], **context):
    """
    Create the table for INE Empresas data if it doesn't exist.
    Uses Cloud Run executor to run CREATE TABLE directly from INE API URL.
    
    Parameters:
    - urls: List of URLs to process
    """
    from utils.gcp import execute_sql_or_cloud_run

    table_name = 'bronze_ine_empresas_municipio'
    
    if not urls:
        raise ValueError(f"No URLs provided to create table {table_name}")
    
    first_url = urls[0]
    print(f"[TASK] Creating table {table_name} if not exists")
    
    # SQL to create table from JSON URL with year column as INTEGER
    sql_query = f"""
        CREATE TABLE IF NOT EXISTS {table_name} AS
        SELECT 
            *,
            CURRENT_TIMESTAMP AS loaded_at,
            '{first_url}' AS source_url,
            0 AS year
        FROM read_json('{first_url}', format='array')
        LIMIT 0;
    """
    
    result = execute_sql_or_cloud_run(sql_query=sql_query, **context)
    
    return {'status': 'success', 'table_name': table_name, **result}


@task
def BRONZE_ine_empresas_municipio_filter_urls(urls: list[str]):
    """
    Filter URLs to only include those not already ingested.
    Queries the bronze table for existing source_url values and returns only new URLs.
    """
    from bronze.utils import ine_empresas_filter_urls

    return ine_empresas_filter_urls(urls)


@task
def BRONZE_ine_empresas_municipio_insert(url: str, year: int = None, **context):
    """
    Insert data from a single URL using Cloud Run executor.
    Runs MERGE directly against the INE API URL.
    
    Parameters:
    - url: URL to process
    - year: Year to add to each row
    """
    from utils.gcp import execute_sql_or_cloud_run

    print(f"[TASK] Processing URL: {url}")
    print(f"[TASK] Year: {year}")
    
    table_name = 'bronze_ine_empresas_municipio'
    
    # SQL to merge data from INE API JSON URL
    sql_query = f"""
        -- Merge data from INE API directly
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
