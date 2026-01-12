"""
Airflow task for loading INE Renta per Municipio data into Bronze layer.
Uses Cloud Run executor to run SQL directly against INE API URLs.
Uses dynamic task mapping to process each URL in parallel.
"""

from airflow.sdk import task



@task
def BRONZE_ine_renta_urls(year: int = 2023):
    """
    Generate the list of URLs for INE Renta data.
    Returns a list of 52 URLs to be processed by dynamic task mapping.
    """
    # All table IDs for renta media and mediana indicators
    # Source: https://servicios.ine.es/wstempus/js/ES/TABLAS_OPERACION/353
    base_ids = [
        30656, 30833, 30842, 30851, 30860, 30869, 30878, 30887, 30896,
        30917, 30926, 30935, 30944, 30953, 30962, 30971, 30980, 30989, 30998,
        31007, 31016, 31025, 31034, 31043, 31052, 31061, 31070, 31079, 31088,
        31097, 31106, 31115, 31124, 31133, 31142, 31151, 31160, 31169, 31178,
        31187, 31196, 31205, 31214, 31223, 31232, 31241, 31250, 31259, 31268,
        31277, 31286, 31295
    ]
    
    # Build URLs for all table IDs
    urls = [f'https://servicios.ine.es/wstempus/js/ES/DATOS_TABLA/{id}?date={year}0101' for id in base_ids]
    print(f"[TASK] Generated {len(urls)} URLs for INE Renta data (year {year})")
    return urls


@task
def BRONZE_ine_renta_create_table(urls: list[str], **context):
    """
    Create the table for INE Renta data if it doesn't exist.
    Uses Cloud Run executor to run CREATE TABLE directly from INE API URL.
    
    Parameters:
    - urls: List of URLs to process
    """
    from utils.gcp import execute_sql_or_cloud_run

    table_name = 'bronze_ine_renta_municipio'
    
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
def BRONZE_ine_renta_filter_urls(urls: list[str]):
    """
    Filter URLs to only include those not already ingested.
    Queries the bronze table for existing source_url values and returns only new URLs.
    """
    from bronze.utils import ine_renta_filter_urls

    return ine_renta_filter_urls(urls)


@task
def BRONZE_ine_renta_insert(url: str, year: int = None, **context):
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
    
    table_name = 'bronze_ine_renta_municipio'
    
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
