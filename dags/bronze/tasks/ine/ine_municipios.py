"""
Airflow task for loading INE Municipios data into Bronze layer.
Uses Cloud Run executor to run SQL directly against INE API URLs.
"""

from airflow.sdk import task


@task
def BRONZE_ine_municipios_urls():
    """
    Generate the list of URLs for INE Municipios data.
    Returns a list with a single static URL.
    """
    url = 'https://servicios.ine.es/wstempus/js/ES/VALORES_VARIABLE/19'
    urls = [url]
    print(f"[TASK] Generated {len(urls)} URL(s) for INE Municipios data")
    return urls


@task
def BRONZE_ine_municipios_create_table(urls: list[str], **context):
    """
    Create the table for INE Municipios data if it doesn't exist.
    Uses Cloud Run executor to run CREATE TABLE directly from INE API URL.
    """
    from utils.gcp import execute_sql_or_cloud_run

    table_name = 'bronze_ine_municipios'
    
    if not urls:
        raise ValueError(f"No URLs provided to create table {table_name}")
    
    first_url = urls[0]
    print(f"[TASK] Creating table {table_name} if not exists, using first URL: {first_url}")
    
    # SQL to create table from JSON URL (no year for municipios - static reference table)
    sql_query = f"""
        -- Create table from INE API JSON response
        CREATE TABLE IF NOT EXISTS {table_name} AS
        SELECT 
            *,
            CURRENT_TIMESTAMP AS loaded_at,
            '{first_url}' AS source_url
        FROM read_json('{first_url}', format='array')
        LIMIT 0;
    """
    
    result = execute_sql_or_cloud_run(sql_query=sql_query, **context)
    
    return {'status': 'success', 'table_name': table_name, **result}


@task
def BRONZE_ine_municipios_filter_urls(urls: list[str]):
    """
    Filter URLs to only include those not already ingested.
    Queries the bronze table for existing source_url values and returns only new URLs.
    """
    from bronze.utils import ine_municipios_filter_urls

    return ine_municipios_filter_urls(urls)


@task
def BRONZE_ine_municipios_insert(url: str, **context):
    """
    Insert data from a single URL using Cloud Run executor.
    Runs MERGE directly against the INE API URL.
    """
    from utils.gcp import execute_sql_or_cloud_run

    print(f"[TASK] Processing URL: {url}")
    
    table_name = 'bronze_ine_municipios'
    
    # SQL to merge data from INE API JSON URL
    sql_query = f"""
        -- Merge data from INE API directly
        MERGE INTO {table_name} AS target
        USING (
            SELECT 
                *,
                CURRENT_TIMESTAMP AS loaded_at,
                '{url}' AS source_url
            FROM read_json('{url}', format='array')
        ) AS source
        ON target.Id = source.Id
        WHEN NOT MATCHED THEN
            INSERT *;
    """
    
    result = execute_sql_or_cloud_run(sql_query=sql_query, **context)
    
    return {
        'status': 'success',
        'url': url,
        **result
    }
