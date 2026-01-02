"""
Airflow task for loading INE Renta per Municipio data into Bronze layer.
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
def BRONZE_ine_renta_create_table(urls: list[str]):
    """
    Create the table for INE Renta data if it doesn't exist.
    This is a pre-task that runs once before the dynamic insertion.
    Takes the first URL from the list to create the table schema.
    """
    from bronze.utils import create_table_from_json

    table_name = 'ine_renta_municipio'
    
    if not urls:
        raise ValueError(f"No URLs provided to create table {table_name}")
    
    first_url = urls[0]
    print(f"[TASK] Creating table {table_name} if not exists, using first URL: {first_url}")
    
    create_table_from_json(table_name, first_url)
    
    return {'status': 'success', 'table_name': table_name}


@task
def BRONZE_ine_renta_filter_urls(urls: list[str]):
    """
    Filter URLs to only include those not already ingested.
    Queries the bronze table for existing source_url values and returns only new URLs.
    """
    from bronze.utils import ine_renta_filter_urls

    return ine_renta_filter_urls(urls)


@task
def BRONZE_ine_renta_insert(url: str):
    """
    Insert data from a single URL using the new merge function.
    Creates optimization indexes after successful insert for Silver layer queries.
    """
    from bronze.utils import merge_from_json
    from utils.utils import get_ducklake_connection

    print(f"[TASK] Processing URL: {url}")
    
    table_name = 'ine_renta_municipio'
    full_table_name = f'bronze_{table_name}'
    
    # Insert/merge data from this URL
    # Use 'COD' as the primary key
    merge_from_json(
        table_name, 
        url,
        key_columns=['COD']
    )

    # Update statistics for query optimization (DuckDB alternative to indexes)
    # ANALYZE helps the query optimizer make better decisions for:
    # - Processing of Nombre field in UNNEST operations with filters
    # - Filter conditions on Nombre
    # - JOIN operations
    con = get_ducklake_connection()
    try:
        con.execute(f"ANALYZE {full_table_name};")
        print(f"  Updated statistics for {full_table_name} (query optimization)")
    except Exception as analyze_error:
        print(f"  Could not analyze table (non-critical): {analyze_error}")
    
    return {
        'status': 'success',
        'url': url
    }
