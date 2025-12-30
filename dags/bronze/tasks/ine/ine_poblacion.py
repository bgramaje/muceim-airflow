"""
Airflow task for loading INE Poblacion per Municipio data into Bronze layer.
"""

from airflow.sdk import task


@task
def BRONZE_ine_poblacion_municipio_urls(year: int = 2023):
    """
    Generate the list of URLs for INE Poblacion data.
    Returns a list with a single URL for the specified year.
    
    Parameters:
    - year: Year to fetch data for (default: 2023)
    """
    url = f'https://servicios.ine.es/wstempus/js/ES/DATOS_TABLA/29005?date={year}0101:{year}1231&nult=1&det=2'
    urls = [url]
    print(f"[TASK] Generated {len(urls)} URL(s) for INE Poblacion data (year {year})")
    return urls


@task
def BRONZE_ine_poblacion_municipio_create_table(urls: list[str]):
    """
    Create the table for INE Poblacion data if it doesn't exist.
    Takes the first URL from the list to create the table schema.
    """
    from bronze.utils import create_table_from_json

    table_name = 'ine_poblacion_municipio'
    
    if not urls:
        raise ValueError(f"No URLs provided to create table {table_name}")
    
    first_url = urls[0]
    print(f"[TASK] Creating table {table_name} if not exists, using first URL: {first_url}")
    
    create_table_from_json(table_name, first_url)
    
    return {'status': 'success', 'table_name': table_name}


@task
def BRONZE_ine_poblacion_municipio_filter_urls(urls: list[str]):
    """
    Filter URLs to only include those not already ingested.
    Queries the bronze table for existing source_url values and returns only new URLs.
    """
    from bronze.utils import ine_poblacion_filter_urls

    return ine_poblacion_filter_urls(urls)


@task
def BRONZE_ine_poblacion_municipio_insert(url: str):
    """
    Insert data from a single URL using the merge function.
    Creates optimization indexes after successful insert for Silver layer queries.
    """
    from bronze.utils import merge_from_json
    from utils import get_ducklake_connection

    print(f"[TASK] Processing URL: {url}")
    
    table_name = 'ine_poblacion_municipio'
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
    # - Filter WHERE nombre IS NOT NULL (derived from Nombre)
    # - Processing of Nombre field in UNNEST operations
    con = get_ducklake_connection()
    try:
        con.execute(f"ANALYZE {full_table_name};")
        print(f"  ✅ Updated statistics for {full_table_name} (query optimization)")
    except Exception as analyze_error:
        print(f"  ⚠️ Could not analyze table (non-critical): {analyze_error}")
    
    return {
        'status': 'success',
        'url': url
    }
