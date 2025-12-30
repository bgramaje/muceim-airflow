"""
Airflow task for loading MITMA-INE relations data into Bronze layer.
"""

from airflow.sdk import task


@task
def BRONZE_mitma_ine_relations_urls():
    """
    Generate the list of URLs for MITMA-INE Relations data.
    Returns a list with a single static URL.
    """
    url = "https://movilidad-opendata.mitma.es/zonificacion/relacion_ine_zonificacionMitma.csv"
    urls = [url]
    print(f"[TASK] Generated {len(urls)} URL(s) for MITMA-INE Relations data")
    return urls


@task
def BRONZE_mitma_ine_relations_create_table(urls: list[str]):
    """
    Create the table for MITMA-INE Relations data if it doesn't exist.
    Takes the first URL from the list to create the table schema.
    """
    from bronze.utils import create_table_from_csv

    table_name = 'mitma_ine_relations'
    
    if not urls:
        raise ValueError(f"No URLs provided to create table {table_name}")
    
    first_url = urls[0]
    print(f"[TASK] Creating table {table_name} if not exists, using first URL: {first_url}")
    
    create_table_from_csv(table_name, first_url)
    
    return {'status': 'success', 'table_name': table_name}


@task
def BRONZE_mitma_ine_relations_filter_urls(urls: list[str]):
    """
    Filter URLs to only include those not already ingested.
    Queries the bronze table for existing source_file values and returns only new URLs.
    """
    from bronze.utils import mitma_ine_relations_filter_urls

    return mitma_ine_relations_filter_urls(urls)


@task
def BRONZE_mitma_ine_relations_insert(url: str):
    """
    Insert data from a single URL using the merge function.
    Creates optimization indexes after successful insert for Silver layer queries.
    """
    from bronze.utils import merge_from_csv
    from utils.utils import get_ducklake_connection

    print(f"[TASK] Processing URL: {url}")
    
    table_name = 'mitma_ine_relations'
    full_table_name = f'bronze_{table_name}'
    
    # Insert/merge data from this URL
    merge_from_csv(table_name, url)

    # Update statistics for query optimization (DuckDB alternative to indexes)
    # ANALYZE helps the query optimizer make better decisions for:
    # - JOIN ON m.Codigo = r.municipio_ine
    # - Filter WHERE r.municipio_mitma IS NOT NULL
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

