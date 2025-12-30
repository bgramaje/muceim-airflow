"""
Airflow task for loading MITMA people_day (personas por día) data into Bronze layer.
Handles daily people movement data for distritos, municipios, and GAU zone types.
"""

from airflow.sdk import task


@task
def BRONZE_mitma_people_day_urls(zone_type: str = 'distritos', start_date: str = None, end_date: str = None):
    """
    Generate the list of URLs for MITMA people_day data.
    """
    from bronze.utils import get_mitma_urls

    dataset = 'people_day'
    urls = get_mitma_urls(dataset, zone_type, start_date, end_date)
    print(f"[TASK] Generated {len(urls)} URLs for {dataset} {zone_type}")
    return urls


@task
def BRONZE_mitma_people_day_create_table(urls: list[str], zone_type: str = 'distritos'):
    """
    Create the table for MITMA people_day data if it doesn't exist.
    Takes the first URL from the list to create the table schema.
    """
    from bronze.utils import mitma_create_table

    dataset = 'people_day'
    return mitma_create_table(dataset, zone_type, urls)


@task
def BRONZE_mitma_people_day_filter_urls(urls: list[str], zone_type: str = 'distritos'):
    """
    Filter URLs to only include those not already ingested.
    Queries the bronze table for existing source_file values and returns only new URLs.
    """
    from bronze.utils import mitma_filter_urls

    dataset = 'people_day'
    return mitma_filter_urls(dataset, zone_type, urls)


@task
def BRONZE_mitma_people_day_insert(url: str, zone_type: str = 'distritos'):
    """
    Insert data from a single URL using the new merge function.
    Creates optimization indexes after successful insert for Silver layer queries.
    """
    from bronze.utils import merge_from_csv
    from utils import get_ducklake_connection

    dataset = 'people_day'
    table_name = f'mitma_{dataset}_{zone_type}'
    full_table_name = f'bronze_{table_name}'
    print(f"[TASK] Processing URL: {url} into {table_name}")

    merge_from_csv(table_name, url)

    # Update statistics for query optimization (DuckDB alternative to indexes)
    # ANALYZE helps the query optimizer make better decisions for:
    # - Filtering on fecha, zona_pernoctacion
    # - JOIN operations
    # - SELECT DISTINCT operations
    con = get_ducklake_connection()
    try:
        con.execute(f"ANALYZE {full_table_name};")
        print(f"  ✅ Updated statistics for {full_table_name} (query optimization)")
    except Exception as analyze_error:
        print(f"  ⚠️ Could not analyze table (non-critical): {analyze_error}")

    return {'status': 'success', 'url': url}
