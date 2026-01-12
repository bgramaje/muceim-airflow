"""
Airflow task for loading MITMA overnight_stay (pernoctaciones) data into Bronze layer.
Handles overnight stay data for distritos, municipios, and GAU zone types.
"""

from airflow.sdk import task  # type: ignore


@task
def BRONZE_mitma_overnight_stay_urls(zone_type: str = 'distritos', start_date: str = None, end_date: str = None):
    """
    Generate the list of URLs for MITMA overnight_stay data.
    """
    from bronze.utils import get_mitma_urls

    dataset = 'overnight_stay'
    urls = get_mitma_urls(dataset, zone_type, start_date, end_date)
    print(f"[TASK] Generated {len(urls)} URLs for {dataset} {zone_type}")
    return urls


@task
def BRONZE_mitma_overnight_stay_create_table(urls: list[str], zone_type: str = 'distritos'):
    """
    Create the table for MITMA overnight_stay data if it doesn't exist.
    Takes the first URL from the list to create the table schema.
    """
    from bronze.utils import mitma_create_table

    dataset = 'overnight_stay'
    return mitma_create_table(dataset, zone_type, urls)


@task
def BRONZE_mitma_overnight_stay_filter_urls(urls: list[str], zone_type: str = 'distritos'):
    """
    Filter URLs to only include those not already ingested.
    Queries the bronze table for existing source_file values and returns only new URLs.
    """
    from bronze.utils import mitma_filter_urls

    dataset = 'overnight_stay'
    return mitma_filter_urls(dataset, zone_type, urls)


@task
def BRONZE_mitma_overnight_stay_insert(url: str, zone_type: str = 'distritos'):
    """
    Insert data from a single URL using the new merge function.
    Creates optimization indexes after successful insert for Silver layer queries.
    """
    from bronze.utils import merge_from_csv
    from utils.utils import get_ducklake_connection

    dataset = 'overnight_stay'
    table_name = f'mitma_{dataset}_{zone_type}'
    full_table_name = f'bronze_{table_name}'
    print(f"[TASK] Processing URL: {url} into {table_name}")

    merge_from_csv(table_name, url)

    return {'status': 'success', 'url': url}
