"""
Airflow task for loading MITMA OD (Origin-Destination) matrices into Bronze layer.
Handles viajes (trips) data for distritos, municipios, and GAU zone types.
Uses dynamic task mapping to process urls in parallel.
"""

from airflow.sdk import task


@task
def BRONZE_mitma_od_urls(zone_type: str = 'distritos', start_date: str = None, end_date: str = None):
    """
    Generate the list of URLs for MITMA OD data.
    """
    from bronze.utils import get_mitma_urls

    dataset = 'od'
    urls = get_mitma_urls(dataset, zone_type, start_date, end_date)
    print(f"[TASK] {len(urls)} URLs found for {dataset} {zone_type}")
    return urls


@task
def BRONZE_mitma_od_create_table(urls: list[str], zone_type: str = 'distritos'):
    """
    Create the table for MITMA OD data if it doesn't exist.
    This is a pre-task that runs once before the dynamic insertion.
    Takes the first URL from the list to create the table schema.
    """
    from bronze.utils import mitma_create_table

    dataset = 'od'
    return mitma_create_table(dataset, zone_type, urls)


@task
def BRONZE_mitma_od_filter_urls(urls: list[str], zone_type: str = 'distritos'):
    """
    Filter URLs to only include those not already ingested.
    Queries the bronze table for existing source_file values and returns only new URLs.
    """
    from bronze.utils import mitma_filter_urls

    dataset = 'od'
    return mitma_filter_urls(dataset, zone_type, urls)


@task
def BRONZE_mitma_od_insert(url: str, zone_type: str = 'distritos'):
    """
    Insert data from a single URL using Google Cloud Run Job.
    The Cloud Run Job will download the CSV using Google Cloud's high-bandwidth connection
    and merge it into DuckDB. The job executes and terminates automatically.
    After successful merge, updates table statistics for optimization.
    """
    from utils.gcp import execute_cloud_run_job_merge_csv

    dataset = 'od'
    table_name = f'bronze_mitma_{dataset}_{zone_type}'
    print(
        f"[TASK] Processing URL: {url} into {table_name} via Google Cloud Run Job")

    result = execute_cloud_run_job_merge_csv(table_name, url)

    return {
        'status': 'success',
        'url': url,
        'cloud_run_job_result': result,
        'table_name': table_name
    }
