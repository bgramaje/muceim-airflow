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
def BRONZE_mitma_od_download_to_rustfs(url: str, zone_type: str = 'distritos'):
    """
    Download a MITMA URL and upload it to RustFS bucket mitma-raw.
    This task runs before the insert task to ensure files are available in RustFS
    instead of relying on external URLs that may be blocked from Google Cloud.
    
    Returns:
    - S3 path in format s3://mitma-raw/{dataset}/{zone_type}/{filename}
    """
    from bronze.utils import download_url_to_rustfs
    
    dataset = 'od'
    s3_path = download_url_to_rustfs(url, dataset, zone_type)
    
    return {
        'url': url,
        's3_path': s3_path,
        'dataset': dataset,
        'zone_type': zone_type
    }


@task
def BRONZE_mitma_od_insert(download_result: dict, zone_type: str = 'distritos'):
    """
    Insert data from a RustFS S3 path using Google Cloud Run Job.
    The Cloud Run Job will read the CSV from RustFS S3 bucket and merge it into DuckDB.
    The job executes and terminates automatically.
    After successful merge, deletes the file from RustFS to free up space.
    
    Parameters:
    - download_result: Dict from BRONZE_mitma_od_download_to_rustfs with 's3_path' and 'url' keys
    - zone_type: Type of zone ('distritos', 'municipios', 'gau')
    """
    from utils.gcp import execute_cloud_run_job_merge_csv
    from bronze.utils import delete_file_from_rustfs

    dataset = 'od'
    table_name = f'bronze_mitma_{dataset}_{zone_type}'
    s3_path = download_result['s3_path']
    original_url = download_result.get('url')
    
    print(f"[TASK] Processing S3 path: {s3_path} into {table_name} via Google Cloud Run Job")
    if original_url:
        print(f"[TASK] Original URL: {original_url}")

    # Ejecutar el Cloud Run job
    result = execute_cloud_run_job_merge_csv(
        table_name=table_name, 
        url=s3_path, 
        is_s3_path=True,
        original_url=original_url
    )
    
    # Si el job completó exitosamente, eliminar el archivo de RustFS
    print(f"[TASK] Cloud Run job completed successfully, deleting file from RustFS: {s3_path}")
    delete_success = delete_file_from_rustfs(s3_path)
    
    if delete_success:
        print(f"[TASK] ✅ File deleted from RustFS successfully")
    else:
        print(f"[TASK] ⚠️ Failed to delete file from RustFS (continuing anyway)")

    return {
        'status': 'success',
        's3_path': s3_path,
        'original_url': original_url,
        'cloud_run_job_result': result,
        'table_name': table_name,
        'file_deleted': delete_success
    }
