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
def BRONZE_mitma_od_process(url: str, zone_type: str = 'distritos'):
    """
    Download a MITMA URL, upload it to RustFS, and process it using Cloud Run Job.
    This combines download and insert operations to process each URL as soon as it's downloaded,
    avoiding the need to wait for all downloads to complete before starting processing.
    
    The Cloud Run Job will read the CSV from RustFS S3 bucket and merge it into DuckDB.
    After successful merge, deletes the file from RustFS to free up space.
    
    Parameters:
    - url: URL to download
    - zone_type: Type of zone ('distritos', 'municipios', 'gau')
    
    Returns:
    - Dict with processing results
    """
    from utils.gcp import merge_csv_or_cloud_run
    from bronze.utils import download_url_to_rustfs, delete_file_from_rustfs

    dataset = 'od'
    table_name = f'bronze_mitma_{dataset}_{zone_type}'
    
    print(f"[TASK] Downloading and uploading to RustFS: {url}")
    
    # Descargar y subir a RustFS
    s3_path = download_url_to_rustfs(url, dataset, zone_type)
    
    print(f"[TASK] File uploaded to RustFS: {s3_path}")
    print(f"[TASK] Processing S3 path: {s3_path} into {table_name}")

    # Ejecutar merge (Cloud Run o local según disponibilidad)
    result = merge_csv_or_cloud_run(
        table_name=table_name, 
        url=s3_path, 
        original_url=url
    )
    
    # Si el job completó exitosamente, eliminar el archivo de RustFS
    print(f"[TASK] Processing completed successfully, deleting file from RustFS: {s3_path}")
    delete_success = delete_file_from_rustfs(s3_path)
    
    if delete_success:
        print(f"[TASK] File deleted from RustFS successfully")
    else:
        print(f"[TASK] Failed to delete file from RustFS (continuing anyway)")

    return {
        'status': 'success',
        's3_path': s3_path,
        'original_url': url,
        'cloud_run_job_result': result,
        'table_name': table_name,
        'file_deleted': delete_success
    }
