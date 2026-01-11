"""
Airflow task for loading MITMA OD (Origin-Destination) matrices into Bronze layer.
Handles viajes (trips) data for distritos, municipios, and GAU zone types.
Uses dynamic task mapping to process urls in parallel.

V2 Features:
- Batch download with parallel processing
- Streaming downloads with retry and exponential backoff
- Partitioned tables by year/month/day
- Optimized bulk INSERT
"""

from airflow.sdk import task
from typing import List, Dict, Any


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


# ============================================================================
# V2: NEW TASKS FOR OPTIMIZED BATCH PROCESSING
# ============================================================================

@task
def BRONZE_mitma_od_get_and_filter_urls(
    zone_type: str = 'municipios',
    start_date: str = None,
    end_date: str = None
) -> List[str]:
    """
    V2: Combined task that gets URLs and filters already ingested ones.
    More efficient than separate tasks.
    """
    from bronze.utils import get_mitma_urls, mitma_filter_urls
    
    dataset = 'od'
    
    print(f"[TASK_V2] Getting OD URLs for {zone_type} from {start_date} to {end_date}")
    urls = get_mitma_urls(dataset, zone_type, start_date, end_date)
    
    if not urls:
        print(f"[TASK_V2] No URLs found")
        return []
    
    print(f"[TASK_V2] Found {len(urls)} total URLs, filtering...")
    filtered = mitma_filter_urls(dataset, zone_type, urls)
    
    print(f"[TASK_V2] {len(filtered)} new URLs to process")
    return filtered


@task
def BRONZE_mitma_od_create_url_batches(
    urls: List[str],
    batch_size: int = 10
) -> List[Dict[str, Any]]:
    """
    V2: Creates batches of URLs for parallel processing.
    """
    from bronze.utils import create_url_batches
    
    if not urls:
        return []
    
    batches = create_url_batches(urls, batch_size)
    
    return [
        {'batch_index': i, 'urls': batch}
        for i, batch in enumerate(batches)
    ]


@task
def BRONZE_mitma_od_create_partitioned_table(
    urls: List[str],
    zone_type: str = 'municipios'
) -> Dict[str, Any]:
    """
    V2: Creates a partitioned table for OD data.
    Partitions by year/month/day of the fecha column.
    """
    from bronze.utils import create_partitioned_table_from_csv
    
    if not urls:
        return {'status': 'skipped', 'reason': 'no_urls'}
    
    dataset = 'od'
    table_name = f'mitma_{dataset}_{zone_type}'
    
    print(f"[TASK_V2] Creating partitioned table bronze_{table_name}")
    return create_partitioned_table_from_csv(
        table_name=table_name,
        url=urls[0],
        partition_by_date=True
    )


@task
def BRONZE_mitma_od_download_batch(
    batch: Dict[str, Any],
    zone_type: str = 'municipios',
    max_parallel: int = 4
) -> Dict[str, Any]:
    """
    V2: Downloads a batch of URLs to RustFS with parallel processing.
    Includes retry with exponential backoff.
    """
    from bronze.utils import download_batch_to_rustfs
    
    dataset = 'od'
    urls = batch.get('urls', [])
    batch_index = batch.get('batch_index', 0)
    
    if not urls:
        return {
            'batch_index': batch_index,
            'downloaded': [],
            'failed': []
        }
    
    print(f"[TASK_V2] Downloading batch {batch_index}: {len(urls)} URLs")
    
    results = download_batch_to_rustfs(urls, dataset, zone_type, max_parallel)
    
    downloaded = [
        {'original_url': url, 's3_path': s3_path}
        for url, s3_path in results.items()
    ]
    
    failed = [url for url in urls if url not in results]
    
    return {
        'batch_index': batch_index,
        'downloaded': downloaded,
        'failed': failed
    }


@task
def BRONZE_mitma_od_process_batch(
    download_result: Dict[str, Any],
    zone_type: str = 'municipios'
) -> Dict[str, Any]:
    """
    V2: Processes downloaded files from RustFS into DuckDB.
    """
    from utils.gcp import merge_csv_or_cloud_run
    
    dataset = 'od'
    downloaded = download_result.get('downloaded', [])
    batch_index = download_result.get('batch_index', 0)
    
    if not downloaded:
        return {
            'batch_index': batch_index,
            'status': 'skipped',
            'processed': 0
        }
    
    table_name = f'mitma_{dataset}_{zone_type}'
    processed = 0
    errors = []
    
    print(f"[TASK_V2] Processing batch {batch_index}: {len(downloaded)} files")
    
    for item in downloaded:
        try:
            merge_csv_or_cloud_run(
                table_name=table_name,
                url=item['s3_path'],
                original_url=item['original_url']
            )
            processed += 1
        except Exception as e:
            print(f"[TASK_V2] Error: {e}")
            errors.append({'s3_path': item['s3_path'], 'error': str(e)})
    
    return {
        'batch_index': batch_index,
        'status': 'success' if not errors else 'partial',
        'processed': processed,
        'errors': errors
    }


@task
def BRONZE_mitma_od_cleanup_batch(download_result: Dict[str, Any]) -> Dict[str, Any]:
    """
    V2: Cleans up files from RustFS after processing.
    """
    from bronze.utils import delete_batch_from_rustfs
    
    downloaded = download_result.get('downloaded', [])
    batch_index = download_result.get('batch_index', 0)
    
    if not downloaded:
        return {'batch_index': batch_index, 'deleted': 0}
    
    s3_paths = [item['s3_path'] for item in downloaded]
    
    print(f"[TASK_V2] Cleaning up batch {batch_index}: {len(s3_paths)} files")
    
    results = delete_batch_from_rustfs(s3_paths)
    deleted = sum(1 for v in results.values() if v)
    
    return {'batch_index': batch_index, 'deleted': deleted}


@task
def BRONZE_mitma_od_finalize(
    zone_type: str,
    process_results: List[Dict[str, Any]]
) -> Dict[str, Any]:
    """
    V2: Finalizes the table after all batches are processed.
    Runs ANALYZE once at the end for optimal performance.
    """
    from bronze.utils import finalize_table
    
    dataset = 'od'
    table_name = f'bronze_mitma_{dataset}_{zone_type}'
    
    total_processed = sum(r.get('processed', 0) for r in process_results if r)
    
    if total_processed == 0:
        return {'status': 'skipped', 'reason': 'no_records'}
    
    print(f"[TASK_V2] Finalizing {table_name} ({total_processed} records processed)")
    
    return finalize_table(table_name, run_analyze=True)


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
