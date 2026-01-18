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
from utils.logger import get_logger


@task
def BRONZE_mitma_od_urls(
    zone_type: str = 'municipios',
    start_date: str = None,
    end_date: str = None,
    **context
) -> List[str]:
    """
    Single task that returns ONLY the URLs that must be processed for the requested date range:
    Fallback: if the "inserted dates" query fails (e.g., table not created yet),
    it returns URLs for the whole requested date range.
    """
    from utils.utils import get_ducklake_connection
    from bronze.utils import get_mitma_urls

    if not start_date:
        start_date = context.get('params', {}).get('start')
    if not end_date:
        end_date = context.get('params', {}).get('end')

    if not start_date or not end_date:
        raise ValueError("start_date and end_date are required")

    con = get_ducklake_connection()

    series_sql = f"""
        SELECT unnest(generate_series(
            TIMESTAMP '{start_date} 00:00:00',
            TIMESTAMP '{end_date} 23:59:59',
            INTERVAL '1 day'
        )) AS timestamp
    """

    df_requested = None
    try:
        df_requested = con.execute(f"""
            WITH requested AS (
                SELECT DISTINCT strftime(timestamp, '%Y%m%d') AS fecha
                FROM ({series_sql})
            ),
            inserted AS (
                SELECT DISTINCT strftime(fecha, '%Y%m%d') AS fecha
                FROM bronze_mitma_od_{zone_type}
                WHERE fecha IS NOT NULL
                  AND fecha BETWEEN TIMESTAMP '{start_date} 00:00:00'
                                AND TIMESTAMP '{end_date} 23:59:59'
            )
            SELECT fecha
            FROM requested
            EXCEPT
            SELECT fecha
            FROM inserted
            ORDER BY fecha
        """).fetchdf()

    except Exception as e:
        logger = get_logger(__name__, context)
        logger.warning(f"Could not compute inserted dates. Fallback to full range. Error: {e}")
        df_requested = con.execute(f"""
            SELECT DISTINCT strftime(timestamp, '%Y%m%d') AS fecha
            FROM ({series_sql})
            ORDER BY fecha
        """).fetchdf()


    logger = get_logger(__name__, context)
    missing_dates = df_requested['fecha'].tolist() if not df_requested.empty else []
    logger.info(f"Missing dates to process: {len(missing_dates)}")

    if not missing_dates:
        return []

    return get_mitma_urls('od', zone_type, missing_dates)

@task
def BRONZE_mitma_od_create_table(
    urls: List[str],
    zone_type: str = 'municipios'
) -> Dict[str, Any]:
    """
    Creates a table for OD data.
    Partitions by year/month/day of the fecha TIMESTAMP column (like silver).
    """
    from bronze.utils import create_table_from_csv
    
    if not urls:
        return {'status': 'skipped', 'reason': 'no_urls'}
    
    logger = get_logger(__name__)
    dataset = 'od'
    table_name = f'bronze_mitma_{dataset}_{zone_type}'
    
    logger.info(f"Creating partitioned table bronze_{table_name} with fecha as TIMESTAMP")
    return create_table_from_csv(
        table_name=table_name,
        url=urls[0],
        partition_by_date=True,
        fecha_as_timestamp=True
    )


@task
def BRONZE_mitma_od_download_batch(
    batch: Dict[str, Any],
    zone_type: str = 'municipios',
    **context
) -> Dict[str, Any]:
    """
    Downloads a batch of URLs to RustFS sequentially (one by one).    
    Downloads are done sequentially to reduce local resource usage.
    """
    from bronze.utils import download_batch_to_rustfs
    from airflow.sdk import Variable
    
    bucket = Variable.get('RAW_BUCKET', default='mitma-raw')
    
    dataset = 'od'
    urls = batch.get('urls', [])
    batch_index = batch.get('batch_index', 0)
    
    if not urls:
        return {
            'batch_index': batch_index,
            'downloaded': [],
            'failed': []
        }
    
    logger = get_logger(__name__, context)
    logger.info(f"Downloading batch {batch_index}: {len(urls)} URLs sequentially (one by one)")
    
    results = download_batch_to_rustfs(urls, dataset, zone_type, bucket)
    
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
    zone_type: str = 'municipios',
    **context
) -> Dict[str, Any]:
    """
    Processes downloaded files from RustFS into DuckDB using COPY (INSERT INTO) with multi-threading.
    Uses executor (Cloud Run) instead of ingestor - builds complete SQL query and passes it to executor.
    Parses fecha from VARCHAR to TIMESTAMP and uses read_csv with multiple URLs for faster processing.
    After processing, cleans up files from RustFS.
    """
    from bronze.utils import copy_from_csv_batch, delete_batch_from_rustfs
    from airflow.sdk import Variable
    
    bucket = Variable.get('RAW_BUCKET', default='mitma-raw')
    
    dataset = 'od'
    logger = get_logger(__name__, context)
    table_name = f'bronze_mitma_{dataset}_{zone_type}'
    batch_index = download_result.get('batch_index', 0)
    
    logger.info(f"Processing batch {batch_index} using executor")
    
    result = copy_from_csv_batch(
        table_name=table_name,
        batch=download_result,
        fecha_as_timestamp=True,
        **context
    )
    
    downloaded = download_result.get('downloaded', [])
    if downloaded:
        s3_paths = [item['s3_path'] for item in downloaded]
        logger.info(f"Cleaning up batch {batch_index}: {len(s3_paths)} files from RustFS")
        delete_results = delete_batch_from_rustfs(s3_paths, bucket)
        deleted = sum(1 for v in delete_results.values() if v)
        logger.info(f"Deleted {deleted}/{len(s3_paths)} files from RustFS")
    else:
        deleted = 0
    
    return {
        **download_result,
        'process_status': result.get('status', 'unknown'),
        'processed': result.get('processed', 0),
        'process_failed': result.get('failed', 0),
        'process_errors': result.get('errors', []),
        'deleted': deleted
    }