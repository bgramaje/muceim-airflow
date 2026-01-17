"""MITMA-specific utility functions for Bronze layer data ingestion."""

from utils.utils import get_ducklake_connection
import re
import urllib.request
import urllib.error
import urllib.parse
import requests
import pandas as pd
import geopandas as gpd
import os
import io
import tempfile
import time
from datetime import datetime
from shapely.validation import make_valid
from functools import wraps
from typing import List, Dict, Callable, Optional, Any
from concurrent.futures import ThreadPoolExecutor, as_completed
import sys

# Ensure we can import from parent directory
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

LAKE_LAYER = 'bronze'
RUSTFS_RAW_BUCKET = 'mitma-raw'  # S3 bucket names cannot contain underscores, use hyphen instead
 
MAX_RETRIES = 5
INITIAL_BACKOFF = 2
MAX_BACKOFF = 120
CHUNK_SIZE = 8 * 1024 * 1024
DOWNLOAD_TIMEOUT = 600
def retry_with_backoff(
    max_retries: int = MAX_RETRIES,
    initial_backoff: float = INITIAL_BACKOFF,
    max_backoff: float = MAX_BACKOFF,
    retryable_exceptions: tuple = (
        urllib.error.URLError,
        TimeoutError,
        ConnectionError,
        ConnectionResetError,
        BrokenPipeError,
        OSError,
    )
):
    """Decorator for retry with exponential backoff on transient errors."""
    def decorator(func: Callable):
        @wraps(func)
        def wrapper(*args, **kwargs):
            last_exception = None
            for attempt in range(max_retries):
                try:
                    return func(*args, **kwargs)
                except retryable_exceptions as e:
                    last_exception = e
                    if attempt < max_retries - 1:
                        backoff = min(initial_backoff * (2 ** attempt), max_backoff)
                        import random
                        jitter = random.uniform(0, backoff * 0.1)
                        wait_time = backoff + jitter
                        print(f"[RETRY] Attempt {attempt + 1}/{max_retries} failed: {e}. "
                              f"Retrying in {wait_time:.1f}s...")
                        time.sleep(wait_time)
                    else:
                        print(f"[RETRY] All {max_retries} attempts failed for {func.__name__}")
            raise last_exception
        return wrapper
    return decorator


@retry_with_backoff()
def download_url_to_file(url: str, file_path: str, timeout: int = DOWNLOAD_TIMEOUT) -> int:
    """Downloads a file using streaming to disk with automatic retry."""
    req = urllib.request.Request(url, headers={"User-Agent": "MITMA-DuckLake-Loader"})
    
    total_size = 0
    last_progress_log = 0
    
    with urllib.request.urlopen(req, timeout=timeout) as response:
        content_length = response.headers.get('Content-Length')
        expected_size = int(content_length) if content_length else None
        
        with open(file_path, 'wb') as f:
            while True:
                chunk = response.read(CHUNK_SIZE)
                if not chunk:
                    break
                f.write(chunk)
                total_size += len(chunk)
                
                if expected_size and total_size - last_progress_log >= 10 * 1024 * 1024:
                    progress = (total_size / expected_size) * 100
                    print(f"[DOWNLOAD] Progress: {progress:.1f}% ({total_size:,} / {expected_size:,} bytes)")
                    last_progress_log = total_size
    
    print(f"[DOWNLOAD] Complete: {total_size:,} bytes")
    return total_size


def download_url_to_rustfs_v2(url: str, dataset: str, zone_type: str) -> str:
    """Downloads a file to RustFS S3 using streaming (memory efficient)."""
    from airflow.providers.amazon.aws.hooks.s3 import S3Hook
    
    parsed_url = urllib.parse.urlparse(url)
    filename = os.path.basename(parsed_url.path)
    
    if not filename:
        raise ValueError(f"Could not extract filename from URL: {url}")
    
    s3_key = f"{dataset}/{zone_type}/{filename}"
    s3_path = f"s3://{RUSTFS_RAW_BUCKET}/{s3_key}"
    
    print(f"[DOWNLOAD_V2] Target S3 path: {s3_path}")
    
    s3_hook = S3Hook(aws_conn_id='rustfs_s3_conn')
    
    # Ensure bucket exists
    try:
        if not s3_hook.check_for_bucket(RUSTFS_RAW_BUCKET):
            s3_hook.create_bucket(bucket_name=RUSTFS_RAW_BUCKET)
            print(f"[DOWNLOAD_V2] Created bucket '{RUSTFS_RAW_BUCKET}'")
    except Exception as e:
        print(f"[DOWNLOAD_V2] Warning checking bucket: {e}")
    
    if s3_hook.check_for_key(s3_key, bucket_name=RUSTFS_RAW_BUCKET):
        print(f"[DOWNLOAD_V2] File already exists: {s3_path}")
        return s3_path
    
    temp_file = None
    try:
        with tempfile.NamedTemporaryFile(delete=False, suffix='.tmp') as tmp:
            temp_file = tmp.name
        
        print(f"[DOWNLOAD_V2] Starting streaming download to disk: {url}")
        total_bytes = download_url_to_file(url, temp_file)
        
        print(f"[DOWNLOAD_V2] Uploading {total_bytes:,} bytes to {s3_path}")
        s3_hook.load_file(
            filename=temp_file,
            key=s3_key,
            bucket_name=RUSTFS_RAW_BUCKET,
            replace=True
        )
        print(f"[DOWNLOAD_V2] Successfully uploaded to {s3_path}")
    finally:
        if temp_file and os.path.exists(temp_file):
            try:
                os.unlink(temp_file)
            except Exception as e:
                print(f"[DOWNLOAD_V2] Warning: Could not delete temp file {temp_file}: {e}")
    
    return s3_path


def download_batch_to_rustfs(
    urls: List[str],
    dataset: str,
    zone_type: str,
    max_parallel: int = 4
) -> Dict[str, str]:
    """Downloads multiple URLs in parallel to RustFS S3."""
    results = {}
    failed = []
    
    def download_single(url: str) -> tuple:
        try:
            s3_path = download_url_to_rustfs_v2(url, dataset, zone_type)
            return (url, s3_path, None)
        except Exception as e:
            return (url, None, str(e))
    
    print(f"[BATCH_DOWNLOAD] Starting parallel download of {len(urls)} URLs (max_parallel={max_parallel})")
    
    per_download_timeout = DOWNLOAD_TIMEOUT + 60
    
    with ThreadPoolExecutor(max_workers=max_parallel) as executor:
        futures = {executor.submit(download_single, url): url for url in urls}
        
        for i, future in enumerate(as_completed(futures), 1):
            try:
                url, s3_path, error = future.result(timeout=per_download_timeout)
                if error:
                    print(f"[BATCH_DOWNLOAD] [{i}/{len(urls)}] Failed: {os.path.basename(url)} - {error}")
                    failed.append((url, error))
                else:
                    results[url] = s3_path
                    print(f"[BATCH_DOWNLOAD] [{i}/{len(urls)}] Success: {os.path.basename(url)}")
            except TimeoutError:
                url = futures[future]
                error_msg = f"Download timeout after {per_download_timeout}s"
                print(f"[BATCH_DOWNLOAD] [{i}/{len(urls)}] Timeout: {os.path.basename(url)} - {error_msg}")
                failed.append((url, error_msg))
                future.cancel()
            except Exception as e:
                url = futures.get(future, "unknown")
                error_msg = f"Unexpected error: {str(e)}"
                print(f"[BATCH_DOWNLOAD] [{i}/{len(urls)}] Error: {os.path.basename(url) if url != 'unknown' else url} - {error_msg}")
                failed.append((url, error_msg))
    
    print(f"[BATCH_DOWNLOAD] Completed: {len(results)} success, {len(failed)} failed")
    
    if failed:
        print(f"[BATCH_DOWNLOAD] Failed URLs:")
        for url, error in failed[:5]:
            print(f"  - {os.path.basename(url)}: {error}")
        if len(failed) > 5:
            print(f"  ... and {len(failed) - 5} more")
    
    return results


def delete_batch_from_rustfs(s3_paths: List[str]) -> Dict[str, bool]:
    """Deletes multiple files from RustFS S3 in batch."""
    from airflow.providers.amazon.aws.hooks.s3 import S3Hook
    
    results = {}
    s3_hook = S3Hook(aws_conn_id='rustfs_s3_conn')
    
    print(f"[BATCH_DELETE] Deleting {len(s3_paths)} files from RustFS")
    
    for s3_path in s3_paths:
        try:
            if not s3_path.startswith("s3://"):
                results[s3_path] = False
                continue
            
            path_without_prefix = s3_path[5:]
            parts = path_without_prefix.split("/", 1)
            
            if len(parts) != 2 or parts[0] != RUSTFS_RAW_BUCKET:
                results[s3_path] = False
                continue
            
            s3_key = parts[1]
            
            if s3_hook.check_for_key(s3_key, bucket_name=RUSTFS_RAW_BUCKET):
                s3_client = s3_hook.get_conn()
                s3_client.delete_object(Bucket=RUSTFS_RAW_BUCKET, Key=s3_key)
            
            results[s3_path] = True
        except Exception as e:
            print(f"[BATCH_DELETE] Error deleting {s3_path}: {e}")
            results[s3_path] = False
    
    success_count = sum(1 for v in results.values() if v)
    print(f"[BATCH_DELETE] Completed: {success_count}/{len(s3_paths)} deleted successfully")
    
    return results


def bulk_insert_from_csv(table_name: str, urls: List[str], deduplicate: bool = False):
    """Bulk inserts multiple CSVs at once with optional deduplication."""
    full_table_name = f'{LAKE_LAYER}_{table_name}'
    con = get_ducklake_connection()
    
    if not urls:
        print(f"[BULK_INSERT] No URLs provided, skipping")
        return
    
    url_list_str = "[" + ", ".join([f"'{u}'" for u in urls]) + "]"
    
    insert_sql = f"""
        INSERT INTO {full_table_name}
        SELECT 
            * EXCLUDE (filename),
            CURRENT_TIMESTAMP AS loaded_at,
            filename AS source_file
        FROM read_csv(
            {url_list_str},
            filename = true,
            all_varchar = true
        )
    """
    
    print(f"[BULK_INSERT] Inserting {len(urls)} files into {full_table_name}")
    start_time = time.time()
    
    try:
        con.execute(insert_sql)
        elapsed = time.time() - start_time
        print(f"[BULK_INSERT] Insert completed in {elapsed:.1f}s")
    except Exception as e:
        print(f"[BULK_INSERT] Error during insert: {e}")
        raise
    
    if deduplicate:
        print(f"[BULK_INSERT] Deduplicating {full_table_name}...")
        dedup_start = time.time()
        
        try:
            data_columns = _get_data_columns(full_table_name)
            cols_str = ", ".join(data_columns)
            
            dedup_sql = f"""
                DELETE FROM {full_table_name}
                WHERE rowid IN (
                    SELECT rowid FROM (
                        SELECT rowid,
                               ROW_NUMBER() OVER (PARTITION BY {cols_str} ORDER BY loaded_at DESC) as rn
                        FROM {full_table_name}
                    ) WHERE rn > 1
                )
            """
            con.execute(dedup_sql)
            
            dedup_elapsed = time.time() - dedup_start
            print(f"[BULK_INSERT] Deduplication completed in {dedup_elapsed:.1f}s")
        except Exception as e:
            print(f"[BULK_INSERT] Warning: Deduplication failed (non-critical): {e}")


def create_partitioned_table_from_csv(
    table_name: str,
    url: str,
    partition_by_date: bool = True,
    fecha_as_timestamp: bool = False
):
    """Creates a DuckDB table with optional partitioning by year/month/day."""
    full_table_name = f'{LAKE_LAYER}_{table_name}'
    con = get_ducklake_connection()
    
    print(f"[CREATE_TABLE_V3] Creating table {full_table_name} with schema from {os.path.basename(url)}")
    
    source_sql = _get_csv_source_query([url])
    
    if fecha_as_timestamp:
        create_table_sql = f"""
            CREATE TABLE IF NOT EXISTS {full_table_name} AS
            SELECT 
                strptime(CAST(fecha AS VARCHAR), '%Y%m%d')::TIMESTAMP AS fecha,
                * EXCLUDE (fecha)
            FROM ({source_sql})
            LIMIT 0;
        """
    else:
        create_table_sql = f"""
            CREATE TABLE IF NOT EXISTS {full_table_name} AS
            {source_sql}
            LIMIT 0;
        """
    
    con.execute(create_table_sql)
    
    if partition_by_date:
        try:
            if fecha_as_timestamp:
                ensure_timestamp_sql = f"""
                    ALTER TABLE {full_table_name} 
                    ALTER COLUMN fecha TYPE TIMESTAMP;
                """
                try:
                    con.execute(ensure_timestamp_sql)
                    print(f"[CREATE_TABLE_V3] Ensured fecha column is TIMESTAMP")
                except Exception as e:
                    print(f"[CREATE_TABLE_V3] Note: Could not alter fecha type (may already be TIMESTAMP): {e}")
                
                partition_sql = f"""
                    ALTER TABLE {full_table_name} 
                    SET PARTITIONED BY (year(fecha), month(fecha), day(fecha));
                """
            else:
                partition_sql = f"""
                    ALTER TABLE {full_table_name} 
                    SET PARTITIONED BY (
                        substr(fecha, 1, 4)::INTEGER,
                        substr(fecha, 5, 2)::INTEGER,
                        substr(fecha, 7, 2)::INTEGER
                    );
                """
            con.execute(partition_sql)
            print(f"[CREATE_TABLE_V3] Applied partitioning")
        except Exception as e:
            print(f"[CREATE_TABLE_V3] Warning: Could not apply partitioning (table may already be partitioned): {e}")
    
    print(f"[CREATE_TABLE_V3] Table {full_table_name} is ready")
    return {'status': 'created', 'table_name': full_table_name, 'partitioned': partition_by_date, 'fecha_as_timestamp': fecha_as_timestamp}


def finalize_table(table_name: str, run_analyze: bool = True) -> Dict[str, Any]:
    """Finalizes a table after bulk operations and returns stats."""
    con = get_ducklake_connection()
    
    print(f"[FINALIZE] Finalizing table {table_name}")
    
    try:
        count = con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
        print(f"[FINALIZE] Table {table_name} has {count:,} records")
        return {'status': 'success', 'table_name': table_name, 'record_count': count}
    except Exception as e:
        print(f"[FINALIZE] Warning: Could not get record count: {e}")
        return {'status': 'success', 'table_name': table_name}


def create_url_batches(urls: List[str], batch_size: int = 10) -> List[List[str]]:
    """Divides a list of URLs into batches for parallel processing."""
    if not urls:
        return []
    
    batches = [urls[i:i + batch_size] for i in range(0, len(urls), batch_size)]
    print(f"[BATCH] Created {len(batches)} batches of max {batch_size} URLs each")
    
    return batches


def copy_batch_to_table(
    table_name: str,
    s3_paths: List[str],
    original_urls: List[str] = None,
    threads: int = 4,
    fecha_as_timestamp: bool = False,
    **context
) -> Dict[str, Any]:
    """Copies a batch of CSV files from S3/RustFS to DuckDB using INSERT INTO."""
    from utils.gcp import execute_sql_or_cloud_run
    
    full_table_name = table_name if table_name.startswith('bronze_') else f'bronze_{table_name}'
    
    if not s3_paths:
        return {'success': 0, 'failed': 0, 'errors': []}
    
    if original_urls is None:
        original_urls = s3_paths
    
    print(f"[COPY_BATCH] Processing {len(s3_paths)} files from RustFS into {full_table_name} with {threads} threads using executor")
    if fecha_as_timestamp:
        print(f"[COPY_BATCH] Will parse fecha to TIMESTAMP")
    
    start_time = time.time()
    
    union_parts = []
    for i, s3_path in enumerate(s3_paths):
        source_file_value = original_urls[i] if i < len(original_urls) else s3_path
        source_file_value_escaped = source_file_value.replace("'", "''")
        s3_path_escaped = s3_path.replace("'", "''")
        
        if fecha_as_timestamp:
            union_parts.append(f"""
                SELECT 
                    strptime(CAST(fecha AS VARCHAR), '%Y%m%d')::TIMESTAMP AS fecha,
                    * EXCLUDE (fecha, filename),
                    CURRENT_TIMESTAMP AS loaded_at,
                    '{source_file_value_escaped}' AS source_file
                FROM read_csv(
                    '{s3_path_escaped}',
                    filename = true,
                    header = true,
                    all_varchar = true
                )
            """)
        else:
            union_parts.append(f"""
                SELECT 
                    * EXCLUDE (filename),
                    CURRENT_TIMESTAMP AS loaded_at,
                    '{source_file_value_escaped}' AS source_file
                FROM read_csv(
                    '{s3_path_escaped}',
                    filename = true,
                    header = true,
                    all_varchar = true
                )
            """)
    
    sql_query = f"""
        SET threads={threads};
        SET worker_threads={threads};
        SET preserve_insertion_order=false;
        SET enable_object_cache=true;
        INSERT INTO {full_table_name}
        {' UNION ALL '.join(union_parts)};
    """
    
    try:
        result = execute_sql_or_cloud_run(sql_query=sql_query, **context)
        success_count = len(s3_paths)
        print(f"[COPY_BATCH] Successfully processed {len(s3_paths)} files using executor")
    except Exception as e:
        error_msg = f"Error copying batch: {str(e)}"
        print(f"[COPY_BATCH] Batch insert failed: {error_msg}")
        return {
            'success': 0,
            'failed': len(s3_paths),
            'errors': [error_msg],
            'elapsed_seconds': time.time() - start_time
        }
    
    elapsed = time.time() - start_time
    print(f"[COPY_BATCH] Completed in {elapsed:.1f}s: {success_count} success, 0 failed")
    
    return {
        'success': success_count,
        'failed': 0,
        'errors': [],
        'elapsed_seconds': elapsed
    }


def copy_from_csv_batch(
    table_name: str,
    batch: Dict[str, Any],
    threads: int = 4,
    fecha_as_timestamp: bool = False,
    **context
) -> Dict[str, Any]:
    """Processes a batch of downloaded files using INSERT INTO with multi-threading."""
    full_table_name = f'bronze_{table_name}'
    downloaded = batch.get('downloaded', [])
    batch_index = batch.get('batch_index', 0)
    
    if not downloaded:
        return {
            'batch_index': batch_index,
            'status': 'skipped',
            'processed': 0,
            'failed': 0
        }
    
    s3_paths = [item['s3_path'] for item in downloaded]
    original_urls = [item.get('original_url', item['s3_path']) for item in downloaded]
    
    print(f"[COPY_BATCH_TASK] Processing batch {batch_index}: {len(s3_paths)} files using executor")
    
    result = copy_batch_to_table(
        table_name=full_table_name,
        s3_paths=s3_paths,
        original_urls=original_urls,
        threads=threads,
        fecha_as_timestamp=fecha_as_timestamp,
        **context
    )
    
    return {
        'batch_index': batch_index,
        'status': 'success' if result['failed'] == 0 else 'partial',
        'processed': result['success'],
        'failed': result['failed'],
        'errors': result['errors']
    }


def get_mitma_urls(dataset, zone_type, start_date, end_date):
    """Fetches MITMA URLs from RSS feed filtered by dataset, zone type, and date range."""
    rss_url = "https://movilidad-opendata.mitma.es/RSS.xml"

    dataset_map = {
        "od": ("viajes", "Viajes"),
        "people_day": ("personas", "Personas_dia"),
        "overnight_stay": ("pernoctaciones", "Pernoctaciones")
    }

    if zone_type not in ["distritos", "municipios", "gau"]:
        raise ValueError(
            f"Invalid zone_type: {zone_type}. Must be 'distritos', 'municipios', or 'gau'.")
    if dataset not in dataset_map:
        raise ValueError(
            f"Invalid dataset: {dataset}. Must be one of {list(dataset_map.keys())}.")

    dataset_path, file_prefix = dataset_map[dataset]

    zone_suffix = "GAU" if zone_type == "gau" else zone_type
    file_pattern = f"{file_prefix}_{zone_suffix}"

    pattern = rf'(https?://[^\s"<>]*/estudios_basicos/por-{zone_type}/{dataset_path}/ficheros-diarios/\d{{4}}-\d{{2}}/(\d{{8}})_{file_pattern}\.csv\.gz)'

    req = urllib.request.Request(
        rss_url, headers={"User-Agent": "MITMA-DuckLake-Loader"})
    txt = urllib.request.urlopen(req).read().decode("utf-8", "ignore")

    matches = re.findall(pattern, txt, re.I)

    unique_matches = list(set(matches))

    start_dt = datetime.strptime(start_date, "%Y-%m-%d")
    end_dt = datetime.strptime(end_date, "%Y-%m-%d")

    filtered_urls = []
    for url, date_str in unique_matches:
        file_date = datetime.strptime(date_str, "%Y%m%d")
        if start_dt <= file_date <= end_dt:
            filtered_urls.append((url, date_str))

    filtered_urls.sort(key=lambda x: x[1])

    urls = [url for url, _ in filtered_urls]

    print(
        f"Found {len(urls)} URLs for {dataset} {zone_type} from {start_date} to {end_date}")

    if not urls:
        print(f"WARNING: No URLs found. Check if data exists for the requested date range.")

    return urls


def download_url_to_rustfs(url: str, dataset: str, zone_type: str) -> str:
    """Downloads a file from URL and uploads to RustFS S3 (legacy version, use download_url_to_rustfs_v2)."""
    from airflow.providers.amazon.aws.hooks.s3 import S3Hook
        
    parsed_url = urllib.parse.urlparse(url)
    filename = os.path.basename(parsed_url.path)
    
    if not filename:
        raise ValueError(f"Could not extract filename from URL: {url}")
    
    s3_key = f"{dataset}/{zone_type}/{filename}"
    s3_path = f"s3://{RUSTFS_RAW_BUCKET}/{s3_key}"
    
    print(f"[DOWNLOAD] Target S3 path: {s3_path}")
    
    s3_hook = S3Hook(aws_conn_id='rustfs_s3_conn')
    
    try:
        bucket_exists = s3_hook.check_for_bucket(RUSTFS_RAW_BUCKET)
        if not bucket_exists:
            s3_hook.create_bucket(bucket_name=RUSTFS_RAW_BUCKET)
        else:
            print(f"[DOWNLOAD] Bucket '{RUSTFS_RAW_BUCKET}' exists")
    except Exception as bucket_error:
        print(f"[DOWNLOAD] Error checking bucket: {bucket_error}")
        raise bucket_error
    
    file_exists = s3_hook.check_for_key(s3_key, bucket_name=RUSTFS_RAW_BUCKET)
    if file_exists:
        print(f"[DOWNLOAD] File already exists in RustFS: {s3_path}")
        return s3_path
    
    print(f"[DOWNLOAD] Downloading from URL...")
    req = urllib.request.Request(url, headers={"User-Agent": "MITMA-DuckLake-Loader"})
    
    file_content = None
    try:
        with urllib.request.urlopen(req, timeout=300) as response:
            file_content = response.read()
            print(f"[DOWNLOAD] Downloaded {len(file_content)} bytes")
    except Exception as e:
        raise RuntimeError(f"Failed to download file from {url}: {e}")
    
    print(f"[DOWNLOAD] Uploading to RustFS bucket '{RUSTFS_RAW_BUCKET}'...")
    try:
        s3_hook.load_bytes(
            bytes_data=file_content,
            key=s3_key,
            bucket_name=RUSTFS_RAW_BUCKET,
            replace=True
        )
        print(f"[DOWNLOAD] Successfully uploaded to {s3_path}")
    except Exception as e:
        raise RuntimeError(f"Failed to upload file to RustFS: {e}")
    finally:
        if file_content is not None:
            del file_content
    
    return s3_path


def delete_file_from_rustfs(s3_path: str) -> bool:
    """Deletes a single file from RustFS S3 bucket."""
    from airflow.providers.amazon.aws.hooks.s3 import S3Hook
    print(f"[DELETE] Attempting to delete file from RustFS: {s3_path}")
    
    if not s3_path.startswith("s3://"):
        print(f"[DELETE] Invalid S3 path format: {s3_path}")
        return False
    
    path_without_prefix = s3_path[5:]
    parts = path_without_prefix.split("/", 1)
    
    if len(parts) != 2:
        print(f"[DELETE] Could not parse S3 path: {s3_path}")
        return False
    
    bucket_name = parts[0]
    s3_key = parts[1]
    
    if bucket_name != RUSTFS_RAW_BUCKET:
        print(f"[DELETE] Only files from {RUSTFS_RAW_BUCKET} bucket can be deleted. Got: {bucket_name}")
        return False
    
    print(f"[DELETE] Bucket: {bucket_name}, Key: {s3_key}")
    
    s3_hook = S3Hook(aws_conn_id='rustfs_s3_conn')
    
    try:
        if s3_hook.check_for_key(s3_key, bucket_name=bucket_name):
            s3_client = s3_hook.get_conn()
            s3_client.delete_object(Bucket=bucket_name, Key=s3_key)
            print(f"[DELETE] Successfully deleted file: {s3_path}")
            return True
        else:
            print(f"[DELETE] File does not exist in RustFS: {s3_path} (may have been already deleted)")
            return True
    except Exception as e:
        print(f"[DELETE] Error deleting file from RustFS: {e}")
        return False


def get_mitma_zoning_urls(zone_type):
    """Fetches MITMA zoning URLs (Shapefiles + CSVs) from RSS feed."""
    rss_url = "https://movilidad-opendata.mitma.es/RSS.xml"

    if zone_type not in ["distritos", "municipios", "gau"]:
        raise ValueError(
            f"Invalid zone_type: {zone_type}. Must be 'distritos', 'municipios', or 'gau'.")

    folder_suffix = "GAU" if zone_type == "gau" else zone_type
    file_suffix = "gaus" if zone_type == "gau" else zone_type

    shp_pattern = rf'(https?://[^\s"<>]*/zonificacion/zonificacion_{folder_suffix}/[^"<>]+\.(?:shp|shx|dbf|prj))'
    csv_pattern = rf'(https?://[^\s"<>]*/zonificacion/zonificacion_{folder_suffix}/(?:nombres|poblacion)_{file_suffix}\.csv)'

    print(f"📡 Scanning RSS for {zone_type} zoning files...")

    try:
        req = urllib.request.Request(
            rss_url, headers={"User-Agent": "MITMA-DuckLake-Loader"})
        with urllib.request.urlopen(req) as response:
            txt = response.read().decode("utf-8", "ignore")

        shp_matches = re.findall(shp_pattern, txt, re.IGNORECASE)
        csv_matches = re.findall(csv_pattern, txt, re.IGNORECASE)

        unique_shp = sorted(list(set(shp_matches)))
        unique_csv = sorted(list(set(csv_matches)))

        url_nombres = next(
            (u for u in unique_csv if 'nombres' in u.lower()), None)
        url_poblacion = next(
            (u for u in unique_csv if 'poblacion' in u.lower()), None)

        if not unique_shp and not unique_csv:
            print(
                "WARNING: No zoning URLs found in RSS. The feed might have rotated them out.")
            return {}

        print(
            f"Found {len(unique_shp)} shapefile components and {len(unique_csv)} CSVs.")

        return {
            "shp_components": unique_shp,
            "nombres": url_nombres,
            "poblacion": url_poblacion
        }

    except Exception as e:
        print(f"ERROR fetching RSS: {e}")
        return {}


def clean_id(series):
    """Normalizes ID to clean string (removes .0 suffix and whitespace)."""
    return series.astype(str).str.strip().str.replace(r'\.0$', '', regex=True)


def clean_poblacion(series):
    """Cleans population integers (removes dots and decimals)."""
    return (series.astype(str)
            .str.replace('.', '', regex=False)
            .str.replace(r'\.0$', '', regex=True)
            .apply(pd.to_numeric, errors='coerce')
            .fillna(0).astype(int))


def get_mitma_zoning_dataset(zone_type='municipios'):
    """Downloads, cleans and merges MITMA zoning data into a GeoDataFrame."""
    urls = get_mitma_zoning_urls(zone_type)

    print(f"Generando dataset maestro para: {zone_type.upper()}")

    with tempfile.TemporaryDirectory() as tmp_dir:
        print("   Descargando geometrías...")
        shp_local_path = None

        for url in urls['shp_components']:
            filename = url.split('/')[-1]
            try:
                r = requests.get(url, timeout=15)
                if r.status_code == 200:
                    local_p = os.path.join(tmp_dir, filename)
                    with open(local_p, 'wb') as f:
                        f.write(r.content)
                    if filename.endswith('.shp'):
                        shp_local_path = local_p
            except Exception as e:
                print(f"      Error bajando {filename}: {e}")

        if not shp_local_path:
            print("Error: No se pudo descargar el archivo .shp principal.")
            return None

        gdf = gpd.read_file(shp_local_path)

        id_col = next((c for c in gdf.columns if c.upper() in [
                      'ID', 'CODIGO', 'ZONA', 'COD_GAU']), 'ID')
        gdf['ID'] = clean_id(gdf[id_col])

        gdf['geometry'] = gdf['geometry'].apply(make_valid)
        if gdf.crs and gdf.crs.to_string() != "EPSG:4326":
            gdf = gdf.to_crs("EPSG:4326")

        print("   🔗 Integrating metadata (Names and Population)...")
        df_aux = pd.DataFrame(columns=['ID'])

        aux_config = [
            {
                'type': 'nombres',
                'url': urls['nombres'],
                'header': 0,
                'cols': ['ID', 'Nombre']
            },
            {
                'type': 'poblacion',
                'url': urls['poblacion'],
                'header': None,
                'cols': ['ID', 'Poblacion']
            }
        ]

        for cfg in aux_config:
            try:
                r = requests.get(cfg['url'], timeout=10)
                if r.status_code == 200:
                    df_t = pd.read_csv(
                        io.BytesIO(r.content),
                        sep='|',
                        header=cfg['header'],
                        dtype=str,
                        engine='python'
                    )

                    if len(df_t.columns) >= 3:
                        df_t = df_t.iloc[:, [1, 2]]
                    elif len(df_t.columns) == 2:
                        df_t = df_t.iloc[:, [0, 1]]

                    df_t.columns = cfg['cols']

                    df_t['ID'] = clean_id(df_t['ID'])
                    df_t = df_t.drop_duplicates(subset=['ID'])

                    if cfg['type'] == 'poblacion':
                        df_t['Poblacion'] = clean_poblacion(df_t['Poblacion'])

                    if df_aux.empty:
                        df_aux = df_t
                    else:
                        df_aux = df_aux.merge(df_t, on='ID', how='outer')

                    print(f"      {cfg['type'].capitalize()} OK")
            except Exception as e:
                print(f"      Failed processing {cfg['type']}: {e}")

        if not df_aux.empty:
            gdf = gdf.merge(df_aux, on='ID', how='left')

            if 'Nombre' in gdf.columns:
                gdf['Nombre'] = gdf['Nombre'].fillna(gdf['ID'])
            if 'Poblacion' in gdf.columns:
                gdf['Poblacion'] = gdf['Poblacion'].fillna(0).astype(int)

        cols = ['ID', 'Nombre', 'Poblacion', 'geometry']
        final_cols = [c for c in cols if c in gdf.columns] + \
            [c for c in gdf.columns if c not in cols]
        gdf = gdf[final_cols]

        print(f"Dataset generado: {len(gdf)} registros.")
        return gdf


def load_zonificacion(con, zone_type, lake_layer='bronze'):
    """Loads zonification data into DuckDB for the specified zone type."""
    df = get_mitma_zoning_dataset(zone_type)

    if df is None or df.empty:
        print(f"No data to load for {zone_type}")
        return

    for col in df.columns:
        df[col] = df[col].astype(str)

    table_name = f'{lake_layer}_mitma_{zone_type}'

    con.register('temp_zonificacion', df)

    con.execute(f"""
        CREATE TABLE IF NOT EXISTS {table_name} AS
        SELECT
            *,
            CURRENT_TIMESTAMP AS loaded_at,
        FROM temp_zonificacion
        LIMIT 0;
    """)

    merge_key = 'ID'

    con.execute(f"""
        MERGE INTO {table_name} AS target
        USING (
            SELECT
                *,
                CURRENT_TIMESTAMP AS loaded_at,
            FROM temp_zonificacion
        ) AS source
        ON target.{merge_key} = source.{merge_key}
        WHEN MATCHED THEN
            UPDATE SET *
        WHEN NOT MATCHED THEN
            INSERT *;
    """)

    con.unregister('temp_zonificacion')

    print(f"Table {table_name} merged successfully with {len(df)} records.")


def _get_data_columns(table_name):
    """Gets business columns excluding audit columns (loaded_at, source_file, source_url)."""
    audit_cols = "('loaded_at', 'source_file', 'source_url')"

    con = get_ducklake_connection()
    df = con.execute(f"""
        SELECT column_name 
        FROM information_schema.columns 
        WHERE table_name = '{table_name}'
        AND column_name NOT IN {audit_cols}
        ORDER BY ordinal_position;
    """).fetchdf()
    return df['column_name'].tolist()


def _build_merge_condition(columns):
    """Builds a robust ON clause that handles NULLs correctly."""
    return " AND ".join([
        f"target.{col} IS NOT DISTINCT FROM source.{col}"
        for col in columns
    ])


def _get_csv_source_query(urls):
    """Generates SELECT subquery for reading CSVs with standard configuration."""
    url_list_str = "[" + ", ".join([f"'{u}'" for u in urls]) + "]"

    return f"""
        SELECT 
            * EXCLUDE (filename),
            CURRENT_TIMESTAMP AS loaded_at,
            filename AS source_file
        FROM read_csv(
            {url_list_str},
            filename = true,
            all_varchar = true
        )
    """


def create_table_from_csv(table_name, url):
    """Creates a DuckDB table from a single CSV URL if it doesn't exist."""
    full_table_name = f'{LAKE_LAYER}_{table_name}'
    con = get_ducklake_connection()

    source_sql = _get_csv_source_query([url])

    print(f"Verifying schema for {full_table_name} using first file...")
    con.execute(f"""
        CREATE TABLE IF NOT EXISTS {full_table_name} AS
        {source_sql}
        LIMIT 0;
    """)
    print(f"Table {full_table_name} is ready.")


def merge_from_csv(table_name, url):
    """Merges data from a single CSV URL into an existing DuckDB table."""
    full_table_name = f'{LAKE_LAYER}_{table_name}'
    con = get_ducklake_connection()

    merge_keys = _get_data_columns(full_table_name)
    on_clause = _build_merge_condition(merge_keys)

    source_sql = _get_csv_source_query([url])

    print(f"Merging data from {url} into {full_table_name}...")
    try:
        con.execute(f"""
            MERGE INTO {full_table_name} AS target
            USING ({source_sql}) AS source
            ON {on_clause}
            WHEN NOT MATCHED THEN
                INSERT *;
        """)
        print(f"  Merged successfully.")
    except Exception as e:
        error_str = str(e)

        is_transaction_error = (
            "TransactionContext" in error_str or
            "Failed to commit" in error_str or
            "Failed to execute query" in error_str
        )

        if is_transaction_error:
            print(
                f"  Transaction error detected - forcing new connection for next task")
            try:
                get_ducklake_connection(force_new=True)
            except:
                pass

        error_msg = f"  Error processing {url}: {e}"
        print(error_msg)
        # Re-raise exception to ensure Airflow task fails
        raise RuntimeError(error_msg) from e


def create_table_from_json(table_name, url, year: int = None):
    """Creates a DuckDB table from a single JSON URL if it doesn't exist."""
    full_table_name = f'{LAKE_LAYER}_{table_name}'
    con = get_ducklake_connection()

    source_sql = _get_json_source_query(url, year=year)

    print(f"Verifying schema for {full_table_name} using first file...")
    
    table_exists = False
    try:
        result = con.execute(f"""
            SELECT COUNT(*) as cnt FROM information_schema.tables 
            WHERE table_schema = 'main' AND table_name = '{full_table_name}'
        """).fetchone()
        table_exists = result[0] > 0
    except:
        pass
    
    con.execute(f"""
        CREATE TABLE IF NOT EXISTS {full_table_name} AS
        {source_sql}
        LIMIT 0;
    """)
    
    if year is not None and not table_exists:
        try:
            con.execute(f"ALTER TABLE {full_table_name} SET PARTITIONED BY (year);")
            print(f"  Added partitioning by year for {full_table_name}")
        except Exception as partition_error:
            print(f"  Could not add partitioning (non-critical): {partition_error}")

    print(f"Table {full_table_name} is ready.")


def merge_from_json(table_name, url, key_columns=None, year: int = None):
    """Merges data from a single JSON URL into an existing DuckDB table."""
    full_table_name = f'{LAKE_LAYER}_{table_name}'
    con = get_ducklake_connection()

    if key_columns is None:
        merge_keys = _get_data_columns(full_table_name)
    else:
        existing_cols = _get_data_columns(full_table_name)
        missing = [k for k in key_columns if k not in existing_cols and k != 'year']
        if missing:
            raise ValueError(
                f"Key columns {missing} not found in table metadata.")
        merge_keys = key_columns

    on_clause = _build_merge_condition(merge_keys)
    source_sql = _get_json_source_query(url, year=year)

    print(f"Merging data from {url} into {full_table_name}...")
    if year is not None:
        print(f"  Adding year column: {year}")
    try:
        con.execute(f"""
            MERGE INTO {full_table_name} AS target
            USING ({source_sql}) AS source
            ON {on_clause}
            WHEN NOT MATCHED THEN
                INSERT *;
        """)
        print(f"  Merged successfully.")
    except Exception as e:
        error_str = str(e)
        is_transaction_error = (
            "TransactionContext" in error_str or
            "Failed to commit" in error_str or
            "Failed to execute query" in error_str
        )

        if is_transaction_error:
            print(
                f"  Transaction error detected - forcing new connection for next task")
            try:
                get_ducklake_connection(force_new=True)
            except:
                pass

        error_msg = f"  Error processing {url}: {e}"
        print(error_msg)
        raise RuntimeError(error_msg) from e


def _get_json_source_query(url, year: int = None):
    """Generates SELECT subquery for reading JSON with optional year column."""
    year_column = f",\n            {year} AS year" if year is not None else ""
    return f"""
        SELECT 
            *,
            CURRENT_TIMESTAMP AS loaded_at,
            '{url}' AS source_url{year_column}
        FROM read_json('{url}', format='array')
    """


def ine_renta_filter_urls(urls: list[str]):
    """Filters INE Renta URLs to exclude already ingested ones."""
    table_name = 'bronze_ine_renta_municipio'
    return _filter_json_urls(table_name, urls)


def ine_municipios_filter_urls(urls: list[str]):
    """Filters INE Municipios URLs to exclude already ingested ones."""
    table_name = 'bronze_ine_municipios'
    return _filter_json_urls(table_name, urls)


def ine_empresas_filter_urls(urls: list[str]):
    """Filters INE Empresas URLs to exclude already ingested ones."""
    table_name = 'bronze_ine_empresas_municipio'
    return _filter_json_urls(table_name, urls)


def ine_poblacion_filter_urls(urls: list[str]):
    """Filters INE Poblacion URLs to exclude already ingested ones."""
    table_name = 'bronze_ine_poblacion_municipio'
    return _filter_json_urls(table_name, urls)


def mitma_create_table(dataset: str, zone_type: str, urls: list[str]):
    """Creates the table for MITMA data if it doesn't exist."""
    table_name = f'mitma_{dataset}_{zone_type}'

    if not urls:
        raise ValueError(f"No URLs provided to create table {table_name}")

    first_url = urls[0]
    print(
        f"[TASK] Creating table {table_name} if not exists, using first URL: {first_url}")

    create_table_from_csv(table_name, first_url)

    return {'status': 'success', 'table_name': table_name}


def mitma_filter_urls(dataset: str, zone_type: str, urls: list[str]):
    """Filters MITMA URLs to exclude already ingested ones."""
    table_name = f'bronze_mitma_{dataset}_{zone_type}'
    return _filter_csv_urls(table_name, urls)


def mitma_ine_relations_filter_urls(urls: list[str]):
    """Filters MITMA-INE Relations URLs to exclude already ingested ones."""
    table_name = 'bronze_mitma_ine_relations'
    return _filter_csv_urls(table_name, urls)


def _filter_json_urls(table_name: str, urls: list[str]):
    """Generic function to filter JSON table URLs using source_url column."""
    print(f"[TASK] Filtering URLs for {table_name}")
    print(f"[TASK] Total URLs to check: {len(urls)}")

    con = get_ducklake_connection()

    # Check if table exists
    try:
        table_exists = con.execute(f"""
            SELECT COUNT(*) as count 
            FROM information_schema.tables 
            WHERE table_name = '{table_name}'
        """).fetchone()[0] > 0

        if not table_exists:
            print(
                f"[TASK] Table {table_name} does not exist. Returning all URLs.")
            return urls

        print(f"[TASK] Table {table_name} exists. Filtering URLs...")
    except Exception as e:
        print(f"[TASK] Warning: Could not check if table exists: {e}")
        print(f"[TASK] Assuming table does not exist. Returning all URLs.")
        return urls

    # Table exists, filter URLs
    try:
        url_list_str = "[" + ", ".join([f"'{u}'" for u in urls]) + "]"

        ingested_df = con.execute(f"""
            WITH url_list AS (
                SELECT unnest({url_list_str}) AS url_to_check
            )
            SELECT DISTINCT source_url 
            FROM {table_name}
            WHERE source_url IS NOT NULL
              AND source_url IN (SELECT url_to_check FROM url_list)
        """).fetchdf()

        ingested_urls = set(
            ingested_df['source_url'].tolist()) if not ingested_df.empty else set()
        print(
            f"[TASK] Found {len(ingested_urls)} already ingested URLs (out of {len(urls)} checked)")
    except Exception as e:
        print(f"[TASK] Warning: Could not check existing URLs: {e}")
        print(f"[TASK] Proceeding as if no URLs are ingested")
        ingested_urls = set()

    new_urls = [url for url in urls if url not in ingested_urls]
    print(
        f"[TASK] Filtered result: {len(new_urls)} new URLs to ingest (skipping {len(urls) - len(new_urls)} already ingested)")

    if len(new_urls) == 0:
        print(
            f"[TASK] All URLs have already been ingested. No new data to process.")
    else:
        print(
            f"[TASK] URLs to ingest: {new_urls[:3]}{'...' if len(new_urls) > 3 else ''}")

    return new_urls


def _filter_csv_urls(table_name: str, urls: list[str]):
    """Generic function to filter CSV table URLs using source_file column."""
    print(f"[TASK] Filtering URLs for {table_name}")
    print(f"[TASK] Total URLs to check: {len(urls)}")

    con = get_ducklake_connection()

    # Check if table exists
    try:
        table_exists = con.execute(f"""
            SELECT COUNT(*) as count 
            FROM information_schema.tables 
            WHERE table_name = '{table_name}'
        """).fetchone()[0] > 0

        if not table_exists:
            print(
                f"[TASK] Table {table_name} does not exist. Returning all URLs.")
            return urls

        print(f"[TASK] Table {table_name} exists. Filtering URLs...")
    except Exception as e:
        print(f"[TASK] Warning: Could not check if table exists: {e}")
        print(f"[TASK] Assuming table does not exist. Returning all URLs.")
        return urls

    # Table exists, filter URLs
    try:
        url_list_str = "[" + ", ".join([f"'{u}'" for u in urls]) + "]"

        ingested_df = con.execute(f"""
            WITH url_list AS (
                SELECT unnest({url_list_str}) AS url_to_check
            )
            SELECT DISTINCT source_file 
            FROM {table_name}
            WHERE source_file IS NOT NULL
              AND source_file IN (SELECT url_to_check FROM url_list)
        """).fetchdf()

        ingested_urls = set(
            ingested_df['source_file'].tolist()) if not ingested_df.empty else set()
        print(
            f"[TASK] Found {len(ingested_urls)} already ingested URLs (out of {len(urls)} checked)")
    except Exception as e:
        print(f"[TASK] Warning: Could not check existing URLs: {e}")
        print(f"[TASK] Proceeding as if no URLs are ingested")
        ingested_urls = set()

    new_urls = [url for url in urls if url not in ingested_urls]
    print(
        f"[TASK] Filtered result: {len(new_urls)} new URLs to ingest (skipping {len(urls) - len(new_urls)} already ingested)")

    if len(new_urls) == 0:
        print(
            f"[TASK] All URLs have already been ingested. No new data to process.")
    else:
        print(
            f"[TASK] URLs to ingest: {new_urls[:3]}{'...' if len(new_urls) > 3 else ''}")

    return new_urls
