"""
DAG V2 for MITMA Bronze layer data ingestion.
Optimized for low-resource systems (MacBook Air M2, 16GB RAM).

Key optimizations:
- Batch processing with small batch sizes (default: 3 URLs)
- Sequential downloads (max_parallel_downloads=1) to reduce memory usage
- Streaming downloads to disk (memory efficient)
- Automatic Cloud Run offloading for heavy processing (when configured)
- One batch processed at a time (max_active_tis_per_dag=1)
- Partitioned tables by year/month/day for faster queries
- ANALYZE only at the end of batch processing

Important notes:
- MITMA URLs cannot be read directly from Cloud Run (error 500), so files
  must be downloaded to RustFS (S3) first, then processed from RustFS
- Processing automatically uses Cloud Run Jobs when available, which
  offloads heavy DuckDB work from the local machine
- Downloads happen locally but use streaming to disk to minimize memory

Usage:
    Trigger with parameters:
    - start: Start date (YYYY-MM-DD)
    - end: End date (YYYY-MM-DD)
    - batch_size: Number of URLs per batch (default: 3, optimized for 16GB RAM)
    - max_parallel_downloads: Max parallel downloads per batch (default: 1, sequential)
    - enable_people_day: Enable People Day data (default: False)
    - enable_overnight: Enable Overnight Stay data (default: False)
"""

from datetime import datetime, timedelta
from typing import List, Dict, Any

from airflow import DAG
from airflow.datasets import Dataset
from airflow.models import Param
from airflow.decorators import task, task_group
from airflow.providers.standard.operators.empty import EmptyOperator

from misc.infra import tg_infra
from utils.gcp import merge_csv_or_cloud_run


# ============================================================================
# TASK FUNCTIONS
# ============================================================================

@task
def get_and_filter_urls(
    dataset: str = None,
    zone_type: str = None,
    start_date: str = None,
    end_date: str = None
) -> List[str]:
    """
    Combines URL fetching and filtering in one task.
    Returns only URLs that haven't been ingested yet.
    """
    from bronze.utils import get_mitma_urls, mitma_filter_urls
    
    if not all([dataset, zone_type, start_date, end_date]):
        raise ValueError("All parameters (dataset, zone_type, start_date, end_date) are required")
    
    print(f"[TASK] Getting URLs for {dataset} {zone_type} from {start_date} to {end_date}")
    
    urls = get_mitma_urls(dataset, zone_type, start_date, end_date)
    print(f"[TASK] Found {len(urls)} total URLs")
    
    if not urls:
        return []
    
    filtered = mitma_filter_urls(dataset, zone_type, urls)
    print(f"[TASK] After filtering: {len(filtered)} new URLs to process")
    
    return filtered


@task
def create_url_batches_task(urls: List[str] = None, batch_size: Any = 10) -> List[Dict[str, Any]]:
    """
    Divides URLs into batches for parallel processing.
    Returns list of dicts with batch_index and urls.
    """
    from bronze.utils import create_url_batches
    
    if urls is None:
        urls = []
    
    # Convert batch_size to int (handles template strings from Airflow params)
    batch_size = int(batch_size) if batch_size else 10
    
    if not urls:
        print("[TASK] No URLs to batch")
        return []
    
    batches = create_url_batches(urls, batch_size)
    
    result = [
        {'batch_index': i, 'urls': batch}
        for i, batch in enumerate(batches)
    ]
    
    print(f"[TASK] Created {len(result)} batches")
    return result


@task
def create_partitioned_table(
    dataset: str = None,
    zone_type: str = None,
    urls: List[str] = None
) -> Dict[str, Any]:
    """
    Creates the Bronze table with partitioning if it doesn't exist.
    """
    from bronze.utils import create_partitioned_table_from_csv
    
    if not all([dataset, zone_type]):
        raise ValueError("dataset and zone_type are required")
    
    if urls is None:
        urls = []
    
    if not urls:
        return {'status': 'skipped', 'reason': 'no_urls'}
    
    table_name = f'mitma_{dataset}_{zone_type}'
    
    print(f"[TASK] Creating partitioned table {table_name}")
    return create_partitioned_table_from_csv(
        table_name=table_name,
        url=urls[0],
        partition_by_date=True
    )


@task(
    retries=5,
    retry_delay=timedelta(seconds=60),
    retry_exponential_backoff=True,
    max_retry_delay=timedelta(minutes=10),
)
def download_batch(
    batch: Dict[str, Any] = None,
    dataset: str = None,
    zone_type: str = None,
    max_parallel: Any = 4
) -> Dict[str, Any]:
    """
    Downloads a batch of URLs to RustFS in parallel.
    Has automatic retry with exponential backoff.
    """
    from bronze.utils import download_batch_to_rustfs
    
    if batch is None:
        batch = {}
    if not all([dataset, zone_type]):
        raise ValueError("dataset and zone_type are required")
    
    # Convert max_parallel to int (handles template strings from Airflow params)
    max_parallel = int(max_parallel) if max_parallel else 4
    
    urls = batch.get('urls', [])
    batch_index = batch.get('batch_index', 0)
    
    if not urls:
        return {
            'batch_index': batch_index,
            'downloaded': [],
            'failed': []
        }
    
    print(f"[TASK] Downloading batch {batch_index}: {len(urls)} URLs")
    
    results = download_batch_to_rustfs(urls, dataset, zone_type, max_parallel)
    
    downloaded = [
        {'original_url': url, 's3_path': s3_path}
        for url, s3_path in results.items()
    ]
    
    failed = [url for url in urls if url not in results]
    
    print(f"[TASK] Batch {batch_index}: {len(downloaded)} downloaded, {len(failed)} failed")
    
    return {
        'batch_index': batch_index,
        'downloaded': downloaded,
        'failed': failed
    }


@task
def process_batch(
    download_result: Dict[str, Any] = None,
    dataset: str = None,
    zone_type: str = None
) -> Dict[str, Any]:
    """
    Processes downloaded files from RustFS into DuckDB.
    
    This function automatically uses Cloud Run Jobs for processing when available,
    which offloads heavy work from the local machine. Falls back to local execution
    only if Cloud Run is not configured.
    
    Note: MITMA URLs cannot be read directly from Cloud Run (error 500),
    so files must be downloaded to RustFS first, then processed from RustFS.
    """
    if download_result is None:
        download_result = {}
    if not all([dataset, zone_type]):
        raise ValueError("dataset and zone_type are required")
    
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
    
    print(f"[TASK] Processing batch {batch_index}: {len(downloaded)} files")
    print(f"[TASK] Note: Processing uses Cloud Run when available to offload work from local machine")
    
    for item in downloaded:
        s3_path = item['s3_path']
        original_url = item['original_url']
        
        try:
            # merge_csv_or_cloud_run automatically uses Cloud Run if available
            # This offloads heavy DuckDB processing from the local MacBook
            merge_csv_or_cloud_run(
                table_name=table_name,
                url=s3_path,
                original_url=original_url
            )
            processed += 1
        except Exception as e:
            print(f"[TASK] Error processing {s3_path}: {e}")
            errors.append({'s3_path': s3_path, 'error': str(e)})
    
    print(f"[TASK] Batch {batch_index}: {processed}/{len(downloaded)} processed successfully")
    
    return {
        'batch_index': batch_index,
        'status': 'success' if not errors else 'partial',
        'processed': processed,
        'errors': errors
    }


@task
def cleanup_batch(download_result: Dict[str, Any] = None) -> Dict[str, Any]:
    """
    Cleans up files from RustFS after processing.
    """
    from bronze.utils import delete_batch_from_rustfs
    
    if download_result is None:
        download_result = {}
    
    downloaded = download_result.get('downloaded', [])
    batch_index = download_result.get('batch_index', 0)
    
    if not downloaded:
        return {'batch_index': batch_index, 'deleted': 0}
    
    s3_paths = [item['s3_path'] for item in downloaded]
    
    print(f"[TASK] Cleaning up batch {batch_index}: {len(s3_paths)} files")
    
    results = delete_batch_from_rustfs(s3_paths)
    deleted = sum(1 for v in results.values() if v)
    
    return {'batch_index': batch_index, 'deleted': deleted}


@task
def finalize_table_task(
    dataset: str = None,
    zone_type: str = None,
    process_results: List[Dict[str, Any]] = None
) -> Dict[str, Any]:
    """
    Finalizes the table after all batches are processed.
    Runs ANALYZE once at the end.
    """
    from bronze.utils import finalize_table
    
    if not all([dataset, zone_type]):
        raise ValueError("dataset and zone_type are required")
    if process_results is None:
        process_results = []
    
    table_name = f'bronze_mitma_{dataset}_{zone_type}'
    
    # Count total processed
    total_processed = sum(r.get('processed', 0) for r in process_results if r)
    
    if total_processed == 0:
        print(f"[TASK] No records processed, skipping finalize")
        return {'status': 'skipped', 'reason': 'no_records'}
    
    print(f"[TASK] Finalizing {table_name} after {total_processed} records processed")
    
    return finalize_table(table_name, run_analyze=True)


@task.branch
def check_urls_to_process(urls: List[str] = None, group_id: str = None) -> str:
    """
    Branch task to check if there are URLs to process.
    """
    if urls is None:
        urls = []
    if group_id is None:
        raise ValueError("group_id is required")
    
    if urls and len(urls) > 0:
        print(f"[TASK] Found {len(urls)} URLs to process")
        return f"{group_id}.create_table"
    else:
        print(f"[TASK] No URLs to process, skipping")
        return f"{group_id}.skipped"


# ============================================================================
# TASK GROUP FACTORY
# ============================================================================

def create_dataset_pipeline(
    dag: DAG,
    dataset: str,
    zone_type: str,
    group_id: str,
    enabled_param: str = None
):
    """
    Factory function to create a complete dataset processing pipeline.
    
    Parameters:
    - dag: The parent DAG
    - dataset: Dataset type ('od', 'people_day', 'overnight_stay')
    - zone_type: Zone type ('municipios', 'distritos', 'gau')
    - group_id: TaskGroup ID
    - enabled_param: Optional parameter name to check if enabled
    """
    
    @task_group(group_id=group_id)
    def pipeline():
        # Get and filter URLs
        urls = get_and_filter_urls.override(
            task_id="get_urls"
        )(
            dataset=dataset,
            zone_type=zone_type,
            start_date="{{ params.start }}",
            end_date="{{ params.end }}"
        )
        
        # Branch based on URLs availability
        branch = check_urls_to_process.override(
            task_id="check_urls"
        )(urls=urls, group_id=group_id)
        
        # Create table if needed
        table_created = create_partitioned_table.override(
            task_id="create_table"
        )(
            dataset=dataset,
            zone_type=zone_type,
            urls=urls
        )
        
        # Create batches
        batches = create_url_batches_task.override(
            task_id="create_batches"
        )(
            urls=urls,
            batch_size="{{ params.batch_size }}"
        )
        
        # Download batches sequentially (dynamic task mapping)
        # Reduced to 1 batch at a time for low-resource systems
        downloaded = download_batch.override(
            task_id="download",
            pool="default_pool",
            max_active_tis_per_dag=1,  # Process one batch at a time to reduce memory usage
        ).partial(
            dataset=dataset,
            zone_type=zone_type,
            max_parallel="{{ params.max_parallel_downloads }}"
        ).expand(batch=batches)
        
        # Process batches (dynamic task mapping)
        processed = process_batch.override(
            task_id="process",
            pool="default_pool",
        ).partial(
            dataset=dataset,
            zone_type=zone_type
        ).expand(download_result=downloaded)
        
        # Cleanup batches (dynamic task mapping)
        cleaned = cleanup_batch.override(
            task_id="cleanup"
        ).expand(download_result=downloaded)
        
        # Finalize table
        finalized = finalize_table_task.override(
            task_id="finalize"
        )(
            dataset=dataset,
            zone_type=zone_type,
            process_results=processed
        )
        
        # Skipped path
        skipped = EmptyOperator(task_id="skipped")
        
        # Dependencies
        urls >> branch
        branch >> table_created >> batches >> downloaded
        branch >> skipped
        
        processed >> cleaned >> finalized
        
        # Return endpoints
        return {
            'start': urls,
            'end': [finalized, skipped]
        }
    
    return pipeline()


# ============================================================================
# DAG DEFINITION
# ============================================================================

with DAG(
    dag_id="bronze_mitma_v2",
    start_date=datetime(2025, 12, 1),
    schedule=None,
    catchup=False,
    tags=["bronze", "mitma", "v2", "optimized"],
    params={
        "start": Param(
            type="string",
            description="Start date for MITMA data loading (YYYY-MM-DD)"
        ),
        "end": Param(
            type="string",
            description="End date for MITMA data loading (YYYY-MM-DD)"
        ),
        "batch_size": Param(
            type="integer",
            default=3,
            description="Number of URLs per batch (optimized for low-resource systems like MacBook Air M2)"
        ),
        "max_parallel_downloads": Param(
            type="integer",
            default=1,
            description="Maximum parallel downloads per batch (1=sequential, recommended for 16GB RAM)"
        ),
        "enable_people_day": Param(
            type="boolean",
            default=False,
            description="Enable People Day data insertion"
        ),
        "enable_overnight": Param(
            type="boolean",
            default=False,
            description="Enable Overnight Stay data insertion"
        ),
    },
    description="MITMA Bronze layer V2 - Optimized for low-resource systems (MacBook Air M2). Uses Cloud Run for heavy processing when available.",
    default_args={
        "retries": 5,
        "retry_delay": timedelta(seconds=60),
        "retry_exponential_backoff": True,
        "max_retry_delay": timedelta(minutes=10),
    },
    max_active_tasks=4,  # Reduced for low-resource systems
    max_active_runs=1,
) as dag:
    
    # Infrastructure setup
    infra = tg_infra()
    
    zone_type = "municipios"
    
    # Create pipelines for each dataset
    od_pipeline = create_dataset_pipeline(
        dag=dag,
        dataset='od',
        zone_type=zone_type,
        group_id='od'
    )
    
    # People Day pipeline (conditional)
    @task_group(group_id="people_day")
    def people_day_group():
        @task.branch
        def check_enabled(**context):
            params = context.get('params', {})
            enabled = params.get('enable_people_day', False)
            # Handle both string and boolean values (Airflow UI can send strings)
            if isinstance(enabled, str):
                enabled = enabled.lower() in ('true', '1', 'yes')
            if enabled:
                return "people_day.pipeline"
            return "people_day.skipped"
        
        branch = check_enabled()
        
        # Simplified pipeline for people_day
        @task_group(group_id="pipeline")
        def pipeline():
            urls = get_and_filter_urls(
                dataset='people_day',
                zone_type=zone_type,
                start_date="{{ params.start }}",
                end_date="{{ params.end }}"
            )
            
            table = create_partitioned_table(
                dataset='people_day',
                zone_type=zone_type,
                urls=urls
            )
            
            batches = create_url_batches_task(urls, batch_size="{{ params.batch_size }}")
            
            downloaded = download_batch.partial(
                dataset='people_day',
                zone_type=zone_type,
                max_parallel="{{ params.max_parallel_downloads }}"
            ).expand(batch=batches)
            
            processed = process_batch.partial(
                dataset='people_day',
                zone_type=zone_type
            ).expand(download_result=downloaded)
            
            cleaned = cleanup_batch.expand(download_result=downloaded)
            
            finalized = finalize_table_task(
                dataset='people_day',
                zone_type=zone_type,
                process_results=processed
            )
            
            urls >> table >> batches >> downloaded
            processed >> cleaned >> finalized
            
            return finalized
        
        inner_pipeline = pipeline()
        skipped = EmptyOperator(task_id="skipped")
        
        branch >> inner_pipeline
        branch >> skipped
    
    people_day = people_day_group()
    
    # Overnight Stay pipeline (conditional)
    @task_group(group_id="overnight")
    def overnight_group():
        @task.branch
        def check_enabled(**context):
            params = context.get('params', {})
            enabled = params.get('enable_overnight', False)
            # Handle both string and boolean values (Airflow UI can send strings)
            if isinstance(enabled, str):
                enabled = enabled.lower() in ('true', '1', 'yes')
            if enabled:
                return "overnight.pipeline"
            return "overnight.skipped"
        
        branch = check_enabled()
        
        @task_group(group_id="pipeline")
        def pipeline():
            urls = get_and_filter_urls(
                dataset='overnight_stay',
                zone_type=zone_type,
                start_date="{{ params.start }}",
                end_date="{{ params.end }}"
            )
            
            table = create_partitioned_table(
                dataset='overnight_stay',
                zone_type=zone_type,
                urls=urls
            )
            
            batches = create_url_batches_task(urls, batch_size="{{ params.batch_size }}")
            
            downloaded = download_batch.partial(
                dataset='overnight_stay',
                zone_type=zone_type,
                max_parallel="{{ params.max_parallel_downloads }}"
            ).expand(batch=batches)
            
            processed = process_batch.partial(
                dataset='overnight_stay',
                zone_type=zone_type
            ).expand(download_result=downloaded)
            
            cleaned = cleanup_batch.expand(download_result=downloaded)
            
            finalized = finalize_table_task(
                dataset='overnight_stay',
                zone_type=zone_type,
                process_results=processed
            )
            
            urls >> table >> batches >> downloaded
            processed >> cleaned >> finalized
            
            return finalized
        
        inner_pipeline = pipeline()
        skipped = EmptyOperator(task_id="skipped")
        
        branch >> inner_pipeline
        branch >> skipped
    
    overnight = overnight_group()
    
    # Zonification (uses existing task)
    from bronze.tasks.mitma import (
        BRONZE_mitma_zonification_urls,
        BRONZE_mitma_zonification,
    )
    
    @task_group(group_id="zonification")
    def zonification_group():
        zonif_urls = BRONZE_mitma_zonification_urls(zone_type=zone_type)
        
        @task.branch
        def check_zonif(urls_dict: dict):
            shp_components = urls_dict.get('shp_components', [])
            if len(shp_components) > 0:
                return "zonification.zonif"
            return "zonification.skipped"
        
        branch = check_zonif(zonif_urls)
        
        zonif = BRONZE_mitma_zonification.override(task_id="zonif")(zone_type=zone_type)
        skipped = EmptyOperator(task_id="skipped")
        
        zonif_urls >> branch
        branch >> zonif
        branch >> skipped
    
    zonification = zonification_group()
    
    # Final task
    bronze_mitma_asset = Dataset("bronze://mitma/done")
    
    done = EmptyOperator(
        task_id="done",
        outlets=[bronze_mitma_asset],
        trigger_rule="none_failed"
    )
    
    # Dependencies
    infra.bucket >> [od_pipeline['start'], people_day, overnight, zonification]
    
    od_pipeline['end'][0] >> done  # finalized
    od_pipeline['end'][1] >> done  # skipped
    people_day >> done
    overnight >> done
    zonification >> done

