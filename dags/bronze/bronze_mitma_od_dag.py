"""
DAG optimizado para cargar solo las matrices origen-destino (OD) de MITMA.
Procesa todo directamente en Cloud Run desde URLs HTTP sin descargar a RustFS.

Optimizaciones clave:
- Procesamiento 100% en Cloud Run desde URLs HTTP (sin descargas locales)
- DuckDB 1.4.X con ducklake desde core_nightly
- Optimización mejorada de batches para Cloud Run
- Extensiones httpfs y postgres instaladas por defecto (spatial solo cuando se necesite)

Mejoras respecto a versiones anteriores:
- No descarga archivos a RustFS, procesa directamente desde HTTP
- Mejor gestión de memoria y batches optimizados para Cloud Run
- Menor latencia al eliminar el paso de descarga local

Usage:
    Trigger with parameters:
    - start: Start date (YYYY-MM-DD)
    - end: End date (YYYY-MM-DD)
    - batch_size: Number of URLs per batch (default: 5, optimized for Cloud Run)
    - zone_type: Zone type ('municipios', 'distritos', 'gau') (default: 'municipios')
"""

from datetime import datetime, timedelta
from typing import List, Dict, Any

from airflow import DAG
from airflow.datasets import Dataset
from airflow.models import Param
from airflow.sdk import task, task_group
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
def create_url_batches_task(urls: List[str] = None, batch_size: Any = 5) -> List[Dict[str, Any]]:
    """
    Divides URLs into batches optimized for Cloud Run processing.
    Returns list of dicts with batch_index and urls.
    """
    from bronze.utils import create_url_batches
    
    if urls is None:
        urls = []
    
    # Convert batch_size to int (handles template strings from Airflow params)
    batch_size = int(batch_size) if batch_size else 5
    
    if not urls:
        print("[TASK] No URLs to batch")
        return []
    
    batches = create_url_batches(urls, batch_size)
    
    result = [
        {'batch_index': i, 'urls': batch}
        for i, batch in enumerate(batches)
    ]
    
    print(f"[TASK] Created {len(result)} batches (optimized for Cloud Run)")
    return result


@task
def create_partitioned_table(
    dataset: str = None,
    zone_type: str = None,
    urls: List[str] = None
) -> Dict[str, Any]:
    """
    Creates the Bronze table with partitioning if it doesn't exist.
    Uses first URL to infer schema (can be HTTP URL directly).
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
    # Note: create_partitioned_table_from_csv can handle HTTP URLs directly via DuckDB httpfs
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
def process_batch_http(
    batch: Dict[str, Any] = None,
    dataset: str = None,
    zone_type: str = None
) -> Dict[str, Any]:
    """
    Processes a batch of HTTP URLs directly in Cloud Run.
    No downloads to RustFS - everything processed in Cloud Run from HTTP URLs.
    
    This is the key optimization: we skip the download step entirely and
    process directly from HTTP URLs in Cloud Run, which is much faster and
    avoids potential container OOM issues from local file storage.
    """
    if batch is None:
        batch = {}
    if not all([dataset, zone_type]):
        raise ValueError("dataset and zone_type are required")
    
    urls = batch.get('urls', [])
    batch_index = batch.get('batch_index', 0)
    
    if not urls:
        return {
            'batch_index': batch_index,
            'status': 'skipped',
            'processed': 0
        }
    
    # Use full table name with bronze_ prefix (as created by create_partitioned_table_from_csv)
    table_name = f'bronze_mitma_{dataset}_{zone_type}'
    processed = 0
    errors = []
    
    print(f"[TASK] Processing batch {batch_index} in Cloud Run: {len(urls)} HTTP URLs")
    print(f"[TASK] All processing happens in Cloud Run - no local downloads")
    print(f"[TASK] Target table: {table_name}")
    
    for url in urls:
        try:
            # Process directly from HTTP URL in Cloud Run
            # merge_csv_or_cloud_run will detect HTTP URLs and use exec_gcp_ducklake_ingestor_from_http
            # Note: We pass the full table name with bronze_ prefix
            merge_csv_or_cloud_run(
                table_name=table_name,
                url=url,
                original_url=url  # Original URL is the same since we're processing directly
            )
            processed += 1
            print(f"[TASK] Processed {processed}/{len(urls)}: {url.split('/')[-1]}")
        except Exception as e:
            print(f"[TASK] Error processing {url}: {e}")
            errors.append({'url': url, 'error': str(e)})
    
    print(f"[TASK] Batch {batch_index}: {processed}/{len(urls)} processed successfully")
    
    return {
        'batch_index': batch_index,
        'status': 'success' if not errors else 'partial',
        'processed': processed,
        'errors': errors
    }


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
def check_urls_to_process(urls: List[str] = None) -> str:
    """
    Branch task to check if there are URLs to process.
    """
    if urls is None:
        urls = []
    
    if urls and len(urls) > 0:
        print(f"[TASK] Found {len(urls)} URLs to process")
        return "create_partitioned_table"
    else:
        print(f"[TASK] No URLs to process, skipping")
        return "skipped"


# ============================================================================
# DAG DEFINITION
# ============================================================================

with DAG(
    dag_id="bronze_mitma_od",
    start_date=datetime(2025, 12, 1),
    schedule=None,
    catchup=False,
    tags=["bronze", "mitma", "od", "optimized", "cloud-run"],
    params={
        "start": Param(
            type="string",
            description="Start date for MITMA OD data loading (YYYY-MM-DD)"
        ),
        "end": Param(
            type="string",
            description="End date for MITMA OD data loading (YYYY-MM-DD)"
        ),
        "batch_size": Param(
            type="integer",
            default=5,
            description="Number of URLs per batch (optimized for Cloud Run processing)"
        ),
        "zone_type": Param(
            type="string",
            default="municipios",
            description="Zone type: 'municipios', 'distritos', or 'gau'"
        ),
    },
    description="MITMA Bronze layer - Origin-Destination matrices only. Optimized for Cloud Run with direct HTTP processing (no RustFS downloads).",
    default_args={
        "retries": 5,
        "retry_delay": timedelta(seconds=60),
        "retry_exponential_backoff": True,
        "max_retry_delay": timedelta(minutes=10),
    },
    max_active_tasks=10,  # More parallel tasks since we're using Cloud Run
    max_active_runs=1,
) as dag:
    
    # Infrastructure setup
    infra = tg_infra()
    
    # Get parameters
    zone_type = "{{ params.zone_type }}"
    dataset = 'od'
    
    # Get and filter URLs
    urls = get_and_filter_urls(
        dataset=dataset,
        zone_type=zone_type,
        start_date="{{ params.start }}",
        end_date="{{ params.end }}"
    )
    
    # Branch based on URLs availability
    branch = check_urls_to_process(urls)
    
    # Create table if needed
    table_created = create_partitioned_table(
        dataset=dataset,
        zone_type=zone_type,
        urls=urls
    )
    
    # Create batches
    batches = create_url_batches_task(
        urls=urls,
        batch_size="{{ params.batch_size }}"
    )
    
    # Process batches directly from HTTP URLs in Cloud Run (dynamic task mapping)
    processed = process_batch_http.override(
        task_id="process",
        pool="default_pool",
    ).partial(
        dataset=dataset,
        zone_type=zone_type
    ).expand(batch=batches)
    
    # Finalize table
    finalized = finalize_table_task(
        dataset=dataset,
        zone_type=zone_type,
        process_results=processed
    )
    
    # Skipped path
    skipped = EmptyOperator(task_id="skipped")
    
    # Final task
    bronze_mitma_od_asset = Dataset("bronze://mitma/od/done")
    
    done = EmptyOperator(
        task_id="done",
        outlets=[bronze_mitma_od_asset],
        trigger_rule="none_failed"
    )
    
    # Dependencies
    infra.bucket >> urls
    urls >> branch
    branch >> table_created >> batches >> processed >> finalized >> done
    branch >> skipped >> done

