"""
DAG optimizado para cargar solo las matrices origen-destino (OD) de MITMA.
Usa DuckLake executor con SQL directo en lugar del ingestor.

Optimizaciones clave:
- Usa executor ducklake con SQL directo (no ingestor)
- INSERT en lugar de MERGE para mejor rendimiento
- Tracking de fechas ingestadas usando SELECT DISTINCT
- Fecha casteada a TIMESTAMP para particionado eficiente
- Filtrado de URLs por fechas faltantes antes de procesar

Usage:
    Trigger with parameters:
    - start: Start date (YYYY-MM-DD)
    - end: End date (YYYY-MM-DD)
    - batch_size: Number of URLs per batch (default: 5)
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
from bronze_2.tasks.mitma_od import (
    get_and_filter_urls,
    create_partitioned_table,
    create_url_batches,
    process_batch_insert,
    finalize_table
)


# ============================================================================
# DAG DEFINITION
# ============================================================================

with DAG(
    dag_id="bronze_2_mitma_od",
    start_date=datetime(2025, 12, 1),
    schedule=None,
    catchup=False,
    tags=["bronze_2", "mitma", "od", "executor", "optimized"],
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
    description="MITMA Bronze 2 layer - Origin-Destination matrices using DuckLake executor with INSERT and date tracking.",
    default_args={
        "retries": 5,
        "retry_delay": timedelta(seconds=60),
        "retry_exponential_backoff": True,
        "max_retry_delay": timedelta(minutes=10),
    },
    max_active_tasks=10,
    max_active_runs=1,
) as dag:
    
    # Infrastructure setup
    infra = tg_infra()
    
    # Get parameters
    zone_type = "{{ params.zone_type }}"
    dataset = 'od'
    
    # Get and filter URLs (excludes already ingested dates)
    filtered_urls = get_and_filter_urls(
        dataset=dataset,
        zone_type=zone_type,
        start_date="{{ params.start }}",
        end_date="{{ params.end }}"
    )
    
    # Branch task to check if there are URLs to process
    @task.branch
    def check_urls_to_process(urls: List[str] = None) -> str:
        """Branch task to check if there are URLs to process."""
        if urls is None:
            urls = []
        
        if urls and len(urls) > 0:
            print(f"[TASK] Found {len(urls)} URLs to process")
            return "create_partitioned_table"
        else:
            print(f"[TASK] No URLs to process, skipping")
            return "skipped"
    
    branch = check_urls_to_process(filtered_urls)
    
    # Get first URL for table creation (if URLs exist)
    @task
    def get_first_url(urls: List[str] = None) -> str:
        """Gets the first URL from the list for table schema creation."""
        if urls and len(urls) > 0:
            return urls[0]
        return None
    
    first_url = get_first_url(filtered_urls)
    
    # Create table if needed (use first URL for schema)
    table_created = create_partitioned_table(
        dataset=dataset,
        zone_type=zone_type,
        url=first_url
    )
    
    # Create batches
    batches = create_url_batches(
        urls=filtered_urls,
        batch_size="{{ params.batch_size }}"
    )
    
    # Process batches using INSERT with executor ducklake (dynamic task mapping)
    processed = process_batch_insert.override(
        task_id="process",
        pool="default_pool",
    ).partial(
        dataset=dataset,
        zone_type=zone_type
    ).expand(batch=batches)
    
    # Finalize table
    finalized = finalize_table(
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
    infra.bucket >> filtered_urls
    filtered_urls >> branch
    filtered_urls >> first_url
    first_url >> table_created
    branch >> table_created >> batches >> processed >> finalized >> done
    branch >> skipped >> done

