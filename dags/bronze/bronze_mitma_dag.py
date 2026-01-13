"""
DAG for MITMA Bronze layer data ingestion.
Orchestrates MITMA OD, People Day, Overnights, and Zonification data loading.
"""

from datetime import datetime, timedelta
from types import SimpleNamespace

from airflow import DAG
from airflow.datasets import Dataset
from airflow.models import Param
from airflow.sdk import task, task_group
from airflow.providers.standard.operators.empty import EmptyOperator

from misc.infra import tg_infra

from utils.utils import get_default_pool_slots

from bronze.tasks.mitma import (
    BRONZE_mitma_od_urls,
    BRONZE_mitma_od_create_table,
    BRONZE_mitma_od_filter_urls,
    BRONZE_mitma_od_process,
    BRONZE_mitma_od_create_url_batches,
    BRONZE_mitma_od_create_partitioned_table,
    BRONZE_mitma_od_download_batch,
    BRONZE_mitma_od_process_batch,
    BRONZE_mitma_od_download_and_process_batch,
    BRONZE_mitma_od_cleanup_batch,
    BRONZE_mitma_people_day_urls,
    BRONZE_mitma_people_day_create_table,
    BRONZE_mitma_people_day_filter_urls,
    BRONZE_mitma_people_day_insert,
    BRONZE_mitma_overnight_stay_urls,
    BRONZE_mitma_overnight_stay_create_table,
    BRONZE_mitma_overnight_stay_filter_urls,
    BRONZE_mitma_overnight_stay_insert,
    BRONZE_mitma_zonification_urls,
    BRONZE_mitma_zonification,
    BRONZE_mitma_ine_relations,
)

default_pool_slots = get_default_pool_slots()

def create_tg_od(zone_type: str):
    """Factory function to create OD TaskGroup with dynamic group_id."""
    group_id = f"od_{zone_type}"
    
    @task_group(group_id=group_id)
    def tg_od(zone_type: str):
        """TaskGroup for OD (Origin-Destination) data ingestion."""
        od_urls = BRONZE_mitma_od_urls.override(
            task_id="od_urls",
        )(
            zone_type=zone_type,
            start_date="{{ params.start }}",
            end_date="{{ params.end }}",
        )

        od_filtered_urls = BRONZE_mitma_od_filter_urls.override(
            task_id="od_filter_urls",
        )(
            urls=od_urls,
            zone_type=zone_type,
        )

        @task.branch
        def check_urls_to_process(filtered_urls: list[str], **context):
            """Check if there are URLs to process."""
            if len(filtered_urls) > 0:
                return f"{group_id}.od_create_table"
            else:
                return f"{group_id}.od_skipped"

        branch_task = check_urls_to_process.override(
            task_id="check_urls"
        )(od_filtered_urls)

        # Create partitioned table with fecha as TIMESTAMP
        od_create_table = BRONZE_mitma_od_create_partitioned_table.override(
            task_id="od_create_table",
        )(
            urls=od_urls,
            zone_type=zone_type,
        )
        
        # Create URL batches using batch_size parameter from DAG params
        @task
        def create_batches(filtered_urls: list[str], **context):
            """Create batches from filtered URLs using batch_size parameter."""
            batch_size = context['params'].get('batch_size', 10)
            from bronze.utils import create_url_batches
            batches = create_url_batches(filtered_urls, batch_size)
            return [
                {'batch_index': i, 'urls': batch}
                for i, batch in enumerate(batches)
            ]
        
        od_batches = create_batches.override(
            task_id="od_create_batches"
        )(od_filtered_urls)
        
        # FASE 1: Descargar URLs y subir a RustFS (local - bajo consumo de recursos)
        # Dynamic task mapping para descargar cada batch en paralelo (local)
        od_download = (
            BRONZE_mitma_od_download_batch.override(
                task_id="od_download_to_rustfs",
                pool="default_pool",
            )
            .partial(zone_type=zone_type)
            .expand(batch=od_batches)
        )
        
        # FASE 2: Procesar archivos desde RustFS con Cloud Run (alto consumo de recursos)
        # Dynamic task mapping para procesar cada batch descargado
        od_process = (
            BRONZE_mitma_od_process_batch.override(
                task_id="od_process_cloudrun",
                pool="default_pool",
            )
            .partial(zone_type=zone_type)
            .expand(download_result=od_download)
        )
        
        # FASE 3: Limpieza de archivos temporales de RustFS después del procesamiento
        # Usa el resultado de od_process (que incluye download_result original)
        od_cleanup = (
            BRONZE_mitma_od_cleanup_batch.override(
                task_id="od_cleanup_rustfs",
                pool="default_pool",
            )
            .expand(download_result=od_process)
        )

        od_skipped = EmptyOperator(
            task_id="od_skipped"
        )

        od_urls >> od_filtered_urls >> branch_task

        branch_task >> od_create_table >> od_batches >> od_download >> od_process >> od_cleanup
        branch_task >> od_skipped
        
        return SimpleNamespace(
            start=od_urls,
            insert=[od_cleanup, od_skipped],
        )
    
    return tg_od(zone_type=zone_type)


def create_tg_people_day(zone_type: str):
    """Factory function to create People Day TaskGroup with dynamic group_id."""
    group_id = f"people_day_{zone_type}"
    
    @task_group(group_id=group_id)
    def tg_people_day(zone_type: str):
        """TaskGroup for People Day data ingestion."""
        people_urls = BRONZE_mitma_people_day_urls.override(
            task_id="people_urls",
        )(
            zone_type=zone_type,
            start_date="{{ params.start }}",
            end_date="{{ params.end }}",
        )

        people_filtered_urls = BRONZE_mitma_people_day_filter_urls.override(
            task_id="people_filter_urls",
        )(
            urls=people_urls,
            zone_type=zone_type,
        )

        # Check if enabled and if there are URLs to process
        @task.branch
        def check_people_day_urls(filtered_urls: list[str], **context):
            """Check if people_day is enabled and has URLs to process."""
            enabled = context['params'].get('enable_people_day', False)
            if not enabled:
                return f"{group_id}.people_skipped"
            if len(filtered_urls) > 0:
                return f"{group_id}.people_create"
            else:
                return f"{group_id}.people_skipped"

        branch_task = check_people_day_urls.override(
            task_id="check_urls"
        )(people_filtered_urls)

        people_create = BRONZE_mitma_people_day_create_table.override(
            task_id="people_create",
        )(
            urls=people_urls,
            zone_type=zone_type,
        )
        
        people_insert = (
            BRONZE_mitma_people_day_insert.override(
                task_id="people_insert",
                pool="default_pool", 
                max_active_tis_per_dag=1,
                pool_slots=default_pool_slots,
            )
            .partial(zone_type=zone_type)
            .expand(url=people_filtered_urls)
        )
        
        people_skipped = EmptyOperator(
            task_id="people_skipped"
        )

        people_urls >> people_filtered_urls >> branch_task

        branch_task >> people_create >> people_insert
        branch_task >> people_skipped
        
        return SimpleNamespace(
            start=people_urls,
            insert=[people_insert, people_skipped],
        )
    
    return tg_people_day(zone_type=zone_type)


def create_tg_overnight(zone_type: str):
    """Factory function to create Overnight Stay TaskGroup with dynamic group_id."""
    group_id = f"overnight_{zone_type}"
    
    @task_group(group_id=group_id)
    def tg_overnight(zone_type: str):
        """TaskGroup for Overnight Stay data ingestion."""
        overnight_urls = BRONZE_mitma_overnight_stay_urls.override(
            task_id="overnight_urls",
        )(
            zone_type=zone_type,
            start_date="{{ params.start }}",
            end_date="{{ params.end }}",
        )

        overnight_filtered_urls = BRONZE_mitma_overnight_stay_filter_urls.override(
            task_id="overnight_filter_urls",
        )(
            urls=overnight_urls,
            zone_type=zone_type,
        )

        @task.branch
        def check_overnight_urls(filtered_urls: list[str], **context):
            """Check if overnight is enabled and has URLs to process."""
            enabled = context['params'].get('enable_overnight', False)
            if not enabled:
                return f"{group_id}.overnight_skipped"
            if len(filtered_urls) > 0:
                return f"{group_id}.overnight_create"
            else:
                return f"{group_id}.overnight_skipped"
        
        branch_task = check_overnight_urls.override(
            task_id="check_urls"
        )(overnight_filtered_urls)

        overnight_create = BRONZE_mitma_overnight_stay_create_table.override(
            task_id="overnight_create",
        )(
            urls=overnight_urls,
            zone_type=zone_type,
        )
        
        # Overnight insert: limitado a 1 instancia concurrente
        overnight_insert = (
            BRONZE_mitma_overnight_stay_insert.override(
                task_id="overnight_insert",
                pool="default_pool", 
                max_active_tis_per_dag=1,
                pool_slots=default_pool_slots, 
            )
            .partial(zone_type=zone_type)
            .expand(url=overnight_filtered_urls)
        )
        
        overnight_skipped = EmptyOperator(
            task_id="overnight_skipped"
        )

        overnight_urls >> overnight_filtered_urls >> branch_task

        branch_task >> overnight_create >> overnight_insert
        branch_task >> overnight_skipped
        
        return SimpleNamespace(
            start=overnight_urls,
            insert=[overnight_insert, overnight_skipped],
        )
    
    return tg_overnight(zone_type=zone_type)


def create_tg_zonification(zone_type: str):
    """Factory function to create Zonification TaskGroup with dynamic group_id."""
    group_id = f"zonification"
    
    @task_group(group_id=group_id)
    def tg_zonification(zone_type: str):
        """TaskGroup for Zonification data ingestion."""
        zonif_urls = BRONZE_mitma_zonification_urls.override(
            task_id="zonif_urls",
        )(zone_type=zone_type)

        @task.branch
        def check_zonif_urls(urls_dict: dict, **context):
            """Check if there are URLs to process."""
            shp_components = urls_dict.get('shp_components', [])
            if len(shp_components) > 0:
                return f"{group_id}.zonif_{zone_type}"
            else:
                return f"{group_id}.zonif_skipped"

        zonif_branch = check_zonif_urls.override(
            task_id="check_urls"
        )(zonif_urls)

        zonif = BRONZE_mitma_zonification.override(
            task_id=f"zonif_{zone_type}"
        )(zone_type=zone_type)

        zonif_skipped = EmptyOperator(
            task_id="zonif_skipped"
        )

        zonif_urls >> zonif_branch
        zonif_branch >> zonif
        zonif_branch >> zonif_skipped
        
        return SimpleNamespace(
            start=zonif_urls,
            insert=[zonif, zonif_skipped],
        )
    
    return tg_zonification(zone_type=zone_type)


def create_tg_ine_relations():
    """Factory function to create MITMA-INE Relations TaskGroup."""
    group_id = "ine_relations"
    
    @task_group(group_id=group_id)
    def tg_ine_relations():
        """TaskGroup for MITMA-INE Relations data ingestion.
        Simplified: reads directly from URL (lightweight operation).
        """
        # Single task that reads directly from URL and creates table
        relations_load = BRONZE_mitma_ine_relations.override(
            task_id="relations_load",
        )()
        
        return SimpleNamespace(
            start=relations_load,
            end=relations_load,
        )
    
    return tg_ine_relations()


with DAG(
    dag_id="bronze_mitma",
    start_date=datetime(2025, 12, 1),
    schedule=None,
    catchup=False,
    tags=["bronze", "mitma", "data-ingestion"],
    params={
        "start": Param(type="string", schema={"type": "string", "format": "date"}, description="Start date for MITMA data loading"),
        "end": Param(type="string", schema={"type": "string", "format": "date"}, description="End date for MITMA data loading"),
        "batch_size": Param(type="integer", default=10, description="Number of URLs per batch for processing"),
        "enable_people_day": Param(type="boolean", default=False, description="Enable People Day data insertion"),
        "enable_overnight": Param(type="boolean", default=False, description="Enable Overnight Stay data insertion"),
    },
    description="MITMA Bronze layer data ingestion (OD, People Day, Overnights, Zonification, INE Relations)",
    default_args={
        "retries": 3,
        "retry_delay": timedelta(seconds=30),
    },
    max_active_tasks=4,  # Máximo de tareas concurrentes en el DAG (permite paralelismo general)
    max_active_runs=1,    # Solo 1 ejecución del DAG a la vez
) as dag:
    infra = tg_infra()
    zone_type = "municipios"

    od_group = create_tg_od(zone_type=zone_type)
    people_day_group = create_tg_people_day(zone_type=zone_type)
    overnight_group = create_tg_overnight(zone_type=zone_type)
    zonif_group = create_tg_zonification(zone_type=zone_type)
    ine_relations_group = create_tg_ine_relations()
    
    infra.bucket >> [od_group.start, people_day_group.start, overnight_group.start, zonif_group.start, ine_relations_group.start]
    
    bronze_mitma_asset = Dataset("bronze://mitma/done")
    
    done = EmptyOperator(
        task_id="done",
        outlets=[bronze_mitma_asset],  # Publish asset when task completes successfully
        trigger_rule="none_failed"  # Wait for tasks that execute (skipped ones are ignored)
    )
    
    people_day_group.insert[0] >> done  # actual insert (if enabled)
    people_day_group.insert[1] >> done  # skipped insert (if disabled)
    overnight_group.insert[0] >> done   # actual insert (if enabled)
    overnight_group.insert[1] >> done   # skipped insert (if disabled)
    od_group.insert[0] >> done          # actual insert (if URLs exist)
    od_group.insert[1] >> done          # skipped insert (if no URLs)
    zonif_group.insert[0] >> done       # zonification if URLs exist
    zonif_group.insert[1] >> done       # zonification skipped if no URLs
    ine_relations_group.end >> done     # ine relations (single local task)