"""
DAG for MITMA Bronze layer data ingestion.
Orchestrates MITMA OD and Zonification data loading.
"""

from datetime import datetime, timedelta
from types import SimpleNamespace

from airflow import DAG
from airflow.datasets import Dataset
from airflow.models import Param
from airflow.sdk import task, task_group
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator

from misc.infra import tg_infra
from misc.tasks.ensure_s3_bucket import PRE_s3_bucket

from utils.utils import get_default_pool_slots

from bronze.tasks.mitma import (
    BRONZE_mitma_od_urls,
    BRONZE_mitma_od_create_table,
    BRONZE_mitma_od_download_batch,
    BRONZE_mitma_od_process_batch,
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

        @task.branch
        def check_urls_to_process(urls: list[str], **context):
            """Check if there are URLs to process."""
            if len(urls) > 0:
                return f"{group_id}.od_create_table"
            else:
                return f"{group_id}.od_skipped"

        branch_task = check_urls_to_process.override(
            task_id="check_urls"
        )(od_urls)

        od_create_table = BRONZE_mitma_od_create_table.override(
            task_id="od_create_table",
        )(
            urls=od_urls,
            zone_type=zone_type,
        )
        
        @task
        def create_batches(urls: list[str], **context):
            """Create batches from filtered URLs using batch_size parameter."""
            batch_size = context['params'].get('batch_size', 2)
            from bronze.utils import create_url_batches
            batches = create_url_batches(urls, batch_size)
            return [
                {'batch_index': i, 'urls': batch}
                for i, batch in enumerate(batches)
            ]
        
        od_batches = create_batches.override(
            task_id="od_create_batches"
        )(od_urls)
        od_download = (
            BRONZE_mitma_od_download_batch.override(
                task_id="od_download_to_rustfs",
                pool="default_pool",
            )
            .partial(zone_type=zone_type)
            .expand(batch=od_batches)
        )
        
        od_process = (
            BRONZE_mitma_od_process_batch.override(
                task_id="od_process_cloudrun",
                pool="default_pool",
            )
            .partial(zone_type=zone_type)
            .expand(download_result=od_download)
        )

        od_skipped = EmptyOperator(
            task_id="od_skipped"
        )

        od_urls >> branch_task

        branch_task >> od_create_table >> od_batches >> od_download >> od_process
        branch_task >> od_skipped
        
        return SimpleNamespace(
            start=od_urls,
            insert=[od_process, od_skipped],
        )
    
    return tg_od(zone_type=zone_type)


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
        "batch_size": Param(type="integer", default=2, description="Number of URLs per batch for processing"),
    },
    description="MITMA Bronze layer data ingestion (OD, Zonification, INE Relations)",
    default_args={
        "retries": 3,
        "retry_delay": timedelta(seconds=30),
    },
    max_active_tasks=4,  # Máximo de tareas concurrentes en el DAG (permite paralelismo general)
    max_active_runs=1,    # Solo 1 ejecución del DAG a la vez
) as dag:
    from airflow.sdk import Variable
    
    infra = tg_infra()
    
    # Ensure RAW bucket exists
    raw_bucket = PRE_s3_bucket.override(task_id="ensure_raw_bucket")(
        bucket_name=Variable.get('RAW_BUCKET', default='mitma-raw')
    )
    
    zone_type = "municipios"

    od_group = create_tg_od(zone_type=zone_type)
    zonif_group = create_tg_zonification(zone_type=zone_type)
    ine_relations_group = create_tg_ine_relations()
    
    infra.bucket >> [od_group.start, zonif_group.start, ine_relations_group.start]
    raw_bucket >> [od_group.start, zonif_group.start, ine_relations_group.start]
    
    bronze_mitma_asset = Dataset("bronze://mitma/done")
    bronze_mitma_checker_asset = Dataset("bronze://mitma/checker_done")  # Dataset específico cuando viene del checker
    
    done = EmptyOperator(
        task_id="done",
        outlets=[bronze_mitma_asset],  # Publish asset when task completes successfully
        trigger_rule="none_failed"  # Wait for tasks that execute (skipped ones are ignored)
    )
    
    # Task que triggera el silver_dag si viene del checker
    @task.branch
    def check_if_triggered_by_checker(**context):
        """Verifica si el DAG fue triggerado por el checker."""
        triggered_by_checker = context.get('params', {}).get('triggered_by_checker', False)
        if triggered_by_checker:
            print("[TASK] DAG triggered by checker. Will trigger silver_dag with specific params.")
            return "trigger_silver_from_checker"
        else:
            print("[TASK] DAG not triggered by checker. Normal flow.")
            return "skip_trigger_silver"
    
    check_trigger = check_if_triggered_by_checker.override(task_id="check_checker_trigger")()
    
    # Trigger del silver_dag cuando viene del checker
    trigger_silver = TriggerDagRunOperator(
        task_id="trigger_silver_from_checker",
        trigger_dag_id="silver",
        conf={
            "start": "{{ params.start }}",
            "end": "{{ params.end }}",
            "process_zonification": False,
            "process_ine": False,
        },
        wait_for_completion=False,
    )
    
    skip_trigger = EmptyOperator(task_id="skip_trigger_silver")
    
    od_group.insert[0] >> done          
    od_group.insert[1] >> done          
    zonif_group.insert[0] >> done      
    zonif_group.insert[1] >> done       
    ine_relations_group.end >> done     
    
    done >> check_trigger
    check_trigger >> trigger_silver
    check_trigger >> skip_trigger