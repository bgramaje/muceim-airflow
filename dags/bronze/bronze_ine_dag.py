"""
DAG for INE Bronze layer data ingestion.
Orchestrates INE municipios, empresas, poblacion, renta, and MITMA-INE relations.
"""

from datetime import datetime, timedelta
from types import SimpleNamespace

from airflow import DAG
from airflow.datasets import Dataset
from airflow.models import Param
from airflow.sdk import task, task_group
from airflow.providers.standard.operators.empty import EmptyOperator

# Import infrastructure setup TaskGroup
from misc.infra import tg_infra

from utils.utils import get_default_pool_slots

# Import Bronze INE tasks
from bronze.tasks.ine import (
    BRONZE_ine_municipios_urls,
    BRONZE_ine_municipios_create_table,
    BRONZE_ine_municipios_filter_urls,
    BRONZE_ine_municipios_insert,
    BRONZE_ine_empresas_municipio_urls,
    BRONZE_ine_empresas_municipio_create_table,
    BRONZE_ine_empresas_municipio_filter_urls,
    BRONZE_ine_empresas_municipio_insert,
    BRONZE_ine_poblacion_municipio_urls,
    BRONZE_ine_poblacion_municipio_create_table,
    BRONZE_ine_poblacion_municipio_filter_urls,
    BRONZE_ine_poblacion_municipio_insert,
    BRONZE_ine_renta_urls,
    BRONZE_ine_renta_create_table,
    BRONZE_ine_renta_filter_urls,
    BRONZE_ine_renta_insert,
)

default_pool_slots = get_default_pool_slots()


def create_tg_municipios():
    """Factory function to create Municipios TaskGroup."""
    group_id = "municipios"
    
    @task_group(group_id=group_id)
    def tg_municipios():
        """TaskGroup for INE Municipios data ingestion."""
        municipios_urls = BRONZE_ine_municipios_urls.override(
            task_id="municipios_urls",
        )()

        municipios_filtered_urls = BRONZE_ine_municipios_filter_urls.override(
            task_id="municipios_filter_urls",
        )(urls=municipios_urls)

        @task.branch
        def check_municipios_urls(filtered_urls: list[str], **context):
            """Check if there are URLs to process."""
            if len(filtered_urls) > 0:
                return f"{group_id}.municipios_create"
            else:
                return f"{group_id}.municipios_skipped"

        branch_task = check_municipios_urls.override(
            task_id="check_urls"
        )(municipios_filtered_urls)

        municipios_create = BRONZE_ine_municipios_create_table.override(
            task_id="municipios_create",
        )(urls=municipios_urls)
        
        municipios_insert = (
            BRONZE_ine_municipios_insert.override(
                task_id="municipios_insert",
                pool="default_pool", 
                max_active_tis_per_dag=1,
                pool_slots=default_pool_slots,
            )
            .expand(url=municipios_filtered_urls)
        )

        municipios_skipped = EmptyOperator(
            task_id="municipios_skipped"
        )

        municipios_urls >> municipios_filtered_urls >> branch_task

        branch_task >> municipios_create >> municipios_insert
        branch_task >> municipios_skipped
        
        return SimpleNamespace(
            start=municipios_urls,
            insert=[municipios_insert, municipios_skipped],
        )
    
    return tg_municipios()


def create_tg_empresas():
    """Factory function to create Empresas TaskGroup."""
    group_id = "empresas"
    
    @task_group(group_id=group_id)
    def tg_empresas():
        """TaskGroup for INE Empresas data ingestion."""
        empresas_urls = BRONZE_ine_empresas_municipio_urls.override(
            task_id="empresas_urls",
        )(year="{{ params.year }}")

        empresas_filtered_urls = BRONZE_ine_empresas_municipio_filter_urls.override(
            task_id="empresas_filter_urls",
        )(urls=empresas_urls)

        @task.branch
        def check_empresas_urls(filtered_urls: list[str], **context):
            """Check if there are URLs to process."""
            if len(filtered_urls) > 0:
                return f"{group_id}.empresas_create"
            else:
                return f"{group_id}.empresas_skipped"

        branch_task = check_empresas_urls.override(
            task_id="check_urls"
        )(empresas_filtered_urls)

        empresas_create = BRONZE_ine_empresas_municipio_create_table.override(
            task_id="empresas_create",
        )(urls=empresas_urls)
        
        empresas_insert = (
            BRONZE_ine_empresas_municipio_insert.override(
                task_id="empresas_insert",
                pool="default_pool", 
                max_active_tis_per_dag=1,
                pool_slots=default_pool_slots,
            )
            .partial(year="{{ params.year }}")
            .expand(url=empresas_filtered_urls)
        )

        empresas_skipped = EmptyOperator(
            task_id="empresas_skipped"
        )

        empresas_urls >> empresas_filtered_urls >> branch_task

        branch_task >> empresas_create >> empresas_insert
        branch_task >> empresas_skipped
        
        return SimpleNamespace(
            start=empresas_urls,
            insert=[empresas_insert, empresas_skipped],
        )
    
    return tg_empresas()


def create_tg_poblacion():
    """Factory function to create Poblacion TaskGroup."""
    group_id = "poblacion"
    
    @task_group(group_id=group_id)
    def tg_poblacion():
        """TaskGroup for INE Poblacion data ingestion."""
        poblacion_urls = BRONZE_ine_poblacion_municipio_urls.override(
            task_id="poblacion_urls",
        )(year="{{ params.year }}")

        poblacion_filtered_urls = BRONZE_ine_poblacion_municipio_filter_urls.override(
            task_id="poblacion_filter_urls",
        )(urls=poblacion_urls)

        @task.branch
        def check_poblacion_urls(filtered_urls: list[str], **context):
            """Check if there are URLs to process."""
            if len(filtered_urls) > 0:
                return f"{group_id}.poblacion_create"
            else:
                return f"{group_id}.poblacion_skipped"

        branch_task = check_poblacion_urls.override(
            task_id="check_urls"
        )(poblacion_filtered_urls)

        poblacion_create = BRONZE_ine_poblacion_municipio_create_table.override(
            task_id="poblacion_create",
        )(urls=poblacion_urls)
        
        poblacion_insert = (
            BRONZE_ine_poblacion_municipio_insert.override(
                task_id="poblacion_insert",
                pool="default_pool", 
                max_active_tis_per_dag=1,
                pool_slots=default_pool_slots,
            )
            .partial(year="{{ params.year }}")
            .expand(url=poblacion_filtered_urls)
        )

        poblacion_skipped = EmptyOperator(
            task_id="poblacion_skipped"
        )

        poblacion_urls >> poblacion_filtered_urls >> branch_task

        branch_task >> poblacion_create >> poblacion_insert
        branch_task >> poblacion_skipped
        
        return SimpleNamespace(
            start=poblacion_urls,
            insert=[poblacion_insert, poblacion_skipped],
        )
    
    return tg_poblacion()


def create_tg_renta():
    """Factory function to create Renta TaskGroup."""
    group_id = "renta"
    
    @task_group(group_id=group_id)
    def tg_renta():
        """TaskGroup for INE Renta data ingestion."""
        renta_urls = BRONZE_ine_renta_urls.override(
            task_id="renta_urls",
        )(year="{{ params.year }}")

        renta_filtered_urls = BRONZE_ine_renta_filter_urls.override(
            task_id="renta_filter_urls",
        )(urls=renta_urls)

        @task.branch
        def check_renta_urls(filtered_urls: list[str], **context):
            """Check if there are URLs to process."""
            if len(filtered_urls) > 0:
                return f"{group_id}.renta_create"
            else:
                return f"{group_id}.renta_skipped"

        branch_task = check_renta_urls.override(
            task_id="check_urls"
        )(renta_filtered_urls)

        renta_create = BRONZE_ine_renta_create_table.override(
            task_id="renta_create",
        )(urls=renta_urls)
        
        renta_insert = (
            BRONZE_ine_renta_insert.override(
                task_id="renta_insert",
                pool="default_pool", 
                max_active_tis_per_dag=1,
                pool_slots=default_pool_slots,
            )
            .partial(year="{{ params.year }}")
            .expand(url=renta_filtered_urls)
        )

        renta_skipped = EmptyOperator(
            task_id="renta_skipped"
        )

        renta_urls >> renta_filtered_urls >> branch_task

        branch_task >> renta_create >> renta_insert
        branch_task >> renta_skipped
        
        return SimpleNamespace(
            start=renta_urls,
            insert=[renta_insert, renta_skipped],
        )
    
    return tg_renta()


with DAG(
    dag_id="bronze_ine",
    start_date=datetime(2025, 12, 1),
    schedule=None,
    catchup=False,
    tags=["bronze", "ine", "data-ingestion"],
    params={
        "year": Param(type="string", description="Year for INE data (YYYY)"),
    },
    description="INE Bronze layer data ingestion (municipios, empresas, poblacion, renta)",
    default_args={
        "retries": 3,
        "retry_delay": timedelta(seconds=30),
    },
    max_active_tasks=4,  # Máximo de tareas concurrentes en el DAG (permite paralelismo general)
    max_active_runs=1,    # Solo 1 ejecución del DAG a la vez
) as dag:
    # Infrastructure setup
    infra = tg_infra()

    # TaskGroups
    municipios_group = create_tg_municipios()
    empresas_group = create_tg_empresas()
    poblacion_group = create_tg_poblacion()
    renta_group = create_tg_renta()

    # Infrastructure -> All INE TaskGroups
    infra.bucket >> [
        municipios_group.start,
        empresas_group.start,
        poblacion_group.start,
        renta_group.start,
    ]

    # Define asset for this DAG
    bronze_ine_asset = Dataset("bronze://ine/done")
    
    # Done marker - last task before DAG completion
    # This task produces the asset, triggering the silver DAG when it completes successfully
    done = EmptyOperator(
        task_id="done",
        outlets=[bronze_ine_asset],  # Publish asset when task completes successfully
        trigger_rule="none_failed"  # Wait for tasks that execute (skipped ones are ignored)
    )
    
    municipios_group.insert[0] >> done  # actual insert (if URLs exist)
    municipios_group.insert[1] >> done  # skipped insert (if no URLs)
    empresas_group.insert[0] >> done
    empresas_group.insert[1] >> done
    poblacion_group.insert[0] >> done
    poblacion_group.insert[1] >> done
    renta_group.insert[0] >> done
    renta_group.insert[1] >> done

