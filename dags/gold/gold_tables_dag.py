"""
DAG for Gold layer table creation.

This DAG creates the gold layer tables for business questions analysis.
Uses incremental processing: only processes dates that are not yet in the gold tables.
Tables with heavy SQL computations use Cloud Run when available for 
better performance.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.datasets import Dataset
from airflow.models import Param
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import TaskGroup

from gold.tasks import (
    GOLD_typical_day_create_table,
    GOLD_typical_day_get_date_batches,
    GOLD_typical_day_process_batch,
    GOLD_gravity_model,
    GOLD_functional_type
)

with DAG(
    dag_id="gold_tables",
    start_date=datetime(2025, 12, 1),
    schedule=[Dataset("silver://done")],  # Wait for Silver to complete
    catchup=False,
    tags=["gold", "create-tables"],
    description="Gold layer tables creation - Automatically triggered when Silver completes. Uses incremental processing to only process new dates",
    params={
        "batch_size": Param(
            type="integer",
            default=2,
            description="Número de fechas a procesar por batch en gold_typical_day_od_hourly (default: 2)"
        ),
    },
    default_args={
        "retries": 3,
        "retry_delay": timedelta(seconds=30),
    },
    max_active_tasks=10,
) as dag:
    
    start = EmptyOperator(task_id="start")
    
    gold_tables_asset = Dataset("gold://tables/done")
    done = EmptyOperator(
        task_id="done",
        outlets=[gold_tables_asset]  # Publish dataset when Gold Tables completes
    )

    with TaskGroup(group_id="typical_day_batches") as typ_day_group:
        typ_day_create_table = GOLD_typical_day_create_table.override(
            task_id="create_table"
        )()
        
        typ_day_date_batches = GOLD_typical_day_get_date_batches.override(
            task_id="get_batches"
        )(batch_size="{{ params.batch_size }}")
        
        typ_day_process_batches = (
            GOLD_typical_day_process_batch.override(
                task_id="process_batch"
            )
            .expand(date_batch=typ_day_date_batches)
        )
        
        typ_day_create_table >> typ_day_date_batches >> typ_day_process_batches
    
    grav_model = GOLD_gravity_model.override(task_id="gravity_model")()

    func_type = GOLD_functional_type.override(task_id="functional_type")()

    start >> [typ_day_group, grav_model, func_type] >> done
