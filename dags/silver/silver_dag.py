"""
DAG for Silver layer data transformation.
Waits for all Bronze DAGs to complete before executing transformations.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.datasets import Dataset
from airflow.models import Param
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import TaskGroup, task

from silver.mitma import (
    SILVER_mitma_zonification,
    SILVER_mitma_overnight_stay_create_table,
    SILVER_mitma_overnight_stay_insert,
    SILVER_mitma_people_day_create_table,
    SILVER_mitma_people_day_insert,
    SILVER_mitma_distances
)
from silver.mitma.mitma_od import (
    SILVER_mitma_od_get_date_batches,
    SILVER_mitma_od_create_table,
    SILVER_mitma_od_process_batch,
    SILVER_mitma_od_check_batches
)
from silver.ine import ine_all
from silver.misc.quality import (
    SILVER_od_quality_create_table,
    SILVER_od_quality_get_date_batches,
    SILVER_od_quality_process_batch
)
from silver.mitma_ine_mapping import SILVER_mitma_ine_mapping

OD_POOL_NAME = "od_pool"
OD_POOL_SLOTS = 15


def create_mitma_od_batches_group(dag: DAG, start_date: str = None, end_date: str = None, batch_size: int = 7):
    """Crea el TaskGroup para procesamiento de MITMA OD por batches."""
    with TaskGroup(group_id="mitma_od_batches", dag=dag) as od_group:
        od_create_table = SILVER_mitma_od_create_table.override(
            task_id="create_table"
        )()
        
        od_date_batches = SILVER_mitma_od_get_date_batches.override(
            task_id="get_batches"
        )(
            batch_size=batch_size,
            start_date=start_date,
            end_date=end_date,
        )
        
        od_check_batches = SILVER_mitma_od_check_batches.override(
            task_id="check_batches"
        )(date_batches=od_date_batches)
        
        od_process_batches = (
            SILVER_mitma_od_process_batch.override(
                task_id="process_batch",
                pool=OD_POOL_NAME,
                pool_slots=OD_POOL_SLOTS,
            )
            .expand(date_batch=od_date_batches)
        )
        
        od_batches_skipped = EmptyOperator(task_id="batches_skipped")
        od_done = EmptyOperator(task_id="done")
        
        od_create_table >> od_date_batches
        od_date_batches >> od_check_batches
        od_check_batches >> od_batches_skipped
        od_check_batches >> od_process_batches
        od_date_batches >> od_process_batches
        
        od_done.trigger_rule = 'none_failed'
        od_batches_skipped >> od_done
        od_process_batches >> od_done
    
    return od_group, od_done


def create_od_quality_batches_group(dag: DAG):
    """Crea el TaskGroup para procesamiento de OD Quality por batches."""
    with TaskGroup(group_id="od_quality_batches", dag=dag) as od_quality_group:
        od_quality_create_table = SILVER_od_quality_create_table.override(
            task_id="create_table"
        )()
        
        od_quality_date_batches = SILVER_od_quality_get_date_batches.override(
            task_id="get_batches"
        )(batch_size=7)
        
        od_quality_process_batches = (
            SILVER_od_quality_process_batch.override(
                task_id="process_batch"
            )
            .expand(date_batch=od_quality_date_batches)
        )
        
        od_quality_create_table >> od_quality_date_batches >> od_quality_process_batches
    
    return od_quality_group, od_quality_create_table


def create_people_day_group(dag: DAG, start_date: str = None, end_date: str = None):
    """Crea el TaskGroup para procesamiento de People Day."""
    with TaskGroup(group_id="people_day", dag=dag) as people_day_group:
        
        @task.branch
        def check_people_day_enabled(**context):
            """Check if people_day processing is enabled via params."""
            enabled = context['params'].get('enable_people_day', False)
            if enabled:
                print("[TASK] People Day processing is enabled")
                return "people_day.create_table"
            else:
                print("[TASK] People Day processing is disabled")
                return "people_day.skipped"
        
        check_enabled = check_people_day_enabled.override(
            task_id="check_enabled"
        )()
        
        create_table = SILVER_mitma_people_day_create_table.override(
            task_id="create_table"
        )()
        
        insert_data = SILVER_mitma_people_day_insert.override(
            task_id="insert_data"
        )(start_date=start_date, end_date=end_date)
        
        skipped = EmptyOperator(task_id="skipped")
        done = EmptyOperator(task_id="done", trigger_rule="none_failed")
        
        check_enabled >> create_table >> insert_data >> done
        check_enabled >> skipped >> done
    
    return people_day_group, done


def create_overnight_stay_group(dag: DAG, start_date: str = None, end_date: str = None):
    """Crea el TaskGroup para procesamiento de Overnight Stay."""
    with TaskGroup(group_id="overnight_stay", dag=dag) as overnight_group:
        
        @task.branch
        def check_overnight_enabled(**context):
            """Check if overnight processing is enabled via params."""
            enabled = context['params'].get('enable_overnight', False)
            if enabled:
                print("[TASK] Overnight Stay processing is enabled")
                return "overnight_stay.create_table"
            else:
                print("[TASK] Overnight Stay processing is disabled")
                return "overnight_stay.skipped"
        
        check_enabled = check_overnight_enabled.override(
            task_id="check_enabled"
        )()
        
        create_table = SILVER_mitma_overnight_stay_create_table.override(
            task_id="create_table"
        )()
        
        insert_data = SILVER_mitma_overnight_stay_insert.override(
            task_id="insert_data"
        )(start_date=start_date, end_date=end_date)
        
        skipped = EmptyOperator(task_id="skipped")
        done = EmptyOperator(task_id="done", trigger_rule="none_failed")
        
        check_enabled >> create_table >> insert_data >> done
        check_enabled >> skipped >> done
    
    return overnight_group, done


with DAG(
    dag_id="silver",
    start_date=datetime(2025, 12, 1),
    schedule=[
        Dataset("bronze://mitma/done"), 
        Dataset("bronze://ine/done"), 
        Dataset("bronze://holidays/done")
    ],
    catchup=False,
    tags=["silver", "data-transformation"],
    description="Silver layer data transformation - automatically triggered when all Bronze DAGs complete",
    params={
        "start": Param(
            type=["string", "null"], 
            default=None,
            schema={"type": ["string", "null"], "format": "date"}, 
            description="Fecha inicio para filtrar datos MITMA (formato: YYYY-MM-DD). Si no se especifica, procesa todas las fechas no procesadas."
        ),
        "end": Param(
            type=["string", "null"], 
            default=None,
            schema={"type": ["string", "null"], "format": "date"}, 
            description="Fecha fin para filtrar datos MITMA (formato: YYYY-MM-DD). Si no se especifica, procesa todas las fechas no procesadas."
        ),
        "enable_people_day": Param(
            type="boolean", 
            default=False, 
            description="Habilitar procesamiento de People Day a Silver"
        ),
        "enable_overnight": Param(
            type="boolean", 
            default=False, 
            description="Habilitar procesamiento de Overnight Stay a Silver"
        ),
        "batch_size_od": Param(
            type="integer",
            default=7,
            description="Número de fechas a procesar por batch en MITMA OD (default: 7)"
        ),
    },
    default_args={
        "retries": 3,
        "retry_delay": timedelta(seconds=30),
    }
) as dag:

    mitma_zonif = SILVER_mitma_zonification.override(task_id="zonification")()
    
    people_day_group, people_day_done = create_people_day_group(
        dag, 
        start_date="{{ params.start }}", 
        end_date="{{ params.end }}"
    )
    
    overnight_group, overnight_done = create_overnight_stay_group(
        dag, 
        start_date="{{ params.start }}", 
        end_date="{{ params.end }}"
    )
    
    od_group, od_done = create_mitma_od_batches_group(
        dag, 
        start_date="{{ params.start }}",
        batch_size="{{ params.batch_size_od }}", 
        end_date="{{ params.end }}"
    )
    
    mitma_distances = SILVER_mitma_distances.override(task_id="distances")()

    mapping = SILVER_mitma_ine_mapping.override(task_id="mitma_ine_mapping")()

    ine_group = ine_all()

    od_quality_group, od_quality_create_table = create_od_quality_batches_group(dag)

    start = EmptyOperator(task_id="start")
    done = EmptyOperator(task_id="done")

    start >> mapping
    mapping >> mitma_zonif
    start >> [overnight_group, people_day_group, od_group]
    mitma_zonif >> mitma_distances
    mapping >> ine_group.coverage_check
    ine_group.coverage_check >> done
    mitma_zonif >> ine_group.ine_all
    
    # od_quality_create_table (primera tarea del TaskGroup) depende de:
    # - od_done (que se ejecuta cuando termina process_batches o batches_skipped)
    # - ine_all (para silver_ine_all)
    # od_done siempre existe y se ejecuta cuando termina cualquiera de los dos caminos
    [od_done, ine_group.ine_all] >> od_quality_create_table
    od_quality_group >> done

