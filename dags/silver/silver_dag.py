"""
DAG for Silver layer data transformation.
Waits for all Bronze DAGs to complete before executing transformations.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.datasets import Dataset
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import TaskGroup

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


def create_mitma_od_batches_group(dag: DAG):
    """Crea el TaskGroup para procesamiento de MITMA OD por batches."""
    with TaskGroup(group_id="mitma_od_batches", dag=dag) as od_group:
        od_create_table = SILVER_mitma_od_create_table.override(
            task_id="create_table"
        )()
        
        od_date_batches = SILVER_mitma_od_get_date_batches.override(
            task_id="get_batches"
        )(batch_size=7)
        
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


def create_people_day_group(dag: DAG):
    """Crea el TaskGroup para procesamiento de People Day."""
    with TaskGroup(group_id="people_day", dag=dag) as people_day_group:
        create_table = SILVER_mitma_people_day_create_table.override(
            task_id="create_table"
        )()
        
        insert_data = SILVER_mitma_people_day_insert.override(
            task_id="insert_data"
        )()
        
        create_table >> insert_data
    
    return people_day_group, insert_data


def create_overnight_stay_group(dag: DAG):
    """Crea el TaskGroup para procesamiento de Overnight Stay."""
    with TaskGroup(group_id="overnight_stay", dag=dag) as overnight_group:
        create_table = SILVER_mitma_overnight_stay_create_table.override(
            task_id="create_table"
        )()
        
        insert_data = SILVER_mitma_overnight_stay_insert.override(
            task_id="insert_data"
        )()
        
        create_table >> insert_data
    
    return overnight_group, insert_data


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
    default_args={
        "retries": 3,
        "retry_delay": timedelta(seconds=30),
    }
) as dag:

    # Silver layer tasks
    mitma_zonif = SILVER_mitma_zonification.override(task_id="zonification")()
    
    # MITMA People Day con TaskGroup (create table + insert)
    people_day_group, people_day_done = create_people_day_group(dag)
    
    # MITMA Overnight Stay con TaskGroup (create table + insert)
    overnight_group, overnight_done = create_overnight_stay_group(dag)
    
    # MITMA OD con procesamiento por batches - TaskGroup
    od_group, od_done = create_mitma_od_batches_group(dag)
    
    mitma_distances = SILVER_mitma_distances.override(task_id="distances")()

    mapping = SILVER_mitma_ine_mapping.override(task_id="mitma_ine_mapping")()

    # INE processing TaskGroup (incluye coverage_check dentro del TaskGroup)
    ine_group = ine_all()

    # OD Quality con procesamiento por batches - TaskGroup
    od_quality_group, od_quality_create_table = create_od_quality_batches_group(dag)

    start = EmptyOperator(task_id="start")
    done = EmptyOperator(task_id="done")

    start >> mapping
    
    # ============ SUBGRUPO MITMA ============
    # Zonification depende del mapping (mitma_ine_mapping)
    mapping >> mitma_zonif
    
    # Otras tareas MITMA se ejecutan en paralelo después de start
    # Start is automatically triggered when all bronze assets are ready
    start >> [overnight_group, people_day_group, od_group]
    
    # Distances depende solo de zonification
    mitma_zonif >> mitma_distances
    
    # ============ SUBGRUPO INE ============
    # Coverage check depende del mapping (conexión externa al TaskGroup)
    mapping >> ine_group.coverage_check
    
    # El branching de coverage_check decide:
    # - Si coverage es completa: salta todo y va directo a done
    # - Si coverage es incompleta: ejecuta INE tasks
    # (Las dependencias internas coverage_check -> business/population/income están dentro del TaskGroup)
    ine_group.coverage_check >> done
    
    # INE all depende de mitma_zonification + todos los outputs INE
    # (Las dependencias internas del TaskGroup ya manejan business/population/income -> ine_all)
    # Necesitamos añadir la dependencia externa de mitma_zonif
    mitma_zonif >> ine_group.ine_all
    
    # od_quality_create_table (primera tarea del TaskGroup) depende de:
    # - od_done (que se ejecuta cuando termina process_batches o batches_skipped)
    # - ine_all (para silver_ine_all)
    # od_done siempre existe y se ejecuta cuando termina cualquiera de los dos caminos
    [od_done, ine_group.ine_all] >> od_quality_create_table
    od_quality_group >> done

