"""
DAG for Silver layer data transformation.
Waits for all Bronze DAGs to complete before executing transformations.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.datasets import Dataset
from airflow.models import Variable
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup

from silver.mitma import (
    SILVER_mitma_zonification,
    SILVER_mitma_overnight_stay,
    SILVER_mitma_people_day,
    SILVER_mitma_distances
)
from silver.mitma.mitma_od import (
    SILVER_mitma_od_get_date_batches,
    SILVER_mitma_od_create_table,
    SILVER_mitma_od_process_batch,
    SILVER_mitma_od_check_batches
)
from silver.ine import ine_all
from silver.misc import (
    SILVER_verify_mapping_coverage
)
from silver.misc.quality import (
    SILVER_od_quality_create_table,
    SILVER_od_quality_get_date_batches,
    SILVER_od_quality_process_batch
)
from silver.mitma_ine_mapping import SILVER_mitma_ine_mapping

OD_POOL_NAME = "od_pool"
OD_POOL_SLOTS = 15

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
    mitma_overnights = SILVER_mitma_overnight_stay.override(task_id="overnights")()
    mitma_people = SILVER_mitma_people_day.override(task_id="people_day")()
    
    # MITMA OD con procesamiento por batches - TaskGroup
    with TaskGroup(group_id="mitma_od_batches") as od_group:
        od_create_table = SILVER_mitma_od_create_table.override(
            task_id="create_table"
        )()
        
        od_date_batches = SILVER_mitma_od_get_date_batches.override(
            task_id="get_batches"
        )(batch_size=7)
        
        # Branch task que determina si hay batches o no
        od_check_batches = SILVER_mitma_od_check_batches.override(
            task_id="check_batches"
        )(date_batches=od_date_batches)
        
        # process_batches necesita el resultado directo de od_date_batches para el expand
        # Se conecta directamente a od_date_batches, pero el branch controla si se ejecuta
        od_process_batches = (
            SILVER_mitma_od_process_batch.override(
                task_id="process_batch",
                pool=OD_POOL_NAME,
                pool_slots=OD_POOL_SLOTS,
            )
            .expand(date_batch=od_date_batches)
        )
        
        # Tarea que se ejecuta cuando no hay batches
        od_batches_skipped = EmptyOperator(task_id="batches_skipped")
        
        # Tarea que se ejecuta cuando termina cualquiera de los dos caminos (batches_skipped o process_batches)
        od_done = EmptyOperator(task_id="done")
        
        # Dependencias dentro del TaskGroup
        # 1. Crear tabla
        # 2. Obtener batches
        # 3. Branch: si length == 0 -> batches_skipped, si length > 0 -> process_batches (dynamic task mapping)
        # 4. done se ejecuta cuando termina cualquiera de los dos caminos
        od_create_table >> od_date_batches
        od_date_batches >> od_check_batches  # Branch para decidir el flujo
        # El branch decide: si hay batches -> retorna None (todas las downstream se ejecutan)
        # Si no hay batches -> retorna 'batches_skipped'
        od_check_batches >> od_batches_skipped  # Se ejecuta solo si no hay batches (branch retorna 'batches_skipped')
        od_check_batches >> od_process_batches  # Se ejecuta solo si hay batches (branch retorna None)
        # process_batches también necesita el resultado directo de od_date_batches para el expand (XCom)
        od_date_batches >> od_process_batches  # Dynamic task mapping (necesita el XCom de od_date_batches)
        # done se ejecuta cuando termina cualquiera de los dos caminos
        # Cuando hay batches: od_process_batches se ejecuta -> od_done se ejecuta
        # Cuando no hay batches: od_batches_skipped se ejecuta -> od_done se ejecuta
        # Hacemos que od_done dependa solo de od_batches_skipped primero (para cuando no hay batches),
        # y luego también de od_process_batches cuando existe (para cuando hay batches)
        # Usamos trigger_rule='none_failed' similar a bronze_mitma_dag
        od_done.trigger_rule = 'none_failed'
        od_batches_skipped >> od_done
        od_process_batches >> od_done  # Esta dependencia solo se aplica si od_process_batches existe
    
    mitma_distances = SILVER_mitma_distances.override(task_id="distances")()

    mapping = SILVER_mitma_ine_mapping.override(task_id="mitma_ine_mapping")()
    
    coverage_check = SILVER_verify_mapping_coverage.override(task_id="verify_mapping_coverage")()

    ine_group = ine_all()

    # OD Quality con procesamiento por batches - TaskGroup
    with TaskGroup(group_id="od_quality_batches") as od_quality_group:
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
        
        # Dependencias dentro del TaskGroup
        # Primero crear tablas, luego obtener batches, luego procesar
        od_quality_create_table >> od_quality_date_batches >> od_quality_process_batches

    # Start and done markers
    start = EmptyOperator(task_id="start")
    done = EmptyOperator(task_id="done")

    # Mapping es prioritario, se ejecuta después de start
    start >> mapping
    
    # ============ SUBGRUPO MITMA ============
    # Zonification depende del mapping (mitma_ine_mapping)
    mapping >> mitma_zonif
    
    # Otras tareas MITMA se ejecutan en paralelo después de start
    # Start is automatically triggered when all bronze assets are ready
    start >> [mitma_overnights, mitma_people, od_group]
    
    # Distances depende solo de zonification
    mitma_zonif >> mitma_distances
    
    # ============ SUBGRUPO INE ============
    # Coverage check depende del mapping
    mapping >> coverage_check
    
    # El branching de coverage_check decide:
    # - Si coverage es completa: salta todo y va directo a done
    # - Si coverage es incompleta: ejecuta INE tasks
    # El branching se conecta automáticamente según lo que retorne
    coverage_check >> ine_group.business
    coverage_check >> ine_group.population
    coverage_check >> ine_group.income
    coverage_check >> done
    
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

