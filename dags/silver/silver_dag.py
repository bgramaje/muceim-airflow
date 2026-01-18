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

from silver.tasks.mitma import (
    SILVER_mitma_zonification,
    SILVER_mitma_distances
)
from silver.tasks.mitma.mitma_od import (
    SILVER_mitma_od_get_date_batches,
    SILVER_mitma_od_create_table,
    SILVER_mitma_od_process_batch,
    SILVER_mitma_od_check_batches
)
from silver.tasks.ine import (
    SILVER_ine_empresas,
    SILVER_ine_poblacion_municipio,
    SILVER_ine_renta,
    SILVER_ine_all,
    CLEANUP_intermediate_ine_tables,
)
from silver.tasks.misc import SILVER_verify_mapping_coverage
from silver.tasks.misc.quality import (
    SILVER_od_quality_create_table,
    SILVER_od_quality_get_date_batches,
    SILVER_od_quality_process_batch
)
from silver.tasks.mitma.mitma_ine_mapping import SILVER_mitma_ine_mapping

OD_POOL_NAME = "od_pool"
OD_POOL_SLOTS = 15


def create_mitma_od_batches_group(dag: DAG, start_date: str = None, end_date: str = None, batch_size: int = 2):
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
        )(batch_size=2)
        
        od_quality_process_batches = (
            SILVER_od_quality_process_batch.override(
                task_id="process_batch"
            )
            .expand(date_batch=od_quality_date_batches)
        )
        
        od_quality_create_table >> od_quality_date_batches >> od_quality_process_batches
    
    return od_quality_group, od_quality_create_table


def create_zonification_group(dag: DAG):
    """Crea el TaskGroup para procesamiento de Zonificación y Distancias."""
    with TaskGroup(group_id="zonification_distances", dag=dag) as zonif_group:
        
        @task.branch
        def check_zonification_enabled(**context):
            """Check if zonification processing is enabled via params."""
            from utils.logger import get_logger
            logger = get_logger(__name__, context)
            enabled = context['params'].get('process_zonification', True)
            if enabled:
                logger.info("Zonification processing is enabled")
                return "zonification_distances.zonification"
            else:
                logger.info("Zonification processing is disabled")
                return "zonification_distances.skipped"
        
        check_enabled = check_zonification_enabled.override(
            task_id="check_enabled"
        )()
        
        mitma_zonif = SILVER_mitma_zonification.override(
            task_id="zonification"
        )()
        
        mitma_distances = SILVER_mitma_distances.override(
            task_id="distances"
        )()
        
        skipped = EmptyOperator(task_id="skipped")
        done = EmptyOperator(task_id="done", trigger_rule="none_failed")
        
        check_enabled >> mitma_zonif >> mitma_distances >> done
        check_enabled >> skipped >> done
    
    return zonif_group, done


def create_ine_group_with_branch(dag: DAG):
    """Crea el TaskGroup para procesamiento de INE con branch para skip."""
    with TaskGroup(group_id="ine_processing", dag=dag) as ine_group:
        
        @task.branch
        def check_ine_enabled(**context):
            """Check if INE processing is enabled via params."""
            from utils.logger import get_logger
            logger = get_logger(__name__, context)
            enabled = context['params'].get('process_ine', True)
            if enabled:
                logger.info("INE processing is enabled")
                return "ine_processing.verify_mapping_coverage"
            else:
                logger.info("INE processing is disabled")
                return "ine_processing.skipped"
        
        check_enabled = check_ine_enabled.override(
            task_id="check_enabled"
        )()
        
        coverage_check = SILVER_verify_mapping_coverage.override(
            task_id="verify_mapping_coverage"
        )()
        
        ine_emp = SILVER_ine_empresas.override(task_id="business")()
        ine_pob = SILVER_ine_poblacion_municipio.override(task_id="population")()
        ine_renta = SILVER_ine_renta.override(task_id="income")()
        
        ine_all_task = SILVER_ine_all.override(task_id="ine_all")()
        cleanup_ine = CLEANUP_intermediate_ine_tables.override(task_id="cleanup_intermediate_ine_tables")()
        
        skipped = EmptyOperator(task_id="skipped")
        done = EmptyOperator(task_id="done", trigger_rule="none_failed")
        
        # El branching de coverage_check decide:
        # - Si coverage es completa: salta todo y va directo a done
        # - Si coverage es incompleta: ejecuta INE tasks
        check_enabled >> coverage_check
        coverage_check >> ine_emp
        coverage_check >> ine_pob
        coverage_check >> ine_renta
        
        # Business, population, and income can run in parallel
        # ine_all depends on all three completing
        # cleanup runs after ine_all, and done runs after cleanup
        [ine_emp, ine_pob, ine_renta] >> ine_all_task >> cleanup_ine >> done
        check_enabled >> skipped >> done
    
    return ine_group, done, ine_all_task


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
        "batch_size_od": Param(
            type="integer",
            default=2,
            description="Número de fechas a procesar por batch en MITMA OD (default: 2)"
        ),
        "process_zonification": Param(
            type="boolean",
            default=True,
            description="Habilitar procesamiento de Zonificación y Distancias"
        ),
        "process_ine": Param(
            type="boolean",
            default=True,
            description="Habilitar procesamiento de datos INE"
        ),
    },
    default_args={
        "retries": 3,
        "retry_delay": timedelta(seconds=30),
    }
) as dag:

    mapping = SILVER_mitma_ine_mapping.override(task_id="mitma_ine_mapping")()

    od_group, od_done = create_mitma_od_batches_group(
        dag, 
        start_date="{{ params.start }}",
        batch_size="{{ params.batch_size_od }}", 
        end_date="{{ params.end }}"
    )

    zonif_group, zonif_done = create_zonification_group(dag)

    ine_group, ine_done, ine_all_task = create_ine_group_with_branch(dag)

    od_quality_group, od_quality_create_table = create_od_quality_batches_group(dag)

    start = EmptyOperator(task_id="start")
    
    silver_asset = Dataset("silver://done")
    done = EmptyOperator(
        task_id="done",
        outlets=[silver_asset]  # Publish dataset when Silver completes
    )

    start >> mapping
    start >> od_group
    mapping >> zonif_group
    
    # INE group depende del mapping
    mapping >> ine_group
    
    # Zonificación puede alimentar a INE (conectamos directamente a ine_all_task, el branch maneja el skip)
    # Si INE está disabled, el branch redirige a skipped y las tareas no se ejecutan
    zonif_done >> ine_all_task
    
    # OD Quality depende de OD y del done de INE (que maneja el skip automáticamente)
    [od_done, ine_done] >> od_quality_create_table
    
    # Todo debe terminar en done
    od_quality_group >> done

