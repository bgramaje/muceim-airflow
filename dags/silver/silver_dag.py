"""
DAG for Silver layer data transformation.
Waits for all Bronze DAGs to complete before executing transformations.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.datasets import Dataset
from airflow.models import Variable
from airflow.providers.standard.operators.empty import EmptyOperator

from silver.mitma import (
    SILVER_mitma_zonification,
    SILVER_mitma_overnight_stay,
    SILVER_mitma_people_day,
    SILVER_mitma_od,
    SILVER_mitma_distances
)
from silver.ine import ine_all
from silver.misc import (
    SILVER_verify_mapping_coverage,
    SILVER_od_quality
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
    mitma_od = SILVER_mitma_od.override(
        task_id="od", 
        pool=OD_POOL_NAME,
        pool_slots=OD_POOL_SLOTS,
    )()
    mitma_distances = SILVER_mitma_distances.override(task_id="distances")()

    mapping = SILVER_mitma_ine_mapping.override(task_id="mitma_ine_mapping")()
    
    # Verificación de cobertura después del mapping
    coverage_check = SILVER_verify_mapping_coverage.override(task_id="verify_mapping_coverage")()

    # INE TaskGroup
    ine_group = ine_all()

    od_quality = SILVER_od_quality.override(task_id="od_quality")()

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
    start >> [mitma_overnights, mitma_people, mitma_od]
    
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
    
    [ine_group.cleanup, mitma_od] >> od_quality
    od_quality >> done

