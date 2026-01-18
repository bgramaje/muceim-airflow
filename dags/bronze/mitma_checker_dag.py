"""
DAG for checking MITMA OD URLs and triggering bronze_mitma if there are URLs to process.
Uses bronze.utils functions to get OD URLs and check if processing is needed.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sdk import task
from utils.logger import get_logger


@task
def check_od_urls(zone_type: str = "municipios", **context):
    """
    Obtiene las URLs de OD (Origin-Destination) y verifica si hay URLs para procesar.
    Verifica URLs para la fecha de ejecución del DAG.
    
    Returns:
        dict: Diccionario con 'has_urls' (bool) y 'urls' (lista)
    """
    from bronze.utils import get_mitma_urls
    
    ds = context.get('ds')
    fecha_str = ds.replace('-', '') if ds else None
    
    if not fecha_str:
        raise ValueError("No date found")
    
    urls = get_mitma_urls('od', zone_type, [fecha_str])
    
    has_urls = len(urls) > 0
    
    logger = get_logger(__name__, context)
    logger.info(f"OD URLs check for {zone_type} on {ds}:")
    logger.info(f"  URLs found: {len(urls)}")
   
    return {
        'has_urls': has_urls,
        'urls': urls,
        'zone_type': zone_type,
        'fecha': ds
    }


@task.branch
def decide_action(check_result, **context):
    """
    Decide si ejecutar bronze_mitma o saltar.
    
    Returns:
        str: Task ID a ejecutar ('trigger_bronze_mitma' o 'skip')
    """
    has_urls = check_result['has_urls']
    
    return "trigger_bronze_mitma" if has_urls else "skip"


with DAG(
    dag_id="bronze_mitma_checker",
    start_date=datetime(2025, 12, 1),
    schedule="0 0 * * *",  # Diario a las 00:00
    catchup=False,
    tags=["bronze", "mitma", "sensor", "od"],
    description="Daily checker for MITMA OD URLs. Triggers bronze_mitma if URLs are available",
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
    },
    max_active_runs=1,
) as dag:
    check_urls = check_od_urls(zone_type="municipios")
    decide = decide_action(check_urls)
    
    trigger_bronze = TriggerDagRunOperator(
        task_id="trigger_bronze_mitma",
        trigger_dag_id="bronze_mitma",
        conf={
            "start": "{{ ds }}",  # Fecha de ejecución del checker
            "end": "{{ ds }}",    # Fecha de ejecución del checker
            "triggered_by_checker": True,  # Indica que viene del checker
        },
        wait_for_completion=False,
    )
    
    skip = EmptyOperator(task_id="skip")
    
    check_urls >> decide
    decide >> [trigger_bronze, skip]
