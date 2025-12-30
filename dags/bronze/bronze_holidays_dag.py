"""
DAG for Spanish Holidays Bronze layer data ingestion.
Loads Spanish nationwide public holidays into DuckDB.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.datasets import Dataset
from airflow.models import Param
from airflow.providers.standard.operators.empty import EmptyOperator

# Import infrastructure setup TaskGroup
from misc.infra import tg_infra

# Import Bronze Holidays tasks
from bronze.tasks.holidays.spanish_holidays import BRONZE_load_spanish_holidays


with DAG(
    dag_id="bronze_holidays",
    start_date=datetime(2025, 12, 1),
    schedule=None,
    catchup=False,
    tags=["bronze", "holidays", "data-ingestion"],
    params={
        "year": Param(type="integer", default=2023, description="Year for holidays data"),
    },
    description="Spanish Holidays Bronze layer data ingestion",
    default_args={
        "retries": 3,
        "retry_delay": timedelta(seconds=30),
    }
) as dag:
    infra = tg_infra()

    holidays = BRONZE_load_spanish_holidays.override(
        task_id="spanish_holidays"
    )(year="{{ params.year }}")

    infra.bucket >> holidays

    done = EmptyOperator(
        task_id="done",
        outlets=[Dataset("bronze://holidays/done")]
    )

    holidays >> done

