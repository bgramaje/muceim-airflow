"""
DAG for Gold layer Question 1 report generation (Typical Day).

This DAG generates visualizations and reports for typical day analysis.
Reports are generated locally as they require pandas/matplotlib/plotly processing
and are uploaded to S3.
"""

from datetime import datetime, timedelta
from airflow.models import Param
from airflow.sdk import Variable
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.sdk import task, TaskGroup
from airflow import DAG
from airflow.providers.standard.operators.empty import EmptyOperator

from gold.tasks import (
    GOLD_generate_typical_day_map,
    GOLD_generate_top_origins,
    GOLD_generate_hourly_distribution,
    GOLD_verify_s3_connection,
)
from gold.utils import build_dir_name, get_params
from utils.dag import validate_dates
from utils.logger import get_logger


@task
def generate_directory(**context):
    """
    Generate a unique directory identifier for the report and save info file.
    
    Parameters are extracted from context['params'].
    
    Returns:
    - str: Directory name in format DDMMYYYY_6digitsUUID
    """
    params = context.get('params', {})
    start_date = params.get('start_date')
    end_date = params.get('end_date')
    polygon_wkt = params.get('polygon_wkt')
    
    execution_date = context.get('execution_date') or context.get('ds')
    directory_name = build_dir_name(execution_date=execution_date)

    content = f"Start date: {start_date}\nEnd date: {end_date}\n\n{polygon_wkt}"
    bucket_name = Variable.get('RUSTFS_BUCKET', default='mitma')
    
    s3 = S3Hook(aws_conn_id="rustfs_s3_conn")
    s3_key = f"gold/question1/{directory_name}/info.txt"
    
    s3.load_string(
        string_data=content,
        key=s3_key,
        bucket_name=bucket_name,
        replace=True
    )

    logger = get_logger(__name__, context)
    logger.info(f"Uploaded to s3://{bucket_name}/{s3_key}")
    return directory_name


with DAG(
    dag_id="gold_report_question_1",
    start_date=datetime(2025, 12, 1),
    schedule=None,  # Manual execution only
    catchup=False,
    tags=["gold", "report", "question_1", "typical_day"],
    description="Gold layer Question 1 report generation - Typical Day analysis. Manual execution only with parameters",
    params=get_params(),
    default_args={
        "retries": 3,
        "retry_delay": timedelta(seconds=30),
    },
    max_active_tasks=3,
    max_active_runs=1,
) as dag:

    start = EmptyOperator(task_id="start")
    done = EmptyOperator(task_id="done")

    with TaskGroup(group_id="infra") as infra_group:
        validate_dates_task = validate_dates.override(task_id="validate_dates")(
            start_date="{{ params.start_date }}",
            end_date="{{ params.end_date }}"
        )

        save_id = generate_directory.override(task_id="create_directory")()

        verify_s3 = GOLD_verify_s3_connection.override(task_id="verify_s3_connection")()

        validate_dates_task >> save_id >> verify_s3

    bucket_name = Variable.get('RUSTFS_BUCKET', default='mitma')
    
    typ_day_map = GOLD_generate_typical_day_map.override(task_id="typical_day_map")(
        save_id=save_id,
        start_date="{{ params.start_date }}",
        end_date="{{ params.end_date }}",
        polygon_wkt="{{ params.polygon_wkt }}",
        bucket_name=bucket_name
    )
    typ_day_top = GOLD_generate_top_origins.override(task_id="top_origins")(
        save_id=save_id,
        start_date="{{ params.start_date }}",
        end_date="{{ params.end_date }}",
        polygon_wkt="{{ params.polygon_wkt }}",
        bucket_name=bucket_name
    )
    typ_day_hourly = GOLD_generate_hourly_distribution.override(task_id="hourly_distribution")(
        save_id=save_id,
        start_date="{{ params.start_date }}",
        end_date="{{ params.end_date }}",
        polygon_wkt="{{ params.polygon_wkt }}",
        bucket_name=bucket_name
    )

    start >> infra_group >> [typ_day_map, typ_day_top, typ_day_hourly] >> done

