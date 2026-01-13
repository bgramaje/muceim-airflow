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
import uuid

from gold.tasks import (
    GOLD_generate_typical_day_map,
    GOLD_generate_top_origins,
    GOLD_generate_hourly_distribution,
    GOLD_verify_s3_connection,
)
from utils.dag import validate_dates


@task
def generate_directory(start_date: str = None, end_date: str = None, polygon_wkt: str = None):
    """
    Generate a unique directory identifier for the report and save info file.
    
    Parameters:
    - start_date: Start date for the report
    - end_date: End date for the report
    - polygon_wkt: WKT polygon defining the area of interest
    
    Returns:
    - str: UUID for the report directory
    """
    # Generate UUID for this report
    report_uuid = str(uuid.uuid4())

    content = f"Start date: {start_date}\nEnd date: {end_date}\n\n{polygon_wkt}"
    bucket_name = Variable.get('RUSTFS_BUCKET', default='mitma')
    s3 = S3Hook(aws_conn_id="rustfs_s3_conn")
    s3_key = f"gold/question1/{report_uuid}/info.txt"
    s3.load_string(
        string_data=content,
        key=s3_key,
        bucket_name=bucket_name,
        replace=True
    )
    print(f"[SUCCESS] Uploaded to s3://{bucket_name}/{s3_key}")
    return report_uuid


with DAG(
    dag_id="gold_report_question_1",
    start_date=datetime(2025, 12, 1),
    catchup=False,
    tags=["gold", "report", "question_1", "typical_day"],
    description="Gold layer Question 1 report generation - Typical Day analysis. Has to be manually triggered with parameters",
    params={
        "start": Param(
            type="string",
            description="Start date (YYYY-MM-DD)",
            schema={"type": "string", "format": "date"}
        ),
        "end": Param(
            type="string",
            description="End date (YYYY-MM-DD)",
            schema={"type": "string", "format": "date"}
        ),
        "polygon": Param(type="string", description="Polygon WKT"),
    },
    default_args={
        "retries": 3,
        "retry_delay": timedelta(seconds=30),
    },
    max_active_tasks=3,
    max_active_runs=1,
) as dag:

    start = EmptyOperator(task_id="start")
    done = EmptyOperator(task_id="done")

    # Infrastructure setup TaskGroup
    with TaskGroup(group_id="infra") as infra_group:
        validate_dates_task = validate_dates.override(task_id="validate_dates")(
            start_date="{{ params.start }}",
            end_date="{{ params.end }}"
        )

        save_id = generate_directory.override(task_id="create_directory")(
            start_date="{{ params.start }}",
            end_date="{{ params.end }}",
            polygon_wkt="{{ params.polygon }}"
        )

        verify_s3 = GOLD_verify_s3_connection.override(task_id="verify_s3_connection")()

        # Sequential execution within infra group
        validate_dates_task >> save_id >> verify_s3

    # Question 1: Typical day reports (parallel execution)
    typ_day_map = GOLD_generate_typical_day_map.override(task_id="typical_day_map")(
        save_id=save_id,
        start_date="{{ params.start }}",
        end_date="{{ params.end }}",
        polygon_wkt="{{ params.polygon }}"
    )
    typ_day_top = GOLD_generate_top_origins.override(task_id="top_origins")(
        save_id=save_id,
        start_date="{{ params.start }}",
        end_date="{{ params.end }}",
        polygon_wkt="{{ params.polygon }}"
    )
    typ_day_hourly = GOLD_generate_hourly_distribution.override(task_id="hourly_distribution")(
        save_id=save_id,
        start_date="{{ params.start }}",
        end_date="{{ params.end }}",
        polygon_wkt="{{ params.polygon }}"
    )

    # Execution flow: start -> infra -> reports (parallel) -> done
    start >> infra_group >> [typ_day_map, typ_day_top, typ_day_hourly] >> done

