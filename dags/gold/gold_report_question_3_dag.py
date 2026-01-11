"""
DAG for Gold layer Question 3 report generation (Functional Type).

This DAG generates visualizations and reports for functional type classification.
Reports are generated locally as they require pandas/plotly/kepler processing
and are uploaded to S3.
"""

from datetime import datetime, timedelta
from airflow.models import Param
from airflow.sdk import Variable
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.sdk import task
from airflow import DAG
from airflow.providers.standard.operators.empty import EmptyOperator
import uuid

from gold.tasks import (
    GOLD_generate_in_out_distribution,
    GOLD_generate_functional_type_map,
    GOLD_verify_s3_connection,
)


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
    s3_key = f"gold/question3/{report_uuid}/info.txt"
    s3.load_string(
        string_data=content,
        key=s3_key,
        bucket_name=bucket_name,
        replace=True
    )
    print(f"[SUCCESS] Uploaded to s3://{bucket_name}/{s3_key}")
    return report_uuid


with DAG(
    dag_id="gold_report_question_3",
    start_date=datetime(2025, 12, 1),
    catchup=False,
    tags=["gold", "report", "question_3", "functional_type"],
    description="Gold layer Question 3 report generation - Functional Type classification. Has to be manually triggered with parameters",
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
    max_active_tasks=2,
    max_active_runs=1,
) as dag:

    start = EmptyOperator(task_id="start")
    done = EmptyOperator(task_id="done")

    save_id = generate_directory.override(task_id="create_directory")(
        start_date="{{ params.start }}",
        end_date="{{ params.end }}",
        polygon_wkt="{{ params.polygon }}"
    )

    # Verify S3 connection before generating reports
    verify_s3 = GOLD_verify_s3_connection.override(task_id="verify_s3_connection")()

    # Question 3: Functional type reports
    func_type_dist = GOLD_generate_in_out_distribution.override(task_id="functional_type_hourly")(
        save_id=save_id,
        start_date="{{ params.start }}",
        end_date="{{ params.end }}",
        polygon_wkt="{{ params.polygon }}"
    )
    func_type_map = GOLD_generate_functional_type_map.override(task_id="functional_type_map")(
        save_id=save_id,
        start_date="{{ params.start }}",
        end_date="{{ params.end }}",
        polygon_wkt="{{ params.polygon }}"
    )

    start >> save_id >> verify_s3 >> [func_type_dist, func_type_map] >> done

