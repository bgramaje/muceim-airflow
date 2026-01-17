"""
DAG for Gold layer report generation.

This DAG generates visualizations and reports for the business questions.
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
import hashlib

from gold.tasks import (
    GOLD_generate_typical_day_map,
    GOLD_generate_top_origins,
    GOLD_generate_hourly_distribution,
    GOLD_generate_mismatch_distribution,
    GOLD_generate_table,
    GOLD_generate_mismatch_map,
    GOLD_generate_in_out_distribution,
    GOLD_generate_functional_type_map
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
    - str: Short unique identifier for the report directory
    """
    combined = f"{start_date}_{end_date}_{polygon_wkt}"
    short_id = hashlib.md5(combined.encode()).hexdigest()[:4]

    content = f"Start date: {start_date}\nEnd date: {end_date}\n\n{polygon_wkt}"
    bucket_name = Variable.get('RUSTFS_BUCKET', default='mitma')
    s3 = S3Hook(aws_conn_id="rustfs_s3_conn")
    s3_key = f"gold/{short_id}/info.txt"
    s3.load_string(
        string_data=content,
        key=s3_key,
        bucket_name=bucket_name,
        replace=True
    )
    print(f"[SUCCESS] Uploaded to s3://{bucket_name}/{s3_key}")
    return short_id


with DAG(
    dag_id="gold_report",
    start_date=datetime(2025, 12, 1),
    catchup=False,
    tags=["gold", "report"],
    description="Gold layer report generation - Has to be manually triggered with parameters",
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
    max_active_tasks=5,
    max_active_runs=1,
) as dag:

    start = EmptyOperator(task_id="start")
    done = EmptyOperator(task_id="done")

    save_id = generate_directory.override(task_id="create_directory")(
        start_date="{{ params.start }}",
        end_date="{{ params.end }}",
        polygon_wkt="{{ params.polygon }}"
    )

    with TaskGroup("question_1", tooltip="Typical Day Tasks") as q1_group:
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
        
        typ_day_map >> typ_day_top >> typ_day_hourly

    with TaskGroup("question_2", tooltip="Gravity Model Tasks") as q2_group:
        grav_dist = GOLD_generate_mismatch_distribution.override(task_id="gravity_model_mismatch_distribution")(
            save_id=save_id,
            start_date="{{ params.start }}",
            end_date="{{ params.end }}",
            polygon_wkt="{{ params.polygon }}"
        )
        grav_table = GOLD_generate_table.override(task_id="gravity_model_table")(
            save_id=save_id,
            start_date="{{ params.start }}",
            end_date="{{ params.end }}",
            polygon_wkt="{{ params.polygon }}"
        )
        grav_map = GOLD_generate_mismatch_map.override(task_id="gravity_model_mismatch_map")(
            save_id=save_id,
            start_date="{{ params.start }}",
            end_date="{{ params.end }}",
            polygon_wkt="{{ params.polygon }}"
        )

    with TaskGroup("question_3", tooltip="Functional Type Tasks") as q3_group:
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

    start >> save_id >> [q1_group, q2_group, q3_group] >> done
