"""
DAG for Gold layer table creation.

This DAG creates the gold layer tables for business questions analysis.
Tables with heavy SQL computations use Cloud Run when available for 
better performance.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.standard.operators.empty import EmptyOperator

from gold.tasks import (
    GOLD_typical_day,
    GOLD_get_best_k_value,
    GOLD_gravity_model,
    GOLD_functional_type
)

with DAG(
    dag_id="gold_tables",
    start_date=datetime(2025, 12, 1),
    catchup=False,
    tags=["gold", "create-tables"],
    description="Gold layer tables creation - Has to be manually triggered to generate the gold tables",
    default_args={
        "retries": 3,
        "retry_delay": timedelta(seconds=30),
    }
) as dag:
    
    start = EmptyOperator(task_id="start")
    done = EmptyOperator(task_id="done")

    # Question 1: Typical Day (uses Cloud Run for heavy SQL)
    typ_day = GOLD_typical_day.override(task_id="typical_day")()
    
    # Question 2: Gravity Model (uses Cloud Run for heavy SQL)
    k_value = GOLD_get_best_k_value.override(task_id="best_k_value")()
    grav_model = GOLD_gravity_model.override(task_id="gravity_model")(k_value)

    # Question 3: Functional Type (runs locally - requires sklearn)
    func_type = GOLD_functional_type.override(task_id="functional_type")()

    # Dependencies
    start >> k_value >> grav_model >> done
    start >> [typ_day, func_type] >> done
