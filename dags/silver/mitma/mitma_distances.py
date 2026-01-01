import sys
import os
from airflow.sdk import task

# Add parent directory to path to import utils
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))))

from utils.gcp import execute_cloud_run_job_sql


@task
def SILVER_mitma_distances(**context):
    """
    Airflow task to calculate distances between zones
    and store them in silver_distances table.
    The distances are calculated between zones of the same type.
    Executes using Cloud Run Job (ducklake-executor).
    """
    print("[TASK] Building silver_mitma_distances table using Cloud Run")

    sql_query = """
        LOAD spatial;
        CREATE OR REPLACE TABLE silver_mitma_distances AS
        SELECT
            o.id AS origin,
            d.id AS destination,
            ST_Distance_Sphere(o.centroid, d.centroid) / 1000.0 AS distance_km
        FROM silver_zones AS o
            CROSS JOIN silver_zones AS d
        WHERE o.id < d.id;
    """

    result = execute_cloud_run_job_sql(sql_query=sql_query, **context)
    
    print(f"[TASK] ✅ silver_mitma_distances table built successfully")
    print(f"[TASK] Execution time: {result.get('execution_time_seconds', 0)} seconds")

    return {
        "status": "success",
        "table": "silver_mitma_distances",
        "execution_name": result.get("execution_name"),
        "execution_time_seconds": result.get("execution_time_seconds")
    }
