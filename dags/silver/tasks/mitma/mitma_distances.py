from airflow.sdk import task

from utils.gcp import execute_sql_or_cloud_run
from utils.logger import get_logger


@task
def SILVER_mitma_distances(**context):
    """
    Airflow task to calculate distances between zones
    and store them in silver_distances table.
    The distances are calculated between zones of the same type.
    Executes using Cloud Run Job (ducklake-executor).
    """
    logger = get_logger(__name__, context)
    logger.info("Building silver_mitma_distances table using Cloud Run")

    sql_query = """
        INSTALL spatial;
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

    result = execute_sql_or_cloud_run(sql_query=sql_query, **context)
    
    logger.info("silver_mitma_distances table built successfully")
    logger.info(f"Execution time: {result.get('execution_time_seconds', 0)} seconds")

    return {
        "status": "success",
        "table": "silver_mitma_distances",
        "execution_name": result.get("execution_name"),
        "execution_time_seconds": result.get("execution_time_seconds")
    }
