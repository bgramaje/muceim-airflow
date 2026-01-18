"""
Question 2: Gravity Model - Table Creation Tasks

This module contains tasks for creating gold layer tables for gravity model analysis.
Heavy SQL queries are delegated to Cloud Run when available.
"""

import numpy as np
from airflow.sdk import task  # type: ignore

from utils.gcp import execute_sql_or_cloud_run
from utils.logger import get_logger


def _post_process_gravity_model_complete(df, con, result_dict):
    """
    Post-processing function that calculates best_k_value and creates gold_gravity_mismatch table.
    This combines both operations in a single Cloud Run execution.
    
    The k_value is calculated using data from the most recent year available in silver_mitma_od.
    
    Parameters:
    - df: DataFrame result from the SQL query (should be None, we'll calculate year dynamically)
    - con: DuckDB connection
    - result_dict: Result dictionary from SQL execution
    
    Returns:
    - Dict with best_k_value, table name and record count
    """
    # Imports necesarios (se incluyen en el código serializado)
    import numpy as np
    
    logger = get_logger(__name__)
    logger.info("Calculating best k value and creating gravity model table")
    
    # Get the most recent year from silver_mitma_od
    year_result = con.execute("""
        SELECT EXTRACT(YEAR FROM MAX(fecha))::INTEGER AS max_year
        FROM silver_mitma_od
        WHERE fecha IS NOT NULL
    """).fetchdf()
    
    if year_result.empty or year_result.iloc[0]['max_year'] is None:
        raise ValueError("No data available in silver_mitma_od to calculate k_value")
    
    target_year = int(year_result.iloc[0]['max_year'])
    logger.info(f"Using year {target_year} for k_value calculation")
    
    # Generate SQL query for the target year (inline, since function may not be available in Cloud Run)
    best_k_sql = f"""
        WITH actual_trips AS (
            SELECT
                origen_zone_id,
                destino_zone_id,
                SUM(viajes) AS actual_viajes
            FROM silver_mitma_od
            WHERE EXTRACT(YEAR FROM fecha) = {target_year}
            GROUP BY 1,2
        ),
        zone_attributes AS (
            SELECT
                z.id AS zone_id,
                i.poblacion_total AS population,
                i.empresas AS economic_activity
            FROM silver_zones z
            LEFT JOIN silver_ine_all i
                ON z.id = i.id
            AND i.year IS NOT NULL
            AND i.year != ''
            AND TRY_CAST(i.year AS INTEGER) = {target_year}
        ),
        distances AS (
            SELECT
                origin AS origen_zone_id,
                destination AS destino_zone_id,
                distance_km
            FROM silver_mitma_distances
        )
        SELECT
            a.origen_zone_id,
            a.destino_zone_id,
            a.actual_viajes,
            (za.population * zb.economic_activity) / NULLIF(d.distance_km * d.distance_km, 0) AS gravity_no_k
        FROM actual_trips a
        JOIN zone_attributes za ON a.origen_zone_id = za.zone_id
        JOIN zone_attributes zb ON a.destino_zone_id = zb.zone_id
        JOIN distances d
        ON a.origen_zone_id = d.origen_zone_id
        AND a.destino_zone_id = d.destino_zone_id
    """
    
    # Execute query to get data for k calculation
    df = con.execute(best_k_sql).fetchdf()
    
    # 1. Calculate optimal k using numpy
    logger.info("Calculating best k value using RMSE minimization")
    k_values = np.linspace(10e-5, 1, 5000)
    rmse_list = []

    for k in k_values:
        estimated = k * df['gravity_no_k']
        rmse = np.sqrt(np.mean((df['actual_viajes'] - estimated)**2))
        rmse_list.append(rmse)

    best_k = float(k_values[np.argmin(rmse_list)])
    logger.info(f"Best k value found: {best_k}")
    
    # 2. Create gold_gravity_mismatch table with the calculated k_value
    logger.info(f"Creating gold_gravity_mismatch table with k={best_k}")
    
    # Inline SQL generation (no need for _get_gravity_model_sql function)
    gravity_sql = f"""
        CREATE OR REPLACE TABLE gold_gravity_mismatch AS
        WITH actual_trips AS (
            SELECT
                origen_zone_id,
                destino_zone_id,
                DATE(fecha) AS date,
                SUM(viajes) AS actual_viajes
            FROM silver_mitma_od
            GROUP BY 1,2,3
        ),
        zone_attributes AS (
            SELECT
                z.id AS zone_id,
                i.poblacion_total AS population,
                i.empresas AS economic_activity
            FROM silver_zones z
                LEFT JOIN silver_ine_all i ON z.id = i.id
        ),
        distances AS (
            SELECT
                origin AS origen_zone_id,
                destination AS destino_zone_id,
                distance_km
            FROM silver_mitma_distances
        )
        SELECT
            a.origen_zone_id AS origin_id,
            a.destino_zone_id AS destination_id,
            a.date AS date,
            a.actual_viajes AS actual_trips,
            {best_k} * ((za.population * zb.economic_activity) / NULLIF(d.distance_km * d.distance_km, 0)) AS estimated_trips,
            a.actual_viajes / estimated_trips AS mismatch_ratio
        FROM actual_trips a
            JOIN zone_attributes za ON a.origen_zone_id = za.zone_id
            JOIN zone_attributes zb ON a.destino_zone_id = zb.zone_id
            JOIN distances d ON a.origen_zone_id = d.origen_zone_id
                AND a.destino_zone_id = d.destino_zone_id;
        
        -- Apply partitioning by year/month/day (same pattern as bronze_mitma_od and silver_mitma_od)
        ALTER TABLE gold_gravity_mismatch SET PARTITIONED BY (year(date), month(date), day(date));
    """
    con.execute(gravity_sql)
    
    # 3. Get record count
    count = con.execute("SELECT COUNT(*) AS count FROM gold_gravity_mismatch").fetchdf()
    record_count = int(count.iloc[0]['count'])
    logger.info(f"Created gold_gravity_mismatch with {record_count:,} records")
    
    return {
        "best_k_value": best_k,
        "table": "gold_gravity_mismatch",
        "records": record_count
    }


@task
def GOLD_gravity_model(**context):
    """
    Airflow task to calculate best k value and create gold_gravity_mismatch table.
    
    This task combines both operations:
    1. Calculates the optimal k value using RMSE minimization (using most recent year available)
    2. Creates the gold_gravity_mismatch table with that k value
    
    The k_value is calculated using data from the most recent year available in silver_mitma_od.
    Everything runs in Cloud Run (if available) for better performance.

    Returns:
    - Dict with task status and info
    """
    logger = get_logger(__name__, context)
    logger.info("Building gold_gravity_mismatch table with best k value (Business Question 2)")

    # Execute with post-processing function
    # The post_process_func will determine the year dynamically and calculate k_value
    # We pass a dummy query since the year will be calculated in post_process
    dummy_query = "SELECT 1 AS dummy"
    
    result = execute_sql_or_cloud_run(
        sql_query=dummy_query,
        post_process_func=_post_process_gravity_model_complete,
        **context
    )

    logger.info(f"Execution: {result.get('execution_name', 'unknown')}")

    return {
        "status": "success",
        "table": result.get("table", "gold_gravity_mismatch"),
        "records": result.get("records", 0),
        "best_k_value": result.get("best_k_value"),
        "execution_time_seconds": result.get('execution_time_seconds', 0)
    }
