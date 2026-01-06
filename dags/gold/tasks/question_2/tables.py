"""
Question 2: Gravity Model - Table Creation Tasks

This module contains tasks for creating gold layer tables for gravity model analysis.
Heavy SQL queries are delegated to Cloud Run when available.
"""

import numpy as np
from airflow.sdk import task  # type: ignore

from utils.gcp import execute_sql_or_cloud_run


def _get_gravity_model_sql(k_value: float) -> str:
    """Generate the SQL query for gravity model with the given k value."""
    return f"""
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
            {k_value} * ((za.population * zb.economic_activity) / NULLIF(d.distance_km * d.distance_km, 0)) AS estimated_trips,
            a.actual_viajes / estimated_trips AS mismatch_ratio
        FROM actual_trips a
            JOIN zone_attributes za ON a.origen_zone_id = za.zone_id
            JOIN zone_attributes zb ON a.destino_zone_id = zb.zone_id
            JOIN distances d ON a.origen_zone_id = d.origen_zone_id
                AND a.destino_zone_id = d.destino_zone_id;
    """


BEST_K_VALUE_SQL = """
    WITH actual_trips AS (
        SELECT
            origen_zone_id,
            destino_zone_id,
            SUM(viajes) AS actual_viajes
        FROM silver_mitma_od
        WHERE EXTRACT(YEAR FROM fecha) = 2023
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
        AND TRY_CAST(i.year AS INTEGER) = 2023
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


def _post_process_best_k_value(df, con, result_dict):
    """
    Post-processing function for best_k_value calculation.
    Receives the DataFrame from the SQL query and calculates optimal k using numpy.

    Parameters:
    - df: DataFrame result from the SQL query (or None if query was not SELECT)
    - con: DuckDB connection (for additional queries if needed)
    - result_dict: Result dictionary from SQL execution

    Returns:
    - Dict with best_k_value
    """
    print("[TASK] Calculating best k value using RMSE minimization")

    # Calculate optimal k using numpy
    k_values = np.linspace(10e-5, 1, 5000)
    rmse_list = []

    for k in k_values:
        estimated = k * df['gravity_no_k']
        rmse = np.sqrt(np.mean((df['actual_viajes'] - estimated)**2))
        rmse_list.append(rmse)

    best_k = float(k_values[np.argmin(rmse_list)])
    print(f"[TASK] Best k value found: {best_k}")

    return {
        "best_k_value": best_k
    }


@task
def GOLD_get_best_k_value(**context):
    """
    Calculate the optimal k value for the gravity model using RMSE minimization.

    The heavy SQL query is executed in Cloud Run (if available), and the
    numpy-based optimization runs locally in the post-processing function.

    Returns:
    - float: The optimal k value that minimizes RMSE
    """
    print("[TASK] Calculating best k value for gravity model")

    # Execute SQL query (can use Cloud Run) with post-processing
    # The post_process_func will run locally to do the numpy calculation
    result = execute_sql_or_cloud_run(
        sql_query=BEST_K_VALUE_SQL,
        post_process_func=_post_process_best_k_value,
        **context
    )

    best_k = result.get('best_k_value')
    if best_k is None:
        raise RuntimeError("Failed to calculate best k value")

    return best_k


@task
def GOLD_gravity_model(k_value: float, **context):
    """
    Airflow task to create gold_gravity_mismatch table.

    This task creates a gold layer table comparing actual vs estimated trips
    using the gravity model. The heavy SQL computation is delegated to 
    Cloud Run when available for better performance.

    Parameters:
    - k_value: The calibration constant for the gravity model

    Returns:
    - Dict with task status and info
    """
    print(
        f"[TASK] Building gold_gravity_mismatch table with k={k_value} (Business Question 2)")

    # Execute the SQL query with post-processing function
    # The post_process_func will run locally after SQL execution (Cloud Run or local)
    result = execute_sql_or_cloud_run(
        sql_query=_get_gravity_model_sql(k_value),
        **context
    )

    print(f"[TASK] Execution: {result.get('execution_name', 'unknown')}")

    return {
        "status": "success",
        "table": result.get("table", "gold_gravity_mismatch"),
        "k_value": k_value,
        "execution_time_seconds": result.get('GOLD_gravity_model', 0)
    }
