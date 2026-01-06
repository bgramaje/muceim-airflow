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


def _post_process_gravity_model_complete(df, con, result_dict):
    """
    Post-processing function that calculates best_k_value and creates gold_gravity_mismatch table.
    This combines both operations in a single Cloud Run execution.
    
    Parameters:
    - df: DataFrame result from the SQL query (BEST_K_VALUE_SQL)
    - con: DuckDB connection
    - result_dict: Result dictionary from SQL execution
    
    Returns:
    - Dict with best_k_value, table name and record count
    """
    # Imports necesarios (se incluyen en el código serializado)
    import numpy as np
    
    print("[TASK] Calculating best k value and creating gravity model table")
    
    # Si no tenemos DataFrame, ejecutar la query
    if df is None:
        df = con.execute(BEST_K_VALUE_SQL).fetchdf()
    
    # 1. Calculate optimal k using numpy
    print("[TASK] Calculating best k value using RMSE minimization")
    k_values = np.linspace(10e-5, 1, 5000)
    rmse_list = []

    for k in k_values:
        estimated = k * df['gravity_no_k']
        rmse = np.sqrt(np.mean((df['actual_viajes'] - estimated)**2))
        rmse_list.append(rmse)

    best_k = float(k_values[np.argmin(rmse_list)])
    print(f"[TASK] Best k value found: {best_k}")
    
    # 2. Create gold_gravity_mismatch table with the calculated k_value
    print(f"[TASK] Creating gold_gravity_mismatch table with k={best_k}")
    
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
    """
    con.execute(gravity_sql)
    
    # 3. Get record count
    count = con.execute("SELECT COUNT(*) AS count FROM gold_gravity_mismatch").fetchdf()
    record_count = int(count.iloc[0]['count'])
    print(f"[TASK] Created gold_gravity_mismatch with {record_count:,} records")
    
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
    1. Calculates the optimal k value using RMSE minimization
    2. Creates the gold_gravity_mismatch table with that k value
    
    Everything runs in Cloud Run (if available) for better performance.

    Returns:
    - Dict with task status and info
    """
    print("[TASK] Building gold_gravity_mismatch table with best k value (Business Question 2)")

    # Execute SQL query with post-processing function
    # The post_process_func calculates k_value and creates the table in Cloud Run
    result = execute_sql_or_cloud_run(
        sql_query=BEST_K_VALUE_SQL,
        post_process_func=_post_process_gravity_model_complete,
        **context
    )

    print(f"[TASK] Execution: {result.get('execution_name', 'unknown')}")

    return {
        "status": "success",
        "table": result.get("table", "gold_gravity_mismatch"),
        "records": result.get("records", 0),
        "best_k_value": result.get("best_k_value"),
        "execution_time_seconds": result.get('execution_time_seconds', 0)
    }
