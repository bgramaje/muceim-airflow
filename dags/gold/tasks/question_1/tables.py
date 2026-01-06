"""
Question 1: Typical Day - Table Creation Tasks

This module contains tasks for creating gold layer tables for typical day analysis.
Heavy SQL queries are delegated to Cloud Run when available.
"""

from airflow.sdk import task  # type: ignore

from utils.gcp import execute_sql_or_cloud_run


GOLD_TYPICAL_DAY_SQL = """
    CREATE OR REPLACE TABLE gold_typical_day_od_hourly AS
    WITH base AS (
        SELECT
            origen_zone_id,
            destino_zone_id,
            EXTRACT(HOUR FROM fecha) AS hour,
            viajes,
            viajes_km,
            DATE(fecha) AS date
        FROM silver_mitma_od
    ),
    daily_agg AS (
        SELECT
            origen_zone_id,
            destino_zone_id,
            hour,
            date,
            SUM(viajes) AS viajes_day,
            SUM(viajes_km) AS km_day
        FROM base
        GROUP BY 1,2,3,4
    )
    SELECT
        origen_zone_id AS origin_id,
        destino_zone_id AS destination_id,
        date,
        hour,
        AVG(viajes_day) AS avg_trips,
        AVG(km_day) AS avg_km
    FROM daily_agg
    GROUP BY 1,2,3,4;
"""


def _post_process_typical_day(df, con, result_dict):
    """
    Post-processing function for typical_day table.
    Gets the record count and adds it to the result.
    
    Parameters:
    - df: DataFrame result from the SQL query (or None if query was not SELECT)
    - con: DuckDB connection (for additional queries if needed)
    - result_dict: Result dictionary from SQL execution
    
    Returns:
    - Dict with additional fields to merge into result
    """
    # Para CREATE TABLE, siempre necesitamos hacer un COUNT
    count = con.execute("SELECT COUNT(*) AS count FROM gold_typical_day_od_hourly").fetchdf()
    record_count = int(count.iloc[0]['count'])
    print(f"[TASK] Created gold_typical_day_od_hourly with {record_count:,} records")
    
    return {
        "table": "gold_typical_day_od_hourly",
        "records": record_count
    }


@task
def GOLD_typical_day(**context):
    """
    Airflow task to create gold_typical_day_od_hourly table.

    This task creates a gold layer table with typical day OD patterns
    aggregated by hour. The heavy SQL computation is delegated to 
    Cloud Run when available for better performance.

    Returns:
    - Dict with task status and info
    """
    print("[TASK] Building gold_typical_day_od_hourly table (Business Question 1)")

    # Execute the SQL query with post-processing function
    # The post_process_func will run in Cloud Run (if available) with the DataFrame
    result = execute_sql_or_cloud_run(
        sql_query=GOLD_TYPICAL_DAY_SQL,
        post_process_func=_post_process_typical_day,
        **context
    )

    print(f"[TASK] Execution: {result.get('execution_name', 'unknown')}")

    return {
        "status": "success",
        "table": result.get("table", "gold_typical_day_od_hourly"),
        "records": result.get("records", 0),
        "execution_time_seconds": result.get('execution_time_seconds', 0)
    }
