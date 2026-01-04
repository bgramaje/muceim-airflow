"""
Task: Calibrate Gravity Model

Computes the calibration constant k for the gravity model.

k = SUM(viajes_reales) / SUM(T_raw)

Where T_raw = (Pi * Ej) / dij^2

This constant scales the gravity model estimates to match the actual
total trip volume in the observed data.
"""

from airflow.sdk import task
from typing import Dict, Any

from utils.gcp import execute_sql_or_cloud_run
from utils.utils import get_ducklake_connection


@task
def GOLD_gravity_calibrate_model(**context) -> Dict[str, Any]:
    """
    Calibrates the gravity model by computing k and creating
    the gold_gravity_calibrated table with estimated trips.
    
    The calibration uses only pairs with actual observed trips
    to compute k, then applies k to all pairs.
    
    Output columns (adds to base_pairs):
    - k: Calibration constant
    - T_estimado: Estimated trips (k * T_raw)
    - T_estimado_diario: Average daily estimated trips
    """
    print("[TASK] Calibrating gravity model and computing T_estimado")
    
    sql_query = """
    -- DuckDB optimizations
    SET preserve_insertion_order=false;
    SET enable_object_cache=true;
    
    -- Create calibrated table with k computed
    CREATE OR REPLACE TABLE gold_gravity_calibrated AS
    WITH 
    -- Compute k using only pairs with observed trips
    calibration AS (
        SELECT 
            SUM(viajes_reales) / NULLIF(SUM(T_raw), 0) AS k,
            SUM(viajes_reales) AS total_viajes_reales,
            SUM(T_raw) AS total_T_raw,
            COUNT(*) AS pairs_with_data
        FROM gold_gravity_base_pairs
        WHERE viajes_reales > 0 
          AND T_raw > 0
    )
    
    SELECT 
        bp.*,
        c.k,
        
        -- Estimated trips using calibrated model
        c.k * bp.T_raw AS T_estimado,
        
        -- Average daily estimates (useful for comparison)
        CASE 
            WHEN bp.dias_observados > 0 THEN bp.viajes_reales / bp.dias_observados 
            ELSE 0 
        END AS viajes_diarios_reales,
        
        CASE 
            WHEN bp.dias_observados > 0 THEN (c.k * bp.T_raw) / bp.dias_observados 
            ELSE c.k * bp.T_raw 
        END AS T_estimado_diario,
        
        -- Statistics from calibration
        c.total_viajes_reales,
        c.total_T_raw,
        c.pairs_with_data
        
    FROM gold_gravity_base_pairs bp
    CROSS JOIN calibration c;
    
    -- Create summary table with calibration stats
    CREATE OR REPLACE TABLE gold_gravity_calibration_stats AS
    SELECT 
        k,
        total_viajes_reales,
        total_T_raw,
        pairs_with_data,
        (SELECT COUNT(*) FROM gold_gravity_calibrated) AS total_pairs,
        (SELECT COUNT(*) FROM gold_gravity_calibrated WHERE viajes_reales > 0) AS pairs_with_trips,
        (SELECT AVG(T_estimado) FROM gold_gravity_calibrated WHERE T_raw > 0) AS avg_T_estimado,
        (SELECT AVG(viajes_reales) FROM gold_gravity_calibrated WHERE viajes_reales > 0) AS avg_viajes_reales,
        (SELECT date_start FROM gold_gravity_calibrated LIMIT 1) AS date_start,
        (SELECT date_end FROM gold_gravity_calibrated LIMIT 1) AS date_end,
        CURRENT_TIMESTAMP AS computed_at
    FROM (
        SELECT 
            SUM(viajes_reales) / NULLIF(SUM(T_raw), 0) AS k,
            SUM(viajes_reales) AS total_viajes_reales,
            SUM(T_raw) AS total_T_raw,
            COUNT(*) AS pairs_with_data
        FROM gold_gravity_base_pairs
        WHERE viajes_reales > 0 AND T_raw > 0
    );
    """
    
    result = execute_sql_or_cloud_run(sql_query=sql_query, **context)
    
    # Get calibration stats
    try:
        con = get_ducklake_connection()
        stats = con.execute("SELECT * FROM gold_gravity_calibration_stats").fetchdf()
        k_value = float(stats['k'].iloc[0]) if not stats.empty else None
        total_trips = int(stats['total_viajes_reales'].iloc[0]) if not stats.empty else None
        pairs_count = int(stats['pairs_with_data'].iloc[0]) if not stats.empty else None
        print(f"[TASK] Calibration complete:")
        print(f"[TASK]   k = {k_value:.6e}")
        print(f"[TASK]   Total real trips used: {total_trips:,}")
        print(f"[TASK]   Pairs with data: {pairs_count:,}")
    except Exception as e:
        print(f"[TASK] Warning: Could not fetch stats locally: {e}")
        k_value = None
        total_trips = None
        pairs_count = None
    
    print(f"[TASK] gold_gravity_calibrated table created successfully")
    print(f"[TASK] Execution time: {result.get('execution_time_seconds', 0)} seconds")
    
    return {
        "status": "success",
        "tables": ["gold_gravity_calibrated", "gold_gravity_calibration_stats"],
        "k": k_value,
        "total_trips": total_trips,
        "pairs_with_data": pairs_count,
        "execution_name": result.get("execution_name"),
        "execution_time_seconds": result.get("execution_time_seconds")
    }

