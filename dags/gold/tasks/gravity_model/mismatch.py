"""
Task: Compute Mismatch Ratio

Computes the mismatch ratio between actual and estimated trips:

    mismatch_ratio = viajes_reales / T_estimado

Interpretation:
- mismatch > 1: More trips than expected → well-served area
- mismatch < 1: Fewer trips than expected → underserved area  
- mismatch ≈ 1: Balanced
"""

from airflow.sdk import task
from typing import Dict, Any

from utils.gcp import execute_sql_or_cloud_run


@task
def GOLD_gravity_compute_mismatch(**context) -> Dict[str, Any]:
    """
    Computes mismatch ratios for each O-D pair and adds classification.
    
    Output columns:
    - mismatch_ratio: viajes_reales / T_estimado
    - log_mismatch: LOG10(mismatch_ratio) for visualization
    - service_level: Classification (well_served, underserved, critical, no_data)
    - demand_gap: T_estimado - viajes_reales (positive = unmet demand)
    """
    print("[TASK] Computing mismatch ratios")
    
    sql_query = """
    -- DuckDB optimizations
    SET preserve_insertion_order=false;
    SET enable_object_cache=true;
    
    -- Create mismatch table
    CREATE OR REPLACE TABLE gold_gravity_mismatch AS
    SELECT 
        gc.*,
        
        -- Mismatch ratio
        CASE 
            WHEN T_estimado > 0 THEN viajes_reales / T_estimado
            ELSE NULL
        END AS mismatch_ratio,
        
        -- Log mismatch (useful for visualization, centered at 0)
        CASE 
            WHEN T_estimado > 0 AND viajes_reales > 0 
            THEN LOG10(viajes_reales / T_estimado)
            ELSE NULL
        END AS log_mismatch,
        
        -- Demand gap (positive = potential unmet demand)
        CASE 
            WHEN T_estimado > 0 THEN T_estimado - viajes_reales
            ELSE NULL
        END AS demand_gap,
        
        -- Relative demand gap as percentage
        CASE 
            WHEN T_estimado > 0 THEN ((T_estimado - viajes_reales) / T_estimado) * 100
            ELSE NULL
        END AS demand_gap_pct,
        
        -- Service level classification
        CASE 
            WHEN viajes_reales = 0 AND T_estimado > 0 THEN 'critical'
            WHEN T_estimado = 0 OR viajes_reales = 0 THEN 'no_data'
            WHEN viajes_reales / T_estimado < 0.3 THEN 'critical'
            WHEN viajes_reales / T_estimado < 0.7 THEN 'underserved'
            WHEN viajes_reales / T_estimado < 1.3 THEN 'balanced'
            WHEN viajes_reales / T_estimado < 2.0 THEN 'well_served'
            ELSE 'over_served'
        END AS service_level,
        
        -- Weight for aggregation (based on population and economic activity)
        (Pi + Pj) * (empresas_i + Ej) AS importance_weight
        
    FROM gold_gravity_calibrated gc;
    
    -- Create summary statistics by service level
    CREATE OR REPLACE TABLE gold_gravity_service_summary AS
    SELECT 
        service_level,
        COUNT(*) AS pair_count,
        SUM(viajes_reales) AS total_viajes_reales,
        SUM(T_estimado) AS total_T_estimado,
        SUM(demand_gap) AS total_demand_gap,
        AVG(mismatch_ratio) AS avg_mismatch,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY mismatch_ratio) AS median_mismatch,
        SUM(Pi + Pj) AS total_population_affected,
        SUM(empresas_i + Ej) AS total_empresas_affected
    FROM gold_gravity_mismatch
    WHERE service_level != 'no_data'
    GROUP BY service_level
    ORDER BY 
        CASE service_level 
            WHEN 'critical' THEN 1 
            WHEN 'underserved' THEN 2 
            WHEN 'balanced' THEN 3
            WHEN 'well_served' THEN 4
            WHEN 'over_served' THEN 5
        END;
    """
    
    result = execute_sql_or_cloud_run(sql_query=sql_query, **context)
    
    print(f"[TASK] gold_gravity_mismatch table created successfully")
    print(f"[TASK] Execution time: {result.get('execution_time_seconds', 0)} seconds")
    
    return {
        "status": "success",
        "tables": ["gold_gravity_mismatch", "gold_gravity_service_summary"],
        "execution_name": result.get("execution_name"),
        "execution_time_seconds": result.get("execution_time_seconds")
    }

