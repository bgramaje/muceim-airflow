"""
Task: Create Base O-D Pairs Table

Creates a table with all origin-destination pairs combining:
- Distances from silver_mitma_distances
- Population from silver_ine_all (origin)
- Economic activity (companies) from silver_ine_all (destination)
- Actual trips aggregated from silver_mitma_od (filtered by date range)
"""

from airflow.sdk import task
from typing import Dict, Any

from utils.gcp import execute_sql_or_cloud_run


@task
def GOLD_gravity_create_base_pairs(**context) -> Dict[str, Any]:
    """
    Creates the gold_gravity_base_pairs table with all necessary data
    for gravity model computation.
    
    Uses DAG params:
    - start_date: Start date for trip aggregation (YYYY-MM-DD)
    - end_date: End date for trip aggregation (YYYY-MM-DD)
    
    Output columns:
    - zone_i: Origin zone ID
    - zone_j: Destination zone ID
    - distance_km: Distance between zone centroids
    - Pi: Population at origin
    - Ej: Economic activity (companies) at destination
    - Pj: Population at destination (for weighting)
    - renta_i: Average income at origin
    - renta_j: Average income at destination
    - viajes_reales: Actual trips (sum of both directions)
    - dias_observados: Number of distinct days with data
    """
    print("[TASK] Creating gold_gravity_base_pairs table")
    
    # Get date range from params
    start_date = context['params'].get('start_date', '2023-01-01')
    end_date = context['params'].get('end_date', '2023-12-31')
    
    print(f"[TASK] Date range: {start_date} to {end_date}")
    
    sql_query = f"""
    -- DuckDB optimizations
    SET preserve_insertion_order=false;
    SET enable_object_cache=true;
    
    -- Create or replace the base pairs table
    CREATE OR REPLACE TABLE gold_gravity_base_pairs AS
    WITH 
    -- Aggregate actual trips from silver_mitma_od for the date range
    -- Sum trips in both directions (i->j and j->i)
    actual_trips AS (
        SELECT 
            LEAST(origen_zone_id, destino_zone_id) AS zone_i,
            GREATEST(origen_zone_id, destino_zone_id) AS zone_j,
            SUM(viajes) AS viajes_reales,
            SUM(viajes_km) AS viajes_km_reales,
            COUNT(DISTINCT DATE(fecha)) AS dias_observados
        FROM silver_mitma_od
        WHERE fecha >= '{start_date}'::TIMESTAMP 
          AND fecha < '{end_date}'::TIMESTAMP + INTERVAL '1 day'
        GROUP BY 1, 2
    ),
    
    -- Get zone pairs with distances (already has origin < destination)
    zone_pairs AS (
        SELECT 
            d.origin AS zone_i,
            d.destination AS zone_j,
            d.distance_km
        FROM silver_mitma_distances d
        WHERE d.distance_km > 0  -- Exclude zero distance pairs
    )
    
    -- Join everything together
    SELECT 
        zp.zone_i,
        zp.zone_j,
        zp.distance_km,
        
        -- Origin zone data
        COALESCE(o.poblacion_total, 0) AS Pi,
        COALESCE(o.empresas, 0) AS empresas_i,
        COALESCE(o.renta_media, 0) AS renta_i,
        o.nombre AS nombre_i,
        
        -- Destination zone data  
        COALESCE(dest.poblacion_total, 0) AS Pj,
        COALESCE(dest.empresas, 0) AS Ej,
        COALESCE(dest.renta_media, 0) AS renta_j,
        dest.nombre AS nombre_j,
        
        -- Actual trips
        COALESCE(at.viajes_reales, 0) AS viajes_reales,
        COALESCE(at.viajes_km_reales, 0) AS viajes_km_reales,
        COALESCE(at.dias_observados, 0) AS dias_observados,
        
        -- Raw gravity estimation (without k constant)
        CASE 
            WHEN zp.distance_km > 0 THEN
                (COALESCE(o.poblacion_total, 0) * COALESCE(dest.empresas, 0)) 
                / POWER(zp.distance_km, 2)
            ELSE 0
        END AS T_raw,
        
        -- Metadata
        '{start_date}' AS date_start,
        '{end_date}' AS date_end
        
    FROM zone_pairs zp
    
    -- Join origin zone INE data
    LEFT JOIN silver_ine_all o 
        ON zp.zone_i = o.id
    
    -- Join destination zone INE data
    LEFT JOIN silver_ine_all dest 
        ON zp.zone_j = dest.id
    
    -- Join actual trips
    LEFT JOIN actual_trips at 
        ON zp.zone_i = at.zone_i 
        AND zp.zone_j = at.zone_j
    
    -- Filter to only include pairs where we have INE data for both zones
    WHERE o.id IS NOT NULL 
      AND dest.id IS NOT NULL
      AND (COALESCE(o.poblacion_total, 0) > 0 OR COALESCE(dest.empresas, 0) > 0);
    """
    
    result = execute_sql_or_cloud_run(sql_query=sql_query, **context)
    
    print(f"[TASK] gold_gravity_base_pairs table created successfully")
    print(f"[TASK] Execution time: {result.get('execution_time_seconds', 0)} seconds")
    
    return {
        "status": "success",
        "table": "gold_gravity_base_pairs",
        "date_range": {"start": start_date, "end": end_date},
        "execution_name": result.get("execution_name"),
        "execution_time_seconds": result.get("execution_time_seconds")
    }

