"""
Task: Zone Ranking by Service Level

Aggregates mismatch data at the zone level to rank zones by service level.
Identifies the best and worst served areas weighted by population and economy.
"""

from airflow.sdk import task
from typing import Dict, Any

from utils.gcp import execute_sql_or_cloud_run


@task
def GOLD_gravity_zone_ranking(**context) -> Dict[str, Any]:
    """
    Creates zone-level rankings based on aggregated mismatch metrics.
    
    Output tables:
    - gold_gravity_zone_ranking: Per-zone aggregated metrics
    - gold_gravity_worst_zones: Top 50 worst-served zones
    - gold_gravity_best_zones: Top 50 best-served zones
    - gold_gravity_critical_corridors: O-D pairs with highest unmet demand
    """
    print("[TASK] Computing zone rankings")
    
    sql_query = """
    -- DuckDB optimizations
    SET preserve_insertion_order=false;
    SET enable_object_cache=true;
    
    -- Zone-level ranking (aggregating as origin AND destination)
    CREATE OR REPLACE TABLE gold_gravity_zone_ranking AS
    WITH 
    -- Aggregate metrics when zone is ORIGIN
    as_origin AS (
        SELECT 
            zone_i AS zone_id,
            COUNT(*) AS connections_as_origin,
            SUM(viajes_reales) AS viajes_salida,
            SUM(T_estimado) AS T_estimado_salida,
            SUM(demand_gap) AS demand_gap_salida,
            SUM(mismatch_ratio * Pj) / NULLIF(SUM(Pj), 0) AS mismatch_ponderado_salida,
            AVG(mismatch_ratio) FILTER (WHERE mismatch_ratio IS NOT NULL) AS avg_mismatch_salida
        FROM gold_gravity_mismatch
        GROUP BY zone_i
    ),
    
    -- Aggregate metrics when zone is DESTINATION
    as_destination AS (
        SELECT 
            zone_j AS zone_id,
            COUNT(*) AS connections_as_dest,
            SUM(viajes_reales) AS viajes_entrada,
            SUM(T_estimado) AS T_estimado_entrada,
            SUM(demand_gap) AS demand_gap_entrada,
            SUM(mismatch_ratio * Pi) / NULLIF(SUM(Pi), 0) AS mismatch_ponderado_entrada,
            AVG(mismatch_ratio) FILTER (WHERE mismatch_ratio IS NOT NULL) AS avg_mismatch_entrada
        FROM gold_gravity_mismatch
        GROUP BY zone_j
    ),
    
    -- Combine both perspectives
    combined AS (
        SELECT 
            COALESCE(o.zone_id, d.zone_id) AS zone_id,
            COALESCE(o.connections_as_origin, 0) + COALESCE(d.connections_as_dest, 0) AS total_connections,
            
            -- Outbound metrics
            COALESCE(o.viajes_salida, 0) AS viajes_salida,
            COALESCE(o.T_estimado_salida, 0) AS T_estimado_salida,
            COALESCE(o.demand_gap_salida, 0) AS demand_gap_salida,
            o.mismatch_ponderado_salida,
            o.avg_mismatch_salida,
            
            -- Inbound metrics
            COALESCE(d.viajes_entrada, 0) AS viajes_entrada,
            COALESCE(d.T_estimado_entrada, 0) AS T_estimado_entrada,
            COALESCE(d.demand_gap_entrada, 0) AS demand_gap_entrada,
            d.mismatch_ponderado_entrada,
            d.avg_mismatch_entrada
            
        FROM as_origin o
        FULL OUTER JOIN as_destination d ON o.zone_id = d.zone_id
    )
    
    SELECT 
        c.zone_id,
        z.nombre AS zone_name,
        ine.poblacion_total,
        ine.empresas,
        ine.renta_media,
        
        c.total_connections,
        
        -- Combined metrics (in + out)
        c.viajes_salida + c.viajes_entrada AS total_viajes,
        c.T_estimado_salida + c.T_estimado_entrada AS total_T_estimado,
        c.demand_gap_salida + c.demand_gap_entrada AS total_demand_gap,
        
        -- Individual direction metrics
        c.viajes_salida,
        c.viajes_entrada,
        c.mismatch_ponderado_salida,
        c.mismatch_ponderado_entrada,
        
        -- Overall mismatch (weighted average of both directions)
        (COALESCE(c.mismatch_ponderado_salida, 0) + COALESCE(c.mismatch_ponderado_entrada, 0)) / 2.0 
            AS mismatch_global,
        
        -- Service level classification for the zone
        CASE 
            WHEN (COALESCE(c.mismatch_ponderado_salida, 0) + COALESCE(c.mismatch_ponderado_entrada, 0)) / 2.0 < 0.3 
                THEN 'critical'
            WHEN (COALESCE(c.mismatch_ponderado_salida, 0) + COALESCE(c.mismatch_ponderado_entrada, 0)) / 2.0 < 0.7 
                THEN 'underserved'
            WHEN (COALESCE(c.mismatch_ponderado_salida, 0) + COALESCE(c.mismatch_ponderado_entrada, 0)) / 2.0 < 1.3 
                THEN 'balanced'
            WHEN (COALESCE(c.mismatch_ponderado_salida, 0) + COALESCE(c.mismatch_ponderado_entrada, 0)) / 2.0 < 2.0 
                THEN 'well_served'
            ELSE 'over_served'
        END AS service_level,
        
        -- Priority score (higher = needs more attention)
        -- Considers: low mismatch, high population, high economic activity
        CASE 
            WHEN (COALESCE(c.mismatch_ponderado_salida, 0) + COALESCE(c.mismatch_ponderado_entrada, 0)) / 2.0 > 0 
            THEN (1.0 / ((COALESCE(c.mismatch_ponderado_salida, 0) + COALESCE(c.mismatch_ponderado_entrada, 0)) / 2.0 + 0.01))
                 * LOG(COALESCE(ine.poblacion_total, 1) + 1) 
                 * LOG(COALESCE(ine.empresas, 1) + 1)
            ELSE 0
        END AS priority_score
        
    FROM combined c
    LEFT JOIN silver_zones z ON c.zone_id = z.id
    LEFT JOIN silver_ine_all ine ON c.zone_id = ine.id
    ORDER BY mismatch_global ASC NULLS LAST;
    
    -- Top 50 worst-served zones (highest priority for infrastructure investment)
    CREATE OR REPLACE TABLE gold_gravity_worst_zones AS
    SELECT 
        zone_id,
        zone_name,
        poblacion_total,
        empresas,
        renta_media,
        mismatch_global,
        total_demand_gap,
        service_level,
        priority_score,
        ROW_NUMBER() OVER (ORDER BY mismatch_global ASC NULLS LAST) AS worst_rank
    FROM gold_gravity_zone_ranking
    WHERE mismatch_global IS NOT NULL 
      AND mismatch_global < 1.0
      AND poblacion_total > 0
    ORDER BY mismatch_global ASC
    LIMIT 50;
    
    -- Top 50 best-served zones
    CREATE OR REPLACE TABLE gold_gravity_best_zones AS
    SELECT 
        zone_id,
        zone_name,
        poblacion_total,
        empresas,
        renta_media,
        mismatch_global,
        total_demand_gap,
        service_level,
        priority_score,
        ROW_NUMBER() OVER (ORDER BY mismatch_global DESC NULLS LAST) AS best_rank
    FROM gold_gravity_zone_ranking
    WHERE mismatch_global IS NOT NULL 
      AND mismatch_global > 1.0
      AND poblacion_total > 0
    ORDER BY mismatch_global DESC
    LIMIT 50;
    
    -- Critical corridors: O-D pairs with highest unmet demand
    CREATE OR REPLACE TABLE gold_gravity_critical_corridors AS
    SELECT 
        zone_i,
        nombre_i,
        zone_j,
        nombre_j,
        distance_km,
        Pi AS poblacion_origen,
        Ej AS empresas_destino,
        viajes_reales,
        T_estimado,
        demand_gap,
        mismatch_ratio,
        service_level,
        ROW_NUMBER() OVER (ORDER BY demand_gap DESC NULLS LAST) AS corridor_rank
    FROM gold_gravity_mismatch
    WHERE demand_gap > 0
      AND T_estimado > 0
      AND mismatch_ratio < 1.0
    ORDER BY demand_gap DESC
    LIMIT 100;
    """
    
    result = execute_sql_or_cloud_run(sql_query=sql_query, **context)
    
    print(f"[TASK] Zone ranking tables created successfully")
    print(f"[TASK] Execution time: {result.get('execution_time_seconds', 0)} seconds")
    
    return {
        "status": "success",
        "tables": [
            "gold_gravity_zone_ranking",
            "gold_gravity_worst_zones", 
            "gold_gravity_best_zones",
            "gold_gravity_critical_corridors"
        ],
        "execution_name": result.get("execution_name"),
        "execution_time_seconds": result.get("execution_time_seconds")
    }

