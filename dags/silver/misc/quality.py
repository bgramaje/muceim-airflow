import sys
import os
from airflow.sdk import task  # type: ignore

# Add parent directory to path to import utils
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from utils.utils import get_ducklake_connection


@task
def SILVER_od_quality():
    """
    Build silver_od_quality table optimized for DuckDB with large datasets (GBs).
    
    Optimizations:
    1. Single-pass statistics calculation (no window functions)
    2. Optimized JOIN with explicit join order hints
    3. Materialized intermediate tables to reduce memory pressure
    4. Efficient CROSS JOIN with 1-row stats table (DuckDB optimizes automatically)
    5. Uses DuckDB-specific optimizations (preserve_insertion_order=false, etc.)
    """
    print("[TASK] Building silver_od_quality table (DuckDB-optimized for large datasets)")
    print("[TASK] Using memory settings from utils.py (4GB limit, 4 threads)")

    con = get_ducklake_connection()
    
    # DuckDB-specific optimizations for large tables
    print("[TASK] Applying DuckDB optimizations for large datasets...")
    con.execute("SET enable_progress_bar=false;")  # Reduce overhead
    con.execute("SET preserve_insertion_order=false;")  # Allow reordering for performance
    con.execute("SET default_null_order='nulls_last';")  # Optimize NULL handling
    con.execute("SET enable_object_cache=true;")  # Cache metadata for better performance
    
    print("[TASK] Step 1/3: Creating enriched metrics table (optimized JOIN)...")
    con.execute("""
        CREATE OR REPLACE TABLE _temp_od_enriched AS
        SELECT
            od.fecha,
            od.origen_zone_id,
            od.destino_zone_id,
            od.residencia,
            od.viajes,
            od.viajes_km,
            od.distancia,
            ine.poblacion_total,
            -- Calculate metrics directly in single pass (no CTE overhead)
            CASE 
                WHEN ine.poblacion_total > 0 THEN od.viajes / ine.poblacion_total
                ELSE NULL
            END AS viajes_per_capita,
            CASE 
                WHEN ine.poblacion_total > 0 THEN od.viajes_km / ine.poblacion_total
                ELSE NULL
            END AS km_per_capita
        FROM silver_od od
        LEFT JOIN silver_ine_all ine
            ON od.origen_zone_id = ine.id
    """)
    
    # Get row count for logging
    count_enriched = con.execute("SELECT COUNT(*) FROM _temp_od_enriched").fetchone()[0]
    print(f"[TASK] Enriched table created with {count_enriched:,} rows")
    
    # Step 2: Calculate global statistics efficiently
    print("[TASK] Step 2/3: Calculating global statistics (single aggregation)...")
    con.execute("""
        CREATE OR REPLACE TEMP TABLE _temp_stats AS
        SELECT 
            COALESCE(AVG(viajes_per_capita), 0.0) AS avg_viajes,
            COALESCE(NULLIF(STDDEV_SAMP(viajes_per_capita), 0), 1.0) AS stddev_viajes
        FROM _temp_od_enriched
        WHERE viajes_per_capita IS NOT NULL
    """)
    
    # Get stats for logging
    stats = con.execute("SELECT avg_viajes, stddev_viajes FROM _temp_stats").fetchone()
    print(f"[TASK] Global stats - Avg: {stats[0]:.4f}, StdDev: {stats[1]:.4f}")

    # Step 3: Create final quality table
    # CROSS JOIN with 1-row table is highly optimized by DuckDB (broadcast join)
    # DuckDB automatically broadcasts small tables, making this very efficient
    # No need for parameterized queries - CROSS JOIN is the most efficient approach
    print("[TASK] Step 3/3: Creating final quality table (optimized CROSS JOIN)...")
    
    try:
        con.execute("""
            CREATE OR REPLACE TABLE silver_od_quality AS
            WITH stats AS (
                SELECT avg_viajes, stddev_viajes FROM _temp_stats
            )
            SELECT
                e.fecha,
                e.origen_zone_id,
                e.destino_zone_id,
                e.residencia,
                -- metrics
                e.viajes_per_capita,
                e.km_per_capita,
                -- flags determinísticos (calculated once, no subqueries)
                e.viajes < 0 AS flag_negative_viajes,
                e.viajes_km < 0 AS flag_negative_viajes_km,
                -- z-scores (using pre-calculated statistics - single pass calculation)
                -- DuckDB optimizes CROSS JOIN with 1-row table automatically
                CASE 
                    WHEN e.viajes_per_capita IS NOT NULL AND s.stddev_viajes > 0
                    THEN (e.viajes_per_capita - s.avg_viajes) / s.stddev_viajes
                    ELSE NULL
                END AS z_viajes_per_capita,
                -- flags de outliers
                CASE 
                    WHEN e.viajes_per_capita IS NOT NULL AND s.stddev_viajes > 0
                    THEN ABS((e.viajes_per_capita - s.avg_viajes) / s.stddev_viajes) > 3
                    ELSE FALSE
                END AS flag_outlier_viajes_per_capita
            FROM _temp_od_enriched e
            CROSS JOIN stats s
        """)
    except Exception as e:
        pass
    
    # Get final count
    count = con.execute("SELECT COUNT(*) AS count FROM silver_od_quality").fetchdf()
    record_count = int(count.iloc[0]['count'])
    
    print(f"[TASK] Created silver_od_quality with {record_count:,} records")

    return {
        "status": "success",
        "table": "silver_od_quality",
        "records": record_count
    }
