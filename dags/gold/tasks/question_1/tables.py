"""
Question 1: Typical Day - Table Creation Tasks

This module contains tasks for creating gold layer tables for typical day analysis.
Uses incremental processing: only processes dates that are not yet in the gold table.
Heavy SQL queries are delegated to Cloud Run when available.
"""

from airflow.sdk import task  # type: ignore
from typing import List, Dict, Any

from utils.gcp import execute_sql_or_cloud_run
from utils.utils import get_ducklake_connection


@task
def GOLD_typical_day_create_table(**context) -> Dict:
    """
    Creates the gold_typical_day_od_hourly table if it doesn't exist.
    The table is partitioned by year/month/day of date column.
    
    Returns:
    - Dict with status of table creation
    """
    print("[TASK] Creating gold_typical_day_od_hourly table if it doesn't exist")
    
    con = get_ducklake_connection()
    
    table_exists = False
    try:
        result = con.execute("""
            SELECT COUNT(*) as cnt FROM information_schema.tables 
            WHERE table_schema = 'main' AND table_name = 'gold_typical_day_od_hourly'
        """).fetchone()
        table_exists = result[0] > 0
    except:
        pass
    
    # Create table if it doesn't exist
    con.execute("""
        SET preserve_insertion_order=false;
        SET enable_object_cache=true;
        
        CREATE TABLE IF NOT EXISTS gold_typical_day_od_hourly (
            origin_id VARCHAR,
            destination_id VARCHAR,
            date DATE,
            hour INTEGER,
            avg_trips DOUBLE,
            avg_km DOUBLE
        );
    """)
    
    # Apply partitioning only if table is new
    if not table_exists:
        con.execute("ALTER TABLE gold_typical_day_od_hourly SET PARTITIONED BY (year(date), month(date), day(date));")
        print("[TASK] Applied partitioning")
    
    print("[TASK] Table gold_typical_day_od_hourly created/verified successfully")
    
    return {
        "status": "success",
        "table": "gold_typical_day_od_hourly",
    }


@task
def GOLD_typical_day_get_date_batches(
    batch_size: int = 2,
    **context
) -> List[Dict[str, Any]]:
    """
    Gets dates from silver_mitma_od that are not yet in gold_typical_day_od_hourly,
    and splits them into batches.
    
    Parameters:
    - batch_size: Number of dates per batch (default: 2). Can be passed from DAG params.
    
    Returns:
    - List of dictionaries with 'fechas' (list of dates) and 'batch_index' for each batch
    """
    # Convert batch_size to int if it comes as string from params
    batch_size = int(batch_size) if isinstance(batch_size, str) else batch_size
    
    print(f"[TASK] Getting unprocessed dates for gold_typical_day_od_hourly (batch_size: {batch_size})")
    
    con = get_ducklake_connection()
    
    # Get dates that are in silver_mitma_od but not in gold_typical_day_od_hourly
    # silver_mitma_od.fecha is TIMESTAMP, gold_typical_day_od_hourly.date is DATE
    query = """
        SELECT DISTINCT 
            strftime(fecha, '%Y%m%d') AS fecha
        FROM silver_mitma_od
        WHERE fecha IS NOT NULL
            AND DATE(fecha) NOT IN (
                SELECT DISTINCT date
                FROM gold_typical_day_od_hourly
                WHERE date IS NOT NULL
            )
        ORDER BY fecha
    """
    
    try:
        df = con.execute(query).fetchdf()
        
        if df.empty:
            print("[TASK] No unprocessed dates found")
            return []
        
        fechas = [str(fecha) for fecha in df['fecha'].unique()]
        fechas = sorted(fechas)
        
        if not fechas:
            print("[TASK] No valid unprocessed dates found")
            return []
        
        print(f"[TASK] Total unprocessed dates: {len(fechas)}")
        
        batches = []
        batch_index = 0
        
        for i in range(0, len(fechas), batch_size):
            batch_fechas = fechas[i:i + batch_size]
            
            batches.append({
                'batch_index': batch_index,
                'fechas': batch_fechas
            })
            
            batch_index += 1
        
        print(f"[TASK] Created {len(batches)} batches")
        return batches
        
    except Exception as e:
        # If gold table doesn't exist, get all dates from silver
        print(f"[TASK] Gold table may not exist, getting all silver dates: {e}")
        try:
            df = con.execute("""
                SELECT DISTINCT strftime(fecha, '%Y%m%d') AS fecha
                FROM silver_mitma_od
                WHERE fecha IS NOT NULL
                ORDER BY fecha
            """).fetchdf()
            
            if df.empty:
                return []
            
            fechas = [str(fecha) for fecha in df['fecha'].unique()]
            fechas = sorted(fechas)
            
            batches = []
            batch_index = 0
            
            for i in range(0, len(fechas), batch_size):
                batch_fechas = fechas[i:i + batch_size]
                
                batches.append({
                    'batch_index': batch_index,
                    'fechas': batch_fechas
                })
                
                batch_index += 1
            
            print(f"[TASK] Created {len(batches)} batches from all silver dates")
            return batches
            
        except Exception as e2:
            print(f"[TASK] Error getting date batches: {e2}")
            import traceback
            traceback.print_exc()
            return []


@task
def GOLD_typical_day_process_batch(date_batch: Dict[str, Any], **context) -> Dict:
    """
    Processes a batch of dates and inserts them into gold_typical_day_od_hourly.
    Each batch is processed in parallel using dynamic task mapping.
    
    NOTE: Uses INSERT INTO instead of MERGE INTO due to DuckLake bug
    with partitioned tables using year()/month()/day() functions + MERGE INTO.
    
    Parameters:
    - date_batch: Dict with 'fechas' (list of dates) and 'batch_index'
    
    Returns:
    - Dict with status and metadata of the processed batch
    """
    # Handle both dict and direct list cases
    if isinstance(date_batch, dict):
        fechas = date_batch.get('fechas', [])
        batch_index = date_batch.get('batch_index', 0)
    elif isinstance(date_batch, list):
        fechas = date_batch
        batch_index = 0
    else:
        raise ValueError(f"Unexpected date_batch type: {type(date_batch)}, value: {date_batch}")
    
    if not fechas:
        raise ValueError(f"No fechas found in date_batch: {date_batch}")
    
    print(f"[TASK] Processing batch {batch_index}: {len(fechas)} dates (from {fechas[0]} to {fechas[-1]})")
    
    # Create list of dates for the query (format: YYYY-MM-DD)
    fechas_formatted = [f"{f[:4]}-{f[4:6]}-{f[6:8]}" for f in fechas]
    fechas_str = "', '".join(fechas_formatted)
    
    # INSERT INTO (incremental processing)
    sql_query = f"""
        SET preserve_insertion_order=false;
        SET enable_object_cache=true;
        
        INSERT INTO gold_typical_day_od_hourly (origin_id, destination_id, date, hour, avg_trips, avg_km)
        WITH base AS (
            SELECT
                origen_zone_id,
                destino_zone_id,
                EXTRACT(HOUR FROM fecha) AS hour,
                viajes,
                viajes_km,
                DATE(fecha) AS date
            FROM silver_mitma_od
            WHERE DATE(fecha) IN ('{fechas_str}')
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
    
    result = execute_sql_or_cloud_run(sql_query=sql_query, **context)
    
    print(f"[TASK] Batch {batch_index} processed successfully: {len(fechas)} dates")
    
    return {
        "status": "success",
        "batch": date_batch,
        "batch_index": batch_index,
        "execution_name": result.get("execution_name"),
        "execution_time_seconds": result.get("execution_time_seconds")
    }
