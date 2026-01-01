"""
Airflow tasks for building the silver_od_quality table.
Includes batch processing with dynamic task mapping for large datasets.
Processes dates that are in silver_od but not yet processed in silver_od_quality.
"""

from airflow.sdk import task
from typing import List, Dict, Any

from utils.gcp import execute_cloud_run_job_sql
from utils.utils import get_ducklake_connection


@task
def SILVER_od_quality_get_date_batches(batch_size: int = 30, **context) -> List[Dict[str, Any]]:
    """
    Obtiene las fechas únicas de silver_od_processed_dates que aún no están en silver_od_quality,
    y las divide en batches. Usa la diferencia entre las fechas procesadas y las que ya están en quality.
    
    Parameters:
    - batch_size: Número de fechas por batch (default: 30)
    
    Returns:
    - Lista de diccionarios con 'fechas' (lista de fechas) y 'batch_index' para cada batch
    """
    print(f"[TASK] Getting unprocessed quality dates (batch_size: {batch_size})")
    
    con = get_ducklake_connection()
    
    # Obtener fechas que están en silver_od_processed_dates pero no en silver_od_quality
    query = """
        SELECT DISTINCT 
            pod.fecha
        FROM silver_od_processed_dates pod
        WHERE pod.fecha IS NOT NULL
            AND pod.fecha NOT IN (
                SELECT DISTINCT strftime(fecha, '%Y%m%d')
                FROM silver_od_quality
                WHERE fecha IS NOT NULL
            )
        ORDER BY pod.fecha
    """
    
    try:
        df = con.execute(query).fetchdf()
        
        if df.empty:
            print("[TASK] ⚠️ No unprocessed quality dates found")
            return []
        
        fechas = [str(fecha) for fecha in df['fecha'].unique()]
        fechas = sorted(fechas)
        
        if not fechas:
            print("[TASK] ⚠️ No valid unprocessed quality dates found")
            return []
        
        print(f"[TASK] Total unprocessed quality dates: {len(fechas)}")
        
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
        for i, batch in enumerate(batches, 1):
            print(f"[TASK]   Batch {i}: {len(batch['fechas'])} dates (from {batch['fechas'][0]} to {batch['fechas'][-1]})")
        
        return batches
        
    except Exception as e:
        print(f"[TASK] ❌ Error getting date batches: {e}")
        import traceback
        traceback.print_exc()
        return []


@task
def SILVER_od_quality_create_table(**context) -> Dict:
    """
    Crea la tabla silver_od_quality si no existe.
    Esta tarea debe ejecutarse antes de procesar los batches.
    Hace el proceso idempotente al no reemplazar tablas existentes.
    No necesita tabla de tracking separada, usa la diferencia con silver_od_processed_dates.
    
    Returns:
    - Dict con status de la creación de la tabla
    """
    print("[TASK] Creating silver_od_quality table if it doesn't exist")
    
    con = get_ducklake_connection()
    
    sql_query = """
        -- DuckDB optimizations for large datasets
        SET enable_progress_bar=false;
        SET preserve_insertion_order=false;
        SET default_null_order='nulls_last';
        SET enable_object_cache=true;
        
        -- Crear tabla silver_od_quality si no existe (idempotente)
        CREATE TABLE IF NOT EXISTS silver_od_quality (
            fecha TIMESTAMP,
            origen_zone_id VARCHAR,
            destino_zone_id VARCHAR,
            residencia VARCHAR,
            viajes_per_capita DOUBLE,
            km_per_capita DOUBLE,
            flag_negative_viajes BOOLEAN,
            flag_negative_viajes_km BOOLEAN,
            z_viajes_per_capita DOUBLE,
            flag_outlier_viajes_per_capita BOOLEAN
        );
    """
    
    con.execute(sql_query)
    
    print("[TASK] ✅ Tables created/verified successfully")
    
    return {
        "status": "success",
        "table": "silver_od_quality"
    }


@task
def SILVER_od_quality_process_batch(date_batch: Dict[str, Any], **context) -> Dict:
    """
    Procesa un batch de fechas de silver_od y hace MERGE en silver_od_quality.
    Calcula métricas de calidad y z-scores usando estadísticas globales.
    Cada batch se procesa en paralelo usando dynamic task mapping.
    
    Parameters:
    - date_batch: Dict con 'fechas' (lista de fechas) y 'batch_index'
    
    Returns:
    - Dict con status y metadata del batch procesado
    """
    print(f"[TASK] DEBUG: date_batch type: {type(date_batch)}")
    print(f"[TASK] DEBUG: date_batch content: {date_batch}")
    
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
    
    # Crear lista de fechas para la query
    fechas_str = "', '".join(str(f) for f in fechas)
    
    print(f"[TASK] Processing quality batch {batch_index}: {len(fechas)} dates (from {fechas[0]} to {fechas[-1]})")
    
    sql_query = f"""
        -- DuckDB optimizations for large datasets
        SET enable_progress_bar=false;
        SET preserve_insertion_order=false;
        SET default_null_order='nulls_last';
        SET enable_object_cache=true;
        
        -- Calcular estadísticas globales sobre todos los datos existentes en silver_od
        CREATE OR REPLACE TEMP TABLE _temp_global_stats AS
        WITH enriched AS (
            SELECT
                DATE(od.fecha)::VARCHAR AS fecha_str,
                od.origen_zone_id,
                od.destino_zone_id,
                od.residencia,
                od.viajes,
                od.viajes_km,
                ine.poblacion_total,
                CASE 
                    WHEN ine.poblacion_total > 0 THEN od.viajes / ine.poblacion_total
                    ELSE NULL
                END AS viajes_per_capita
            FROM silver_od od
            LEFT JOIN silver_ine_all ine
                ON od.origen_zone_id = ine.id
            WHERE 
                od.viajes IS NOT NULL
                AND od.viajes_km IS NOT NULL
        )
        SELECT 
            COALESCE(AVG(viajes_per_capita), 0.0) AS avg_viajes,
            COALESCE(NULLIF(STDDEV_SAMP(viajes_per_capita), 0), 1.0) AS stddev_viajes
        FROM enriched
        WHERE viajes_per_capita IS NOT NULL;
        
        -- MERGE datos del batch en silver_od_quality
        MERGE INTO silver_od_quality AS target
        USING (
            WITH enriched AS (
                SELECT
                    od.fecha,
                    od.origen_zone_id,
                    od.destino_zone_id,
                    od.residencia,
                    od.viajes,
                    od.viajes_km,
                    ine.poblacion_total,
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
                WHERE 
                    strftime(od.fecha, '%Y%m%d') IN ('{fechas_str}')
                    AND od.viajes IS NOT NULL
                    AND od.viajes_km IS NOT NULL
            ),
            with_zscore AS (
                SELECT
                    e.fecha,
                    e.origen_zone_id,
                    e.destino_zone_id,
                    e.residencia,
                    e.viajes_per_capita,
                    e.km_per_capita,
                    e.viajes < 0 AS flag_negative_viajes,
                    e.viajes_km < 0 AS flag_negative_viajes_km,
                    CASE 
                        WHEN e.viajes_per_capita IS NOT NULL AND s.stddev_viajes > 0
                        THEN (e.viajes_per_capita - s.avg_viajes) / s.stddev_viajes
                        ELSE NULL
                    END AS z_viajes_per_capita
                FROM enriched e
                CROSS JOIN _temp_global_stats s
            )
            SELECT
                fecha,
                origen_zone_id,
                destino_zone_id,
                residencia,
                viajes_per_capita,
                km_per_capita,
                flag_negative_viajes,
                flag_negative_viajes_km,
                z_viajes_per_capita,
                CASE 
                    WHEN z_viajes_per_capita IS NOT NULL 
                    THEN ABS(z_viajes_per_capita) > 3
                    ELSE FALSE
                END AS flag_outlier_viajes_per_capita
            FROM with_zscore
        ) AS source
        ON target.fecha = source.fecha
            AND target.origen_zone_id = source.origen_zone_id
            AND target.destino_zone_id = source.destino_zone_id
            AND target.residencia = source.residencia
        WHEN MATCHED THEN
            UPDATE SET
                viajes_per_capita = source.viajes_per_capita,
                km_per_capita = source.km_per_capita,
                flag_negative_viajes = source.flag_negative_viajes,
                flag_negative_viajes_km = source.flag_negative_viajes_km,
                z_viajes_per_capita = source.z_viajes_per_capita,
                flag_outlier_viajes_per_capita = source.flag_outlier_viajes_per_capita
        WHEN NOT MATCHED THEN
            INSERT (fecha, origen_zone_id, destino_zone_id, residencia, viajes_per_capita, km_per_capita, flag_negative_viajes, flag_negative_viajes_km, z_viajes_per_capita, flag_outlier_viajes_per_capita)
            VALUES (source.fecha, source.origen_zone_id, source.destino_zone_id, source.residencia, source.viajes_per_capita, source.km_per_capita, source.flag_negative_viajes, source.flag_negative_viajes_km, source.z_viajes_per_capita, source.flag_outlier_viajes_per_capita);
    """
    
    result = execute_cloud_run_job_sql(sql_query=sql_query, **context)
    
    print(f"[TASK] ✅ Quality batch {batch_index} processed successfully: {len(fechas)} dates (from {fechas[0]} to {fechas[-1]})")
    
    return {
        "status": "success",
        "batch": date_batch,
        "batch_index": batch_index,
        "execution_name": result.get("execution_name"),
        "execution_time_seconds": result.get("execution_time_seconds")
    }


