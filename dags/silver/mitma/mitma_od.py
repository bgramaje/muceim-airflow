"""
Airflow task for building the unified MITMA OD Silver table.
Includes:
- Type casting
- Weekend / holiday flags
- Null filtering of critical fields
- zone_level standardization
- Batch processing with dynamic task mapping for large datasets
- Idempotent processing using MERGE and date tracking

Ejemplo de uso con dynamic task mapping en un DAG:

    from dags.silver.mitma.mitma_od import (
        SILVER_mitma_od_get_date_batches,
        SILVER_mitma_od_create_table,
        SILVER_mitma_od_process_batch
    )
    
    # 1. Crear tablas si no existen (idempotente)
    create_table = SILVER_mitma_od_create_table()
    
    # 2. Obtener batches de fechas no procesadas (idempotente)
    date_batches = SILVER_mitma_od_get_date_batches(
        batch_size=30  # Procesar 30 fechas por batch
    )
    
    # 3. Procesar cada batch en paralelo usando MERGE (suma valores cuando coinciden las claves)
    batch_results = (
        SILVER_mitma_od_process_batch
        .expand(date_batch=date_batches)
    )
    
    # Definir dependencias
    create_table >> date_batches >> batch_results
"""

from airflow.sdk import task
from typing import Any
from typing import List, Dict, Any

from utils.gcp import execute_sql_or_cloud_run
from utils.utils import get_ducklake_connection


@task
def SILVER_mitma_od_get_date_batches(batch_size: int = 30, **context) -> List[Dict[str, Any]]:
    """
    Obtiene las fechas únicas de la tabla bronze excluyendo las ya procesadas,
    y las divide en batches de tamaño fijo.
    Hace el proceso idempotente al solo procesar fechas nuevas.
    
    Parameters:
    - batch_size: Número de fechas por batch (default: 30)
    
    Returns:
    - Lista de diccionarios con 'fechas' (lista de fechas) y 'batch_index' para cada batch
    """
    print(f"[TASK] Getting unprocessed dates from bronze_mitma_od_municipios (batch_size: {batch_size})")
    
    con = get_ducklake_connection()
    
    query = """
        SELECT DISTINCT 
            b.fecha
        FROM bronze_mitma_od_municipios b
        WHERE b.fecha IS NOT NULL
            AND CAST(b.fecha AS VARCHAR) NOT IN (
                SELECT fecha 
                FROM silver_od_processed_dates
                WHERE fecha IS NOT NULL
            )
        ORDER BY b.fecha
    """
    
    df = con.execute(query).fetchdf()
    
    if df.empty:
        print("[TASK] No unprocessed dates found in bronze table")
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
    for i, batch in enumerate(batches, 1):
        print(f"[TASK]   Batch {i}: {len(batch['fechas'])} dates (from {batch['fechas'][0]} to {batch['fechas'][-1]})")
    
    if batches:
        print(f"[TASK] DEBUG: First batch structure: {batches[0]}")
        print(f"[TASK] DEBUG: First batch type: {type(batches[0])}")
        print(f"[TASK] DEBUG: First batch keys: {batches[0].keys() if isinstance(batches[0], dict) else 'not a dict'}")
    
    return batches


@task.branch
def SILVER_mitma_od_check_batches(date_batches: list[dict], **context) -> Any:
    """
    Branch task que determina si hay batches para procesar o no.
    
    Si hay batches (lista no vacía), retorna el task_id de process_batch para ejecutarlo.
    Si no hay batches (lista vacía), retorna el task_id para ejecutar batches_skipped.
    
    Args:
        date_batches: Lista de batches retornada por SILVER_mitma_od_get_date_batches
        
    Returns:
        'mitma_od_batches.process_batch' si hay batches (para ejecutar el dynamic task mapping),
        'mitma_od_batches.batches_skipped' si no hay batches
    """
    if not date_batches or len(date_batches) == 0:
        print("[TASK] No batches to process, will execute batches_skipped")
        # Retornar el task_id completo con el prefijo del TaskGroup
        return "mitma_od_batches.batches_skipped"
    else:
        print(f"[TASK] Found {len(date_batches)} batches to process")
        # Retornar el task_id de process_batch para ejecutar el dynamic task mapping
        return "mitma_od_batches.process_batch"


@task
def SILVER_mitma_od_create_table(**context) -> Dict:
    """
    Crea las tablas silver_od y silver_od_processed_dates si no existen.
    Esta tarea debe ejecutarse antes de procesar los batches.
    Hace el proceso idempotente al no reemplazar tablas existentes.
    
    Returns:
    - Dict con status de la creación de las tablas
    """
    print("[TASK] Creating silver_od and silver_od_processed_dates tables if they don't exist")
    
    con = get_ducklake_connection()
    
    sql_query = """
        -- DuckDB optimizations for large datasets
        SET preserve_insertion_order=false;
        SET enable_object_cache=true;
        
        -- Crear tabla silver_od si no existe (idempotente)
        CREATE TABLE IF NOT EXISTS silver_od (
            fecha TIMESTAMP,
            origen_zone_id VARCHAR,
            destino_zone_id VARCHAR,
            viajes DOUBLE,
            viajes_km DOUBLE,
            residencia VARCHAR
        );
        
        -- Crear tabla de tracking de fechas procesadas (idempotente)
        CREATE TABLE IF NOT EXISTS silver_od_processed_dates (
            fecha VARCHAR
        );
    """
    
    con.execute(sql_query)
    
    print("[TASK] Tables created/verified successfully")
    
    return {
        "status": "success",
        "table": "silver_od"
    }


@task
def SILVER_mitma_od_process_batch(date_batch: Dict[str, Any], **context) -> Dict:
    """
    Procesa un batch de fechas de la tabla bronze usando MERGE para hacer upsert en silver_od.
    Suma los valores de viajes y viajes_km cuando coinciden fecha, origen, destino y residencia,
    permitiendo agregar registros con diferentes segmentos demográficos (edad, renta, etc.).
    Cada batch se procesa en paralelo usando dynamic task mapping.
    Después de procesar, registra las fechas procesadas en la tabla de tracking.
    
    Parameters:
    - date_batch: Dict con 'fechas' (lista de fechas) y 'batch_index'
    
    Returns:
    - Dict con status y metadata del batch procesado
    """
    # Debug: print what we're receiving
    print(f"[TASK] DEBUG: date_batch type: {type(date_batch)}")
    print(f"[TASK] DEBUG: date_batch content: {date_batch}")
    
    # Handle both dict and direct list cases
    if isinstance(date_batch, dict):
        fechas = date_batch.get('fechas', [])
        batch_index = date_batch.get('batch_index', 0)
    elif isinstance(date_batch, list):
        # If it's a list directly, use it as fechas
        fechas = date_batch
        batch_index = 0
    else:
        raise ValueError(f"Unexpected date_batch type: {type(date_batch)}, value: {date_batch}")
    
    if not fechas:
        raise ValueError(f"No fechas found in date_batch: {date_batch}")
        
    print(f"[TASK] Processing batch {batch_index}: {len(fechas)} dates (from {fechas[0]} to {fechas[-1]})")
    
    # Query optimizada para el batch específico usando MERGE (idempotente)
    sql_query = f"""
        -- DuckDB optimizations for large datasets
        SET preserve_insertion_order=false;
        SET enable_object_cache=true;
        
        -- MERGE datos del batch en silver_od (suma valores cuando coinciden las claves)
        MERGE INTO silver_od AS target
        USING (
            WITH base AS (
                SELECT
                    strptime(fecha::VARCHAR || LPAD(periodo::VARCHAR, 2, '0'), '%Y%m%d%H') as fecha,
                    origen AS origen_zone_id,
                    destino AS destino_zone_id,
                    CAST(viajes AS DOUBLE) AS viajes,
                    CAST(viajes_km AS DOUBLE) AS viajes_km,
                    residencia
                FROM bronze_mitma_od_municipios
                WHERE 
                    -- Filtrado por lista de fechas del batch
                    fecha IN ('{"', '".join(str(f) for f in fechas)}')
                    -- Early filtering: filter invalid data before expensive transformations
                    AND fecha IS NOT NULL
                    AND periodo IS NOT NULL
                    AND origen IS NOT NULL
                    AND origen != 'externo'
                    AND destino IS NOT NULL
                    AND destino != 'externo'
                    AND viajes IS NOT NULL
                    AND viajes_km IS NOT NULL
                    AND residencia IS NOT NULL
            )
            -- Agrupar por clave única y sumar viajes y viajes_km
            -- Esto permite agregar registros con misma fecha/origen/destino/residencia
            -- pero diferentes segmentos demográficos (edad, renta, etc.)
            SELECT
                fecha,
                origen_zone_id,
                destino_zone_id,
                residencia,
                SUM(viajes) AS viajes,
                SUM(viajes_km) AS viajes_km
            FROM base
            GROUP BY fecha, origen_zone_id, destino_zone_id, residencia
        ) AS source
        ON target.fecha = source.fecha
            AND target.origen_zone_id = source.origen_zone_id
            AND target.destino_zone_id = source.destino_zone_id
            AND target.residencia = source.residencia
        WHEN MATCHED THEN
            -- Sumar los valores nuevos a los existentes (no reemplazar)
            UPDATE SET
                viajes = target.viajes + source.viajes,
                viajes_km = target.viajes_km + source.viajes_km
        WHEN NOT MATCHED THEN
            INSERT (fecha, origen_zone_id, destino_zone_id, viajes, viajes_km, residencia)
            VALUES (source.fecha, source.origen_zone_id, source.destino_zone_id, source.viajes, source.viajes_km, source.residencia);
        
        -- Registrar fechas procesadas directamente desde el batch (optimizado, sin consultar bronze)
        MERGE INTO silver_od_processed_dates AS target
        USING (
            SELECT fecha::VARCHAR AS fecha
            FROM (VALUES ({"), (".join(f"'{str(f)}'" for f in fechas)}))
            AS t(fecha)
        ) AS source
        ON target.fecha = source.fecha
        WHEN NOT MATCHED THEN
            INSERT (fecha) VALUES (source.fecha);
    """
    
    result = execute_sql_or_cloud_run(sql_query=sql_query, **context)
    
    print(f"[TASK] Batch {batch_index} processed successfully: {len(fechas)} dates (from {fechas[0]} to {fechas[-1]})")
    
    return {
        "status": "success",
        "batch": date_batch,
        "batch_index": batch_index,
        "execution_name": result.get("execution_name"),
        "execution_time_seconds": result.get("execution_time_seconds")
    }
