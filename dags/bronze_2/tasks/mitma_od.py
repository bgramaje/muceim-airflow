"""
Bronze 2 tasks for MITMA OD ingestion using DuckLake executor.
Uses INSERT instead of MERGE for better performance and easier date tracking.
"""

import re
import os
import sys
from typing import List, Dict, Any, Optional
from datetime import datetime, timedelta

from airflow.sdk import task
from utils.gcp import execute_sql_or_cloud_run
from utils.utils import get_ducklake_connection

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from bronze.utils import get_mitma_urls


def extract_date_from_url(url: str) -> Optional[str]:
    """
    Extrae la fecha del nombre de archivo de una URL MITMA.
    Formato esperado: YYYYMMDD_{FilePattern}.csv.gz
    
    Returns:
    - Fecha en formato YYYYMMDD o None si no se puede extraer
    """
    try:
        # Extraer nombre de archivo de la URL
        filename = os.path.basename(url)
        
        # Buscar patrón YYYYMMDD al inicio del nombre de archivo
        match = re.match(r'^(\d{8})_', filename)
        if match:
            return match.group(1)
        
        return None
    except Exception as e:
        print(f"[EXTRACT_DATE] Error extracting date from URL {url}: {e}")
        return None


@task
def get_and_filter_urls(
    dataset: str = None,
    zone_type: str = None,
    start_date: str = None,
    end_date: str = None
) -> List[str]:
    """
    Obtiene URLs de MITMA y las filtra para solo incluir las que no han sido ingestadas.
    """
    if not all([dataset, zone_type, start_date, end_date]):
        raise ValueError("All parameters (dataset, zone_type, start_date, end_date) are required")
    
    print(f"[TASK] Getting URLs for {dataset} {zone_type} from {start_date} to {end_date}")
    
    urls = get_mitma_urls(dataset, zone_type, start_date, end_date)
    print(f"[TASK] Found {len(urls)} total URLs")
    
    if not urls:
        return []
    
    # Filtrar URLs por fechas ya ingestadas
    filtered = filter_urls_by_ingested_dates(
        table_name=f'bronze_mitma_{dataset}_{zone_type}',
        urls=urls,
        start_date=start_date,
        end_date=end_date
    )
    
    print(f"[TASK] After filtering: {len(filtered)} new URLs to process")
    
    return filtered


def filter_urls_by_ingested_dates(
    table_name: str,
    urls: List[str],
    start_date: str,
    end_date: str
) -> List[str]:
    """
    Filtra URLs excluyendo las fechas que ya están ingestadas en la tabla.
    Usa SELECT DISTINCT sobre la columna fecha (timestamp) para obtener fechas ya procesadas.
    """
    con = get_ducklake_connection()
    
    # Verificar si la tabla existe
    try:
        result = con.execute(f"""
            SELECT COUNT(*) FROM information_schema.tables 
            WHERE table_name = '{table_name}'
        """).fetchone()
        table_exists = result[0] > 0
    except:
        table_exists = False
    
    if not table_exists:
        print(f"[FILTER] Table {table_name} does not exist, returning all URLs")
        return urls
    
    # Obtener fechas ya ingestadas (formato YYYYMMDD desde timestamp)
    try:
        ingested_dates_df = con.execute(f"""
            SELECT DISTINCT 
                strftime(fecha, '%Y%m%d') AS fecha_str
            FROM {table_name}
            WHERE fecha IS NOT NULL
        """).fetchdf()
        
        ingested_dates = set(ingested_dates_df['fecha_str'].astype(str).tolist())
        print(f"[FILTER] Found {len(ingested_dates)} already ingested dates")
    except Exception as e:
        print(f"[FILTER] Error getting ingested dates: {e}, returning all URLs")
        return urls
    
    # Filtrar URLs: solo incluir las que tienen fechas no ingestadas
    filtered_urls = []
    for url in urls:
        date_str = extract_date_from_url(url)
        if date_str and date_str not in ingested_dates:
            filtered_urls.append(url)
        elif not date_str:
            # Si no podemos extraer la fecha, incluimos la URL por seguridad
            print(f"[FILTER] Warning: Could not extract date from {url}, including it")
            filtered_urls.append(url)
    
    return filtered_urls


@task
def create_partitioned_table(
    dataset: str = None,
    zone_type: str = None,
    url: str = None
) -> Dict[str, Any]:
    """
    Crea la tabla Bronze particionada con fecha casteada a TIMESTAMP.
    Usa el executor ducklake para ejecutar SQL directamente.
    """
    if not all([dataset, zone_type]):
        raise ValueError("dataset and zone_type are required")
    
    table_name = f'bronze_mitma_{dataset}_{zone_type}'
    
    # Verificar si la tabla ya existe
    con = get_ducklake_connection()
    try:
        result = con.execute(f"""
            SELECT COUNT(*) FROM information_schema.tables 
            WHERE table_name = '{table_name}'
        """).fetchone()
        table_exists = result[0] > 0
    except:
        table_exists = False
    
    if table_exists:
        print(f"[TASK] Table {table_name} already exists, skipping creation")
        return {'status': 'exists', 'table_name': table_name}
    
    if not url:
        print(f"[TASK] No URL provided and table doesn't exist, cannot create table")
        return {'status': 'skipped', 'reason': 'no_url'}
    
    print(f"[TASK] Creating partitioned table {table_name} with fecha as TIMESTAMP")
    
    # SQL para crear la tabla con fecha casteada a TIMESTAMP y particionado
    # La fecha viene como VARCHAR en formato YYYYMMDD, la casteamos a TIMESTAMP
    create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {table_name} AS
        SELECT 
            * EXCLUDE (fecha, filename),
            -- Castear fecha VARCHAR(YYYYMMDD) a TIMESTAMP
            strptime(fecha, '%Y%m%d') AS fecha,
            CURRENT_TIMESTAMP AS loaded_at,
            filename AS source_file
        FROM read_csv(
            '{url}',
            filename = true,
            header = true,
            all_varchar = true
        )
        LIMIT 0;
    """
    
    # Ejecutar creación de tabla
    result = execute_sql_or_cloud_run(sql_query=create_table_sql)
    
    # Aplicar particionado si la tabla es nueva
    try:
        # Verificar si la tabla tiene datos (si no tiene, es nueva)
        check_sql = f"SELECT COUNT(*) FROM {table_name}"
        count_result = execute_sql_or_cloud_run(sql_query=check_sql)
        
        # Aplicar particionado por año/mes/día desde el timestamp
        partition_sql = f"""
            ALTER TABLE {table_name} 
            SET PARTITIONED BY (
                year(fecha)::INTEGER,
                month(fecha)::INTEGER,
                day(fecha)::INTEGER
            );
        """
        
        partition_result = execute_sql_or_cloud_run(sql_query=partition_sql)
        print(f"[TASK] Applied partitioning by year/month/day from TIMESTAMP")
    except Exception as e:
        print(f"[TASK] Warning: Could not apply partitioning (table may already exist): {e}")
    
    print(f"[TASK] Table {table_name} is ready")
    return {'status': 'created', 'table_name': table_name, 'partitioned': True}


@task
def get_ingested_dates(
    dataset: str = None,
    zone_type: str = None,
    start_date: str = None,
    end_date: str = None
) -> List[str]:
    """
    Obtiene las fechas ya ingestadas en la tabla usando SELECT DISTINCT.
    Retorna lista de fechas en formato YYYYMMDD.
    """
    if not all([dataset, zone_type, start_date, end_date]):
        raise ValueError("All parameters are required")
    
    table_name = f'bronze_mitma_{dataset}_{zone_type}'
    
    # Verificar si la tabla existe
    con = get_ducklake_connection()
    try:
        result = con.execute(f"""
            SELECT COUNT(*) FROM information_schema.tables 
            WHERE table_name = '{table_name}'
        """).fetchone()
        table_exists = result[0] > 0
    except:
        table_exists = False
    
    if not table_exists:
        print(f"[TASK] Table {table_name} does not exist, no dates ingested yet")
        return []
    
    # Obtener fechas únicas en formato YYYYMMDD
    sql_query = f"""
        SELECT DISTINCT 
            strftime(fecha, '%Y%m%d') AS fecha_str
        FROM {table_name}
        WHERE fecha IS NOT NULL
            AND fecha >= strptime('{start_date}', '%Y-%m-%d')
            AND fecha <= strptime('{end_date}', '%Y-%m-%d')
        ORDER BY fecha_str
    """
    
    try:
        df = con.execute(sql_query).fetchdf()
        dates = [str(d) for d in df['fecha_str'].tolist()]
        print(f"[TASK] Found {len(dates)} ingested dates in range {start_date} to {end_date}")
        return dates
    except Exception as e:
        print(f"[TASK] Error getting ingested dates: {e}")
        return []


@task
def filter_urls_by_dates(
    urls: List[str] = None,
    ingested_dates: List[str] = None
) -> List[str]:
    """
    Filtra URLs excluyendo las que tienen fechas ya ingestadas.
    """
    if urls is None:
        urls = []
    if ingested_dates is None:
        ingested_dates = []
    
    if not urls:
        return []
    
    ingested_set = set(ingested_dates)
    
    filtered = []
    for url in urls:
        date_str = extract_date_from_url(url)
        if date_str and date_str not in ingested_set:
            filtered.append(url)
        elif not date_str:
            # Si no podemos extraer la fecha, incluimos la URL por seguridad
            print(f"[FILTER] Warning: Could not extract date from {url}, including it")
            filtered.append(url)
    
    print(f"[TASK] Filtered {len(urls)} URLs to {len(filtered)} (excluding {len(ingested_dates)} dates)")
    return filtered


@task(
    retries=5,
    retry_delay=timedelta(seconds=60),
    retry_exponential_backoff=True,
    max_retry_delay=timedelta(minutes=10),
)
def process_batch_insert(
    batch: Dict[str, Any] = None,
    dataset: str = None,
    zone_type: str = None
) -> Dict[str, Any]:
    """
    Procesa un batch de URLs usando INSERT directo con el executor ducklake.
    Usa SQL directo en lugar del ingestor.
    """
    if batch is None:
        batch = {}
    if not all([dataset, zone_type]):
        raise ValueError("dataset and zone_type are required")
    
    urls = batch.get('urls', [])
    batch_index = batch.get('batch_index', 0)
    
    if not urls:
        return {
            'batch_index': batch_index,
            'status': 'skipped',
            'processed': 0
        }
    
    table_name = f'bronze_mitma_{dataset}_{zone_type}'
    processed = 0
    errors = []
    
    print(f"[TASK] Processing batch {batch_index} with {len(urls)} URLs using INSERT")
    
    for url in urls:
        try:
            # Construir SQL para INSERT directo desde HTTP URL
            # Casteamos fecha VARCHAR a TIMESTAMP y agregamos metadatos
            insert_sql = f"""
                SET http_keep_alive=false;
                SET custom_user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64)';
                INSERT INTO {table_name}
                SELECT 
                    * EXCLUDE (fecha, filename),
                    -- Castear fecha VARCHAR(YYYYMMDD) a TIMESTAMP
                    strptime(fecha, '%Y%m%d') AS fecha,
                    CURRENT_TIMESTAMP AS loaded_at,
                    '{url}' AS source_file
                FROM read_csv(
                    '{url}',
                    filename = true,
                    header = true,
                    auto_detect = false,
                    all_varchar = true,
                    compression='gzip'
                )
            """
            
            # Ejecutar INSERT usando executor ducklake
            result = execute_sql_or_cloud_run(sql_query=insert_sql)
            
            processed += 1
            print(f"[TASK] Processed {processed}/{len(urls)}: {os.path.basename(url)}")
        except Exception as e:
            print(f"[TASK] Error processing {url}: {e}")
            errors.append({'url': url, 'error': str(e)})
    
    print(f"[TASK] Batch {batch_index}: {processed}/{len(urls)} processed successfully")
    
    return {
        'batch_index': batch_index,
        'status': 'success' if not errors else 'partial',
        'processed': processed,
        'errors': errors
    }


@task
def create_url_batches(urls: List[str] = None, batch_size: Any = 5) -> List[Dict[str, Any]]:
    """
    Divide URLs en batches optimizados para Cloud Run.
    """
    if urls is None:
        urls = []
    
    batch_size = int(batch_size) if batch_size else 5
    
    if not urls:
        print("[TASK] No URLs to batch")
        return []
    
    batches = []
    for i in range(0, len(urls), batch_size):
        batch = urls[i:i + batch_size]
        batches.append({
            'batch_index': len(batches),
            'urls': batch
        })
    
    print(f"[TASK] Created {len(batches)} batches")
    return batches


@task
def finalize_table(
    dataset: str = None,
    zone_type: str = None,
    process_results: List[Dict[str, Any]] = None
) -> Dict[str, Any]:
    """
    Finaliza la tabla después de procesar todos los batches.
    Ejecuta ANALYZE una vez al final.
    """
    if not all([dataset, zone_type]):
        raise ValueError("dataset and zone_type are required")
    if process_results is None:
        process_results = []
    
    table_name = f'bronze_mitma_{dataset}_{zone_type}'
    
    # Contar total procesado
    total_processed = sum(r.get('processed', 0) for r in process_results if r)
    
    if total_processed == 0:
        print(f"[TASK] No records processed, skipping finalize")
        return {'status': 'skipped', 'reason': 'no_records'}
    
    print(f"[TASK] Finalizing {table_name} after {total_processed} records processed")
    
    # Ejecutar ANALYZE usando executor
    analyze_sql = f"ANALYZE {table_name};"
    execute_sql_or_cloud_run(sql_query=analyze_sql)
    
    # Obtener estadísticas
    try:
        count_sql = f"SELECT COUNT(*) FROM {table_name}"
        con = get_ducklake_connection()
        count = con.execute(count_sql).fetchone()[0]
        print(f"[TASK] Table {table_name} has {count:,} records")
        return {'status': 'success', 'table_name': table_name, 'record_count': count}
    except Exception as e:
        print(f"[TASK] Warning: Could not get record count: {e}")
        return {'status': 'success', 'table_name': table_name}

