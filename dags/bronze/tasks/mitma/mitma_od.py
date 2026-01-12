"""
Airflow task for loading MITMA OD (Origin-Destination) matrices into Bronze layer.
Handles viajes (trips) data for distritos, municipios, and GAU zone types.
Uses dynamic task mapping to process urls in parallel.

V2 Features:
- Batch download with parallel processing
- Streaming downloads with retry and exponential backoff
- Partitioned tables by year/month/day
- Optimized bulk INSERT
"""

from airflow.sdk import task
from typing import List, Dict, Any


@task
def BRONZE_mitma_od_urls(zone_type: str = 'distritos', start_date: str = None, end_date: str = None):
    """
    Generate the list of URLs for MITMA OD data.
    """
    from bronze.utils import get_mitma_urls

    dataset = 'od'
    urls = get_mitma_urls(dataset, zone_type, start_date, end_date)
    print(f"[TASK] {len(urls)} URLs found for {dataset} {zone_type}")
    return urls


# ============================================================================
# V2: NEW TASKS FOR OPTIMIZED BATCH PROCESSING
# ============================================================================

@task
def BRONZE_mitma_od_get_and_filter_urls(
    zone_type: str = 'municipios',
    start_date: str = None,
    end_date: str = None
) -> List[str]:
    """
    V2: Combined task that gets URLs and filters already ingested ones.
    More efficient than separate tasks.
    """
    from bronze.utils import get_mitma_urls, mitma_filter_urls
    
    dataset = 'od'
    
    print(f"[TASK_V2] Getting OD URLs for {zone_type} from {start_date} to {end_date}")
    urls = get_mitma_urls(dataset, zone_type, start_date, end_date)
    
    if not urls:
        print(f"[TASK_V2] No URLs found")
        return []
    
    print(f"[TASK_V2] Found {len(urls)} total URLs, filtering...")
    filtered = mitma_filter_urls(dataset, zone_type, urls)
    
    print(f"[TASK_V2] {len(filtered)} new URLs to process")
    return filtered


@task
def BRONZE_mitma_od_create_url_batches(
    urls: List[str],
    batch_size: int = 10
) -> List[Dict[str, Any]]:
    """
    V2: Creates batches of URLs for parallel processing.
    """
    from bronze.utils import create_url_batches
    
    if not urls:
        return []
    
    batches = create_url_batches(urls, batch_size)
    
    return [
        {'batch_index': i, 'urls': batch}
        for i, batch in enumerate(batches)
    ]


@task
def BRONZE_mitma_od_create_partitioned_table(
    urls: List[str],
    zone_type: str = 'municipios'
) -> Dict[str, Any]:
    """
    V3: Creates a partitioned table for OD data with fecha as TIMESTAMP.
    Partitions by year/month/day of the fecha TIMESTAMP column (like silver).
    """
    from bronze.utils import create_partitioned_table_from_csv
    
    if not urls:
        return {'status': 'skipped', 'reason': 'no_urls'}
    
    dataset = 'od'
    table_name = f'mitma_{dataset}_{zone_type}'
    
    print(f"[TASK_V3] Creating partitioned table bronze_{table_name} with fecha as TIMESTAMP")
    return create_partitioned_table_from_csv(
        table_name=table_name,
        url=urls[0],
        partition_by_date=True,
        fecha_as_timestamp=True
    )


@task
def BRONZE_mitma_od_download_batch(
    batch: Dict[str, Any],
    zone_type: str = 'municipios',
    max_parallel: int = 4
) -> Dict[str, Any]:
    """
    V2: Downloads a batch of URLs to RustFS with parallel processing.
    Includes retry with exponential backoff.
    """
    from bronze.utils import download_batch_to_rustfs
    
    dataset = 'od'
    urls = batch.get('urls', [])
    batch_index = batch.get('batch_index', 0)
    
    if not urls:
        return {
            'batch_index': batch_index,
            'downloaded': [],
            'failed': []
        }
    
    print(f"[TASK_V2] Downloading batch {batch_index}: {len(urls)} URLs")
    
    results = download_batch_to_rustfs(urls, dataset, zone_type, max_parallel)
    
    downloaded = [
        {'original_url': url, 's3_path': s3_path}
        for url, s3_path in results.items()
    ]
    
    failed = [url for url in urls if url not in results]
    
    return {
        'batch_index': batch_index,
        'downloaded': downloaded,
        'failed': failed
    }


@task
def BRONZE_mitma_od_process_batch(
    download_result: Dict[str, Any],
    zone_type: str = 'municipios',
    threads: int = 4,
    **context
) -> Dict[str, Any]:
    """
    V3: Processes downloaded files from RustFS into DuckDB using COPY (INSERT INTO) with multi-threading.
    Uses executor (Cloud Run) instead of ingestor - builds complete SQL query and passes it to executor.
    Parses fecha from VARCHAR to TIMESTAMP and uses read_csv with multiple URLs for faster processing.
    Faster than MERGE because it uses INSERT INTO instead of MERGE.
    """
    from bronze.utils import copy_from_csv_batch
    
    dataset = 'od'
    table_name = f'mitma_{dataset}_{zone_type}'
    
    print(f"[TASK_V3] Processing batch {download_result.get('batch_index', 0)} with {threads} threads using executor")
    print(f"[TASK_V3] Will parse fecha to TIMESTAMP")
    
    result = copy_from_csv_batch(
        table_name=table_name,
        batch=download_result,
        threads=threads,
        fecha_as_timestamp=True,
        **context
    )
    
    return result


@task
def BRONZE_mitma_od_download_and_process_batch(
    batch: Dict[str, Any],
    zone_type: str = 'municipios',
    threads: int = 4,
    **context
) -> Dict[str, Any]:
    """
    V5: Processes batch directly from HTTP URLs without downloading.
    DuckDB with httpfs can read directly from HTTP URLs, eliminating the download step.
    This is more efficient: faster, less storage, less code.
    
    Parameters:
    - batch: Dict with 'urls' list and 'batch_index'
    - zone_type: Zone type (municipios, distritos, etc.)
    - threads: Number of threads for DuckDB processing (default: 4)
    - **context: Airflow context
    """
    from bronze.utils import copy_batch_to_table
    
    dataset = 'od'
    table_name = f'mitma_{dataset}_{zone_type}'
    urls = batch.get('urls', [])
    batch_index = batch.get('batch_index', 0)
    
    if not urls:
        return {
            'batch_index': batch_index,
            'status': 'skipped',
            'processed': 0
        }
    
    print(f"[TASK_V5] Processing batch {batch_index}: {len(urls)} URLs directly from HTTP (no download)")
    
    # Process batch directly from HTTP URLs using executor (Cloud Run)
    # DuckDB with httpfs reads directly from HTTP URLs
    process_result = copy_batch_to_table(
        table_name=table_name,
        urls=urls,
        threads=threads,
        fecha_as_timestamp=True,
        **context
    )
    
    processed_count = process_result.get('success', 0)
    print(f"[TASK_V5] Batch {batch_index} complete: {processed_count} URLs processed successfully")
    
    return {
        'batch_index': batch_index,
        'status': 'success' if process_result.get('failed', 0) == 0 else 'partial',
        'processed': processed_count,
        'failed': process_result.get('failed', 0),
        'errors': process_result.get('errors', [])
    }


@task
def BRONZE_mitma_od_download_all_batches_then_process(
    batches: List[Dict[str, Any]],
    zone_type: str = 'municipios',
    threads: int = 4,
    batch_size: int = 10,
    **context
) -> Dict[str, Any]:
    """
    V6: Descarga todos los batches secuencialmente (uno después de otro),
    y luego ejecuta Cloud Run por batches configurable para procesar los archivos descargados.
    
    Parameters:
    - batches: Lista de diccionarios con 'urls' y 'batch_index'
    - zone_type: Zone type (municipios, distritos, etc.)
    - threads: Number of threads for DuckDB processing (default: 4)
    - batch_size: Número de archivos a procesar por batch en Cloud Run (default: 10)
    - **context: Airflow context
    """
    from bronze.utils import download_batch_to_rustfs, copy_from_csv_batch
    
    dataset = 'od'
    table_name = f'mitma_{dataset}_{zone_type}'
    
    if not batches:
        return {
            'status': 'skipped',
            'processed': 0,
            'downloaded': 0
        }
    
    print(f"[TASK_V6] Downloading {len(batches)} batches sequentially")
    
    # 1. Descargar todos los batches secuencialmente
    all_downloaded = []
    
    for batch_idx, batch in enumerate(batches):
        urls = batch.get('urls', [])
        batch_index = batch.get('batch_index', batch_idx)
        
        if not urls:
            print(f"[TASK_V6] Batch {batch_index} is empty, skipping")
            continue
        
        print(f"[TASK_V6] Downloading batch {batch_index}: {len(urls)} URLs")
        
        # Descargar batch secuencialmente (sin paralelismo entre batches)
        # download_batch_to_rustfs usa paralelismo interno dentro del batch
        results = download_batch_to_rustfs(urls, dataset, zone_type, max_parallel=4)
        
        # Acumular resultados
        for url, s3_path in results.items():
            all_downloaded.append({'original_url': url, 's3_path': s3_path})
        
        print(f"[TASK_V6] Batch {batch_index} downloaded: {len(results)}/{len(urls)} URLs")
    
    print(f"[TASK_V6] All batches downloaded: {len(all_downloaded)} total files")
    
    if not all_downloaded:
        return {
            'status': 'skipped',
            'processed': 0,
            'downloaded': 0
        }
    
    # 2. Procesar archivos descargados con Cloud Run por batches
    print(f"[TASK_V6] Processing {len(all_downloaded)} files with Cloud Run (batch_size={batch_size})")
    
    total_processed = 0
    total_failed = 0
    all_errors = []
    
    # Dividir archivos descargados en batches para procesar
    for i in range(0, len(all_downloaded), batch_size):
        process_batch = all_downloaded[i:i + batch_size]
        process_batch_index = i // batch_size
        
        print(f"[TASK_V6] Processing Cloud Run batch {process_batch_index}: {len(process_batch)} files")
        
        # Crear batch para usar copy_from_csv_batch
        cloud_run_batch = {
            'batch_index': process_batch_index,
            'downloaded': process_batch
        }
        
        # Ejecutar Cloud Run para este batch
        process_result = copy_from_csv_batch(
            table_name=table_name,
            batch=cloud_run_batch,
            threads=threads,
            fecha_as_timestamp=True,
            **context
        )
        
        processed_count = process_result.get('processed', 0)
        failed_count = process_result.get('failed', 0)
        errors = process_result.get('errors', [])
        
        total_processed += processed_count
        total_failed += failed_count
        all_errors.extend(errors)
        
        print(f"[TASK_V6] Cloud Run batch {process_batch_index} complete: {processed_count} processed, {failed_count} failed")
    
    print(f"[TASK_V6] All processing complete: {total_processed}/{len(all_downloaded)} files processed successfully")
    
    return {
        'status': 'success' if total_failed == 0 else 'partial',
        'downloaded': len(all_downloaded),
        'processed': total_processed,
        'failed': total_failed,
        'errors': all_errors
    }


@task
def BRONZE_mitma_od_cleanup_batch(download_result: Dict[str, Any]) -> Dict[str, Any]:
    """
    V2: Cleans up files from RustFS after processing.
    """
    from bronze.utils import delete_batch_from_rustfs
    
    downloaded = download_result.get('downloaded', [])
    batch_index = download_result.get('batch_index', 0)
    
    if not downloaded:
        return {'batch_index': batch_index, 'deleted': 0}
    
    s3_paths = [item['s3_path'] for item in downloaded]
    
    print(f"[TASK_V2] Cleaning up batch {batch_index}: {len(s3_paths)} files")
    
    results = delete_batch_from_rustfs(s3_paths)
    deleted = sum(1 for v in results.values() if v)
    
    return {'batch_index': batch_index, 'deleted': deleted}


@task
def BRONZE_mitma_od_finalize(
    zone_type: str,
    process_results: List[Dict[str, Any]]
) -> Dict[str, Any]:
    """
    V2: Finalizes the table after all batches are processed.
    Runs ANALYZE once at the end for optimal performance.
    """
    from bronze.utils import finalize_table
    
    dataset = 'od'
    table_name = f'bronze_mitma_{dataset}_{zone_type}'
    
    total_processed = sum(r.get('processed', 0) for r in process_results if r)
    
    if total_processed == 0:
        return {'status': 'skipped', 'reason': 'no_records'}
    
    print(f"[TASK_V2] Finalizing {table_name} ({total_processed} records processed)")
    
    return finalize_table(table_name, run_analyze=True)


@task
def BRONZE_mitma_od_create_table(urls: list[str], zone_type: str = 'distritos'):
    """
    Create the table for MITMA OD data if it doesn't exist.
    This is a pre-task that runs once before the dynamic insertion.
    Takes the first URL from the list to create the table schema.
    """
    from bronze.utils import mitma_create_table

    dataset = 'od'
    return mitma_create_table(dataset, zone_type, urls)


@task
def BRONZE_mitma_od_filter_urls(urls: list[str], zone_type: str = 'distritos'):
    """
    Filter URLs to only include those not already ingested.
    Queries the bronze table for existing source_file values and returns only new URLs.
    """
    from bronze.utils import mitma_filter_urls

    dataset = 'od'
    return mitma_filter_urls(dataset, zone_type, urls)


@task
def BRONZE_mitma_od_process(url: str, zone_type: str = 'distritos'):
    """
    Download a MITMA URL, upload it to RustFS, and process it using Cloud Run Job.
    This combines download and insert operations to process each URL as soon as it's downloaded,
    avoiding the need to wait for all downloads to complete before starting processing.
    
    The Cloud Run Job will read the CSV from RustFS S3 bucket and merge it into DuckDB.
    After successful merge, deletes the file from RustFS to free up space.
    
    Parameters:
    - url: URL to download
    - zone_type: Type of zone ('distritos', 'municipios', 'gau')
    
    Returns:
    - Dict with processing results
    """
    from utils.gcp import merge_csv_or_cloud_run
    from bronze.utils import download_url_to_rustfs, delete_file_from_rustfs

    dataset = 'od'
    table_name = f'bronze_mitma_{dataset}_{zone_type}'
    
    print(f"[TASK] Downloading and uploading to RustFS: {url}")
    
    # Descargar y subir a RustFS
    s3_path = download_url_to_rustfs(url, dataset, zone_type)
    
    print(f"[TASK] File uploaded to RustFS: {s3_path}")
    print(f"[TASK] Processing S3 path: {s3_path} into {table_name}")

    # Ejecutar merge (Cloud Run o local según disponibilidad)
    result = merge_csv_or_cloud_run(
        table_name=table_name, 
        url=s3_path, 
        original_url=url
    )
    
    # Si el job completó exitosamente, eliminar el archivo de RustFS
    print(f"[TASK] Processing completed successfully, deleting file from RustFS: {s3_path}")
    delete_success = delete_file_from_rustfs(s3_path)
    
    if delete_success:
        print(f"[TASK] File deleted from RustFS successfully")
    else:
        print(f"[TASK] Failed to delete file from RustFS (continuing anyway)")

    return {
        'status': 'success',
        's3_path': s3_path,
        'original_url': url,
        'cloud_run_job_result': result,
        'table_name': table_name,
        'file_deleted': delete_success
    }


@task
def BRONZE_mitma_od_check_dates(
    zone_type: str = 'municipios',
    start_date: str = None,
    end_date: str = None,
    **context
) -> Dict[str, Any]:
    """
    Comprueba las fechas insertadas en bronze haciendo un SELECT DISTINCT
    sobre la fecha, filtrando en el rango entre start_date y end_date.
    Calcula las fechas faltantes y genera las URLs solo para esas fechas.
    """
    from utils.utils import get_ducklake_connection
    from bronze.utils import get_mitma_urls
    import re
    
    table_name = f'bronze_mitma_od_{zone_type}'
    
    # Si start_date y end_date vienen de params, usarlos
    if not start_date:
        start_date = context.get('params', {}).get('start')
    if not end_date:
        end_date = context.get('params', {}).get('end')
    
    if not start_date or not end_date:
        print("[TASK] start_date y end_date son requeridos")
        return {'fechas_bronze': [], 'fechas_faltantes': [], 'urls_faltantes': []}
    
    print(f"[TASK] Checking dates in {table_name} from {start_date} to {end_date}")
    
    # Convertir fechas al formato YYYYMMDD para la comparación
    start_date_yyyymmdd = start_date.replace('-', '')
    end_date_yyyymmdd = end_date.replace('-', '')
    
    # 1. Obtener fechas insertadas en bronze en el rango
    sql_query = f"""
        SELECT DISTINCT 
            CAST(fecha AS VARCHAR) as fecha
        FROM {table_name}
        WHERE fecha IS NOT NULL
            AND CAST(fecha AS VARCHAR) >= '{start_date_yyyymmdd}'
            AND CAST(fecha AS VARCHAR) <= '{end_date_yyyymmdd}'
        ORDER BY fecha
    """
    
    con = get_ducklake_connection()
    df_bronze = con.execute(sql_query).fetchdf()
    
    fechas_bronze = [str(fecha) for fecha in df_bronze['fecha'].unique()] if not df_bronze.empty else []
    fechas_bronze_set = set(fechas_bronze)
    print(f"[TASK] Fechas insertadas en bronze en el rango: {len(fechas_bronze)}")
    
    # 2. Obtener todas las URLs disponibles y extraer fechas
    dataset = 'od'
    urls_disponibles = get_mitma_urls(dataset, zone_type, start_date, end_date)
    
    # Crear diccionario fecha -> URL para poder filtrar después
    pattern = r'(\d{8})'
    fecha_to_url = {}
    for url in urls_disponibles:
        match = re.search(pattern, url)
        if match:
            fecha_str = match.group(1)
            if start_date_yyyymmdd <= fecha_str <= end_date_yyyymmdd:
                fecha_to_url[fecha_str] = url
    
    fechas_disponibles_set = set(fecha_to_url.keys())
    print(f"[TASK] Fechas disponibles según URLs: {len(fechas_disponibles_set)}")
    
    # 3. Calcular fechas faltantes (disponibles pero no en bronze)
    fechas_faltantes = sorted(list(fechas_disponibles_set - fechas_bronze_set))
    print(f"[TASK] Fechas faltantes por ingestar: {len(fechas_faltantes)}")
    
    # 4. Generar URLs solo para las fechas faltantes
    urls_faltantes = [fecha_to_url[fecha] for fecha in fechas_faltantes if fecha in fecha_to_url]
    print(f"[TASK] URLs para fechas faltantes: {len(urls_faltantes)}")
    
    return {
        'fechas_bronze': fechas_bronze,
        'fechas_faltantes': fechas_faltantes,
        'urls_faltantes': urls_faltantes
    }
