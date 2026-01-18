"""MITMA-specific utility functions for Bronze layer data ingestion."""

from utils.utils import get_ducklake_connection
import re
import urllib.request
import urllib.parse
import requests
import pandas as pd
import geopandas as gpd
import os
import io
import tempfile
import time
from shapely.validation import make_valid
from typing import List, Dict, Any


def download_url_to_file(url: str, file_path: str, timeout: int = 2000) -> int:
    """Downloads a file using streaming to disk with automatic retry."""
 
    chunk_size = 8 * 1024 * 1024

    req = urllib.request.Request(
        url, headers={"User-Agent": "MITMA-DuckLake-Loader"})

    total_size = 0
    last_progress_log = 0

    with urllib.request.urlopen(req, timeout=timeout) as response:
        content_length = response.headers.get('Content-Length')
        expected_size = int(content_length) if content_length else None

        with open(file_path, 'wb') as f:
            while True:
                chunk = response.read(chunk_size)
                if not chunk:
                    break
                f.write(chunk)
                total_size += len(chunk)

                if expected_size and total_size - last_progress_log >= 10 * 1024 * 1024:
                    progress = (total_size / expected_size) * 100
                    print(
                        f"Progress: {progress:.1f}% ({total_size:,} / {expected_size:,} bytes)")
                    last_progress_log = total_size

    print(f"Complete: {total_size:,} bytes")
    return total_size


def download_url_to_rustfs(url: str, dataset: str, zone_type: str, bucket: str) -> str:
    """Downloads a file to RustFS S3 using streaming (memory efficient).

    Args:
        url: URL to download from
        dataset: Dataset name (e.g., 'od')
        zone_type: Zone type (e.g., 'municipios', 'distritos', 'gau')
        bucket: S3 bucket name (from Airflow Variable)

    Returns:
        S3 path of the uploaded file
    """
    from airflow.providers.amazon.aws.hooks.s3 import S3Hook

    parsed_url = urllib.parse.urlparse(url)
    filename = os.path.basename(parsed_url.path)

    if not filename:
        raise ValueError(f"could not extract filename from URL: {url}")

    s3_key = f"{dataset}/{zone_type}/{filename}"
    s3_path = f"s3://{bucket}/{s3_key}"

    print(f"target S3 path: {s3_path}")

    s3_hook = S3Hook(aws_conn_id='rustfs_s3_conn')

    if s3_hook.check_for_key(s3_key, bucket_name=bucket):
        print(f"file already exists: {s3_path}")
        return s3_path

    temp_file = None
    try:
        with tempfile.NamedTemporaryFile(delete=False, suffix='.tmp') as tmp:
            temp_file = tmp.name

        print(f"starting streaming download to disk: {url}")
        total_bytes = download_url_to_file(url, temp_file)

        print(f"uploading {total_bytes:,} bytes to {s3_path}")
        s3_hook.load_file(
            filename=temp_file,
            key=s3_key,
            bucket_name=bucket,
            replace=True
        )
        print(f"successfully uploaded to {s3_path}")
    finally:
        if temp_file and os.path.exists(temp_file):
            try:
                os.unlink(temp_file)
            except Exception as e:
                print(
                    f"Warning: Could not delete temp file {temp_file}: {e}")

    return s3_path


def download_batch_to_rustfs(
    urls: List[str],
    dataset: str,
    zone_type: str,
    bucket: str
) -> Dict[str, str]:
    """Downloads multiple URLs sequentially to RustFS S3.

    Args:
        urls: List of URLs to download
        dataset: Dataset name (e.g., 'od')
        zone_type: Zone type (e.g., 'municipios', 'distritos', 'gau')
        bucket: S3 bucket name (from Airflow Variable)

    Returns:
        Dictionary mapping URLs to their S3 paths
    """
    results = {}
    failed = []

    print(
        f"Starting download of {len(urls)} URLs")

    for i, url in enumerate(urls, 1):
        try:
            s3_path = download_url_to_rustfs(url, dataset, zone_type, bucket)
            results[url] = s3_path
            print(
                f"[{i}/{len(urls)}] Success: {os.path.basename(url)}")
        except Exception as e:
            error_msg = str(e)
            print(
                f"[{i}/{len(urls)}] Failed: {os.path.basename(url)} - {error_msg}")
            failed.append((url, error_msg))

    print(
        f"Completed: {len(results)} success, {len(failed)} failed")

    return results


def delete_batch_from_rustfs(s3_paths: List[str], bucket: str) -> Dict[str, bool]:
    """Deletes multiple files from RustFS S3 in batch.

    Args:
        s3_paths: List of S3 paths to delete (format: s3://bucket/key)
        bucket: S3 bucket name (from Airflow Variable)

    Returns:
        Dictionary mapping S3 paths to deletion success status
    """
    from airflow.providers.amazon.aws.hooks.s3 import S3Hook

    results = {}
    s3_hook = S3Hook(aws_conn_id='rustfs_s3_conn')

    def _get_s3_key(s3_path: str) -> str:
        if not s3_path.startswith("s3://"):
            return None
        path_without_prefix = s3_path[5:]
        parts = path_without_prefix.split("/", 1)
        if len(parts) != 2 or parts[0] != bucket:
            return None
        return parts[1]

    print(f"deleting {len(s3_paths)} files from RustFS")

    s3_client = s3_hook.get_conn()

    for s3_path in s3_paths:
        try:
            s3_key = _get_s3_key(s3_path)
            
            if not s3_key:
                results[s3_path] = False
                continue

            if s3_hook.check_for_key(s3_key, bucket_name=bucket):
                s3_client.delete_object(Bucket=bucket, Key=s3_key)

            results[s3_path] = True
        except Exception as e:
            print(f"error deleting {s3_path}: {e}")
            results[s3_path] = False

    success_count = sum(1 for v in results.values() if v)
    print(
        f"completed: {success_count}/{len(s3_paths)} deleted successfully")

    return results


def create_table_from_csv(
    table_name: str,
    url: str,
    partition_by_date: bool = True,
    fecha_as_timestamp: bool = False
):
    """Creates a DuckDB table with optional partitioning by year/month/day."""
    con = get_ducklake_connection()

    print(f"Creating table {table_name}")

    source_sql = _get_csv_source_query([url], fecha_as_timestamp=fecha_as_timestamp)

    con.execute(f"""
        CREATE TABLE IF NOT EXISTS {table_name} AS
        {source_sql}
        LIMIT 0;
    """)

    if not partition_by_date:
        return {
            'status': 'created',
            'table_name': table_name,
            'partitioned': False,
            'fecha_as_timestamp': fecha_as_timestamp
        }

    partition_clause = ("SET PARTITIONED BY (year(fecha), month(fecha), day(fecha));"
    ) if fecha_as_timestamp else ("SET PARTITIONED BY (substr(fecha, 1, 4)::INTEGER, substr(fecha, 5, 2)::INTEGER, substr(fecha, 7, 2)::INTEGER);")
    
    try: 
        con.execute(f"""
            ALTER TABLE {table_name} 
            {partition_clause}
        """)
        print(f"Applied partitioning")
    except Exception as e:
        print(f"Warning: Could not apply partitioning (table may already be partitioned): {e}")

    return {
        'status': 'created',
        'table_name': table_name,
        'partitioned': partition_by_date,
        'fecha_as_timestamp': fecha_as_timestamp
    }


def create_url_batches(urls: List[str], batch_size: int = 2) -> List[List[str]]:
    """Divides a list of URLs into batches for parallel processing."""
    if not urls:
        return []

    batches = [urls[i:i + batch_size] for i in range(0, len(urls), batch_size)]
    
    return batches


def copy_batch_to_table(
    table_name: str,
    s3_paths: List[str],
    original_urls: List[str] = None,
    fecha_as_timestamp: bool = False,
    **context
) -> Dict[str, Any]:
    """Copies a batch of CSV files from S3/RustFS to DuckDB using INSERT INTO."""
    from utils.gcp import execute_sql_or_cloud_run

    if not s3_paths:
        return {'success': 0, 'failed': 0, 'errors': []}

    if original_urls is None:
        original_urls = s3_paths

    print(f"copying {len(s3_paths)} files from RustFS into {table_name}")

    union_parts = []
    for i, s3_path in enumerate(s3_paths):
        source_file_value = original_urls[i] if i < len(
            original_urls) else s3_path
        source_file_value_escaped = source_file_value.replace("'", "''")
        s3_path_escaped = s3_path.replace("'", "''")

        fecha_column = (
            "strptime(CAST(fecha AS VARCHAR), '%Y%m%d')::TIMESTAMP AS fecha,\n"
            if fecha_as_timestamp
            else ""
        )
        exclude_clause = (
            "* EXCLUDE (fecha, filename),"
            if fecha_as_timestamp
            else "* EXCLUDE (filename),"
        )

        union_parts.append(f"""
            SELECT 
                {fecha_column}{exclude_clause}
                CURRENT_TIMESTAMP AS loaded_at,
                '{source_file_value_escaped}' AS source_file
            FROM read_csv(
                '{s3_path_escaped}',
                filename = true,
                header = true,
                all_varchar = true
            )
        """)

    sql_query = f"""
        INSERT INTO {table_name}
        {' UNION ALL '.join(union_parts)};
    """

    try:
        result = execute_sql_or_cloud_run(sql_query=sql_query, **context)
        success_count = len(s3_paths)
        print(f"Successfully processed {len(s3_paths)} files")
        print(result)
    except Exception as e:
        error_msg = f"Error copying batch: {str(e)}"
        print(f"Batch insert failed: {error_msg}")
        return {
            'success': 0,
            'failed': len(s3_paths),
            'errors': [error_msg],
        }

    return {
        'success': success_count,
        'failed': 0,
        'errors': [],
    }


def copy_from_csv_batch(
    table_name: str,
    batch: Dict[str, Any],
    fecha_as_timestamp: bool = False,
    **context
) -> Dict[str, Any]:
    """Processes a batch of downloaded files using INSERT INTO with multi-threading."""
    downloaded = batch.get('downloaded', [])
    batch_index = batch.get('batch_index', 0)

    if not downloaded:
        return {
            'batch_index': batch_index,
            'status': 'skipped',
            'processed': 0,
            'failed': 0
        }

    s3_paths = [item['s3_path'] for item in downloaded]
    original_urls = [item.get('original_url', item['s3_path'])
                     for item in downloaded]

    print(f"Processing batch {batch_index}: {len(s3_paths)} files")

    result = copy_batch_to_table(
        table_name=table_name,
        s3_paths=s3_paths,
        original_urls=original_urls,
        fecha_as_timestamp=fecha_as_timestamp,
        **context
    )

    return {
        'batch_index': batch_index,
        'status': 'success' if result['failed'] == 0 else 'partial',
        'processed': result['success'],
        'failed': result['failed'],
        'errors': result['errors']
    }


def get_mitma_urls(dataset, zone_type, fechas: list[str]):
    """Fetches MITMA URLs from RSS feed filtered by dataset, zone type, and a set of dates.

    Parameters:
    - dataset: currently only 'od'
    - zone_type: 'distritos' | 'municipios' | 'gau'
    - fechas: list of dates to fetch, as 'YYYYMMDD' (or 'YYYY-MM-DD', which will be normalized)
    """
    rss_url = "https://movilidad-opendata.mitma.es/RSS.xml"

    dataset_map = {
        "od": ("viajes", "Viajes"),
    }

    if zone_type not in ["distritos", "municipios", "gau"]:
        raise ValueError(
            f"Invalid zone_type: {zone_type}. Must be 'distritos', 'municipios', or 'gau'.")
    if dataset not in dataset_map:
        raise ValueError(
            f"Invalid dataset: {dataset}. Must be one of {list(dataset_map.keys())}.")

    dataset_path, file_prefix = dataset_map[dataset]

    zone_suffix = "GAU" if zone_type == "gau" else zone_type
    file_pattern = f"{file_prefix}_{zone_suffix}"

    pattern = rf'(https?://[^\s"<>]*/estudios_basicos/por-{zone_type}/{dataset_path}/ficheros-diarios/\d{{4}}-\d{{2}}/(\d{{8}})_{file_pattern}\.csv\.gz)'

    req = urllib.request.Request(
        rss_url, headers={"User-Agent": "MITMA-DuckLake-Loader"})
    txt = urllib.request.urlopen(req).read().decode("utf-8", "ignore")

    matches = re.findall(pattern, txt, re.I)

    unique_matches = list(set(matches))

    if not fechas:
        print(f"Found 0 URLs for {dataset} {zone_type}: empty fechas list")
        return []

    fechas_set = {str(f).replace("-", "") for f in fechas if f}

    filtered_urls = []
    for url, date_str in unique_matches:
        if date_str in fechas_set:
            filtered_urls.append((url, date_str))

    filtered_urls.sort(key=lambda x: x[1])

    urls = [url for url, _ in filtered_urls]

    print(
        f"Found {len(urls)} URLs for {dataset} {zone_type} for {len(fechas_set)} requested dates")

    if not urls:
        print(f"WARNING: No URLs found. Check if data exists for the requested dates.")

    return urls


def get_mitma_zoning_urls(zone_type):
    """Fetches MITMA zoning URLs (Shapefiles + CSVs) from RSS feed."""
    rss_url = "https://movilidad-opendata.mitma.es/RSS.xml"

    if zone_type not in ["distritos", "municipios", "gau"]:
        raise ValueError(
            f"Invalid zone_type: {zone_type}. Must be 'distritos', 'municipios', or 'gau'.")

    folder_suffix = "GAU" if zone_type == "gau" else zone_type
    file_suffix = "gaus" if zone_type == "gau" else zone_type

    shp_pattern = rf'(https?://[^\s"<>]*/zonificacion/zonificacion_{folder_suffix}/[^"<>]+\.(?:shp|shx|dbf|prj))'
    csv_pattern = rf'(https?://[^\s"<>]*/zonificacion/zonificacion_{folder_suffix}/(?:nombres|poblacion)_{file_suffix}\.csv)'

    print(f"📡 Scanning RSS for {zone_type} zoning files...")

    try:
        req = urllib.request.Request(
            rss_url, headers={"User-Agent": "MITMA-DuckLake-Loader"})
        with urllib.request.urlopen(req) as response:
            txt = response.read().decode("utf-8", "ignore")

        shp_matches = re.findall(shp_pattern, txt, re.IGNORECASE)
        csv_matches = re.findall(csv_pattern, txt, re.IGNORECASE)

        unique_shp = sorted(list(set(shp_matches)))
        unique_csv = sorted(list(set(csv_matches)))

        url_nombres = next(
            (u for u in unique_csv if 'nombres' in u.lower()), None)
        url_poblacion = next(
            (u for u in unique_csv if 'poblacion' in u.lower()), None)

        if not unique_shp and not unique_csv:
            print(
                "WARNING: No zoning URLs found in RSS. The feed might have rotated them out.")
            return {}

        print(
            f"Found {len(unique_shp)} shapefile components and {len(unique_csv)} CSVs.")

        return {
            "shp_components": unique_shp,
            "nombres": url_nombres,
            "poblacion": url_poblacion
        }

    except Exception as e:
        print(f"ERROR fetching RSS: {e}")
        return {}


def clean_id(series):
    """Normalizes ID to clean string (removes .0 suffix and whitespace)."""
    return series.astype(str).str.strip().str.replace(r'\.0$', '', regex=True)


def clean_poblacion(series):
    """Cleans population integers (removes dots and decimals)."""
    return (series.astype(str)
            .str.replace('.', '', regex=False)
            .str.replace(r'\.0$', '', regex=True)
            .apply(pd.to_numeric, errors='coerce')
            .fillna(0).astype(int))


def get_mitma_zoning_dataset(zone_type='municipios'):
    """Downloads, cleans and merges MITMA zoning data into a GeoDataFrame."""
    urls = get_mitma_zoning_urls(zone_type)

    print(f"Generando dataset maestro para: {zone_type.upper()}")

    with tempfile.TemporaryDirectory() as tmp_dir:
        print("   Descargando geometrías...")
        shp_local_path = None

        for url in urls['shp_components']:
            filename = url.split('/')[-1]
            try:
                r = requests.get(url, timeout=15)
                if r.status_code == 200:
                    local_p = os.path.join(tmp_dir, filename)
                    with open(local_p, 'wb') as f:
                        f.write(r.content)
                    if filename.endswith('.shp'):
                        shp_local_path = local_p
            except Exception as e:
                print(f"      Error bajando {filename}: {e}")

        if not shp_local_path:
            print("Error: No se pudo descargar el archivo .shp principal.")
            return None

        gdf = gpd.read_file(shp_local_path)

        id_col = next((c for c in gdf.columns if c.upper() in [
                      'ID', 'CODIGO', 'ZONA', 'COD_GAU']), 'ID')
        gdf['ID'] = clean_id(gdf[id_col])

        gdf['geometry'] = gdf['geometry'].apply(make_valid)
        if gdf.crs and gdf.crs.to_string() != "EPSG:4326":
            gdf = gdf.to_crs("EPSG:4326")

        print("   🔗 Integrating metadata (Names and Population)...")
        df_aux = pd.DataFrame(columns=['ID'])

        aux_config = [
            {
                'type': 'nombres',
                'url': urls['nombres'],
                'header': 0,
                'cols': ['ID', 'Nombre']
            },
            {
                'type': 'poblacion',
                'url': urls['poblacion'],
                'header': None,
                'cols': ['ID', 'Poblacion']
            }
        ]

        for cfg in aux_config:
            try:
                r = requests.get(cfg['url'], timeout=10)
                if r.status_code == 200:
                    df_t = pd.read_csv(
                        io.BytesIO(r.content),
                        sep='|',
                        header=cfg['header'],
                        dtype=str,
                        engine='python'
                    )

                    if len(df_t.columns) >= 3:
                        df_t = df_t.iloc[:, [1, 2]]
                    elif len(df_t.columns) == 2:
                        df_t = df_t.iloc[:, [0, 1]]

                    df_t.columns = cfg['cols']

                    df_t['ID'] = clean_id(df_t['ID'])
                    df_t = df_t.drop_duplicates(subset=['ID'])

                    if cfg['type'] == 'poblacion':
                        df_t['Poblacion'] = clean_poblacion(df_t['Poblacion'])

                    if df_aux.empty:
                        df_aux = df_t
                    else:
                        df_aux = df_aux.merge(df_t, on='ID', how='outer')

                    print(f"      {cfg['type'].capitalize()} OK")
            except Exception as e:
                print(f"      Failed processing {cfg['type']}: {e}")

        if not df_aux.empty:
            gdf = gdf.merge(df_aux, on='ID', how='left')

            if 'Nombre' in gdf.columns:
                gdf['Nombre'] = gdf['Nombre'].fillna(gdf['ID'])
            if 'Poblacion' in gdf.columns:
                gdf['Poblacion'] = gdf['Poblacion'].fillna(0).astype(int)

        cols = ['ID', 'Nombre', 'Poblacion', 'geometry']
        final_cols = [c for c in cols if c in gdf.columns] + \
            [c for c in gdf.columns if c not in cols]
        gdf = gdf[final_cols]

        print(f"Dataset generado: {len(gdf)} registros.")
        return gdf


def load_zonificacion(con, zone_type, lake_layer='bronze'):
    """Loads zonification data into DuckDB for the specified zone type."""
    df = get_mitma_zoning_dataset(zone_type)

    if df is None or df.empty:
        print(f"No data to load for {zone_type}")
        return

    for col in df.columns:
        df[col] = df[col].astype(str)

    table_name = f'{lake_layer}_mitma_{zone_type}'

    con.register('temp_zonificacion', df)

    con.execute(f"""
        CREATE TABLE IF NOT EXISTS {table_name} AS
        SELECT
            *,
            CURRENT_TIMESTAMP AS loaded_at,
        FROM temp_zonificacion
        LIMIT 0;
    """)

    merge_key = 'ID'

    con.execute(f"""
        MERGE INTO {table_name} AS target
        USING (
            SELECT
                *,
                CURRENT_TIMESTAMP AS loaded_at,
            FROM temp_zonificacion
        ) AS source
        ON target.{merge_key} = source.{merge_key}
        WHEN MATCHED THEN
            UPDATE SET *
        WHEN NOT MATCHED THEN
            INSERT *;
    """)

    con.unregister('temp_zonificacion')

    print(f"Table {table_name} merged successfully with {len(df)} records.")

def _get_csv_source_query(urls, fecha_as_timestamp=False):
    """Generates SELECT subquery for reading CSVs with standard configuration.
    
    Args:
        urls: List of URLs to read
        fecha_as_timestamp: If True, transforms fecha column to TIMESTAMP format
    """
    url_list_str = "[" + ", ".join([f"'{u}'" for u in urls]) + "]"

    fecha_column = (
        "strptime(CAST(fecha AS VARCHAR), '%Y%m%d')::TIMESTAMP AS fecha,\n            "
        if fecha_as_timestamp
        else ""
    )
    exclude_clause = (
        "* EXCLUDE (fecha, filename),"
        if fecha_as_timestamp
        else "* EXCLUDE (filename),"
    )

    return f"""
        SELECT 
            {fecha_column}{exclude_clause}
            CURRENT_TIMESTAMP AS loaded_at,
            filename AS source_file
        FROM read_csv(
            {url_list_str},
            filename = true,
            all_varchar = true
        )
    """

def filter_json_urls(table_name: str, urls: list[str]):
    """Generic function to filter JSON table URLs using source_url column.
    
    Args:
        table_name: Name of the bronze table to check for existing URLs
        urls: List of URLs to filter
        
    Returns:
        List of URLs that haven't been ingested yet
    """
    print(f"Filtering {len(urls)} URLs for {table_name}")

    con = get_ducklake_connection()

    try:
        url_list_str = "[" + ", ".join([f"'{u}'" for u in urls]) + "]"

        ingested_df = con.execute(f"""
            WITH url_list AS (
                SELECT unnest({url_list_str}) AS url_to_check
            )
            SELECT DISTINCT source_url 
            FROM {table_name}
            WHERE source_url IS NOT NULL
              AND source_url IN (SELECT url_to_check FROM url_list)
        """).fetchdf()

        ingested_urls = set(
            ingested_df['source_url'].tolist()) if not ingested_df.empty else set()
        print(
            f"Found {len(ingested_urls)} already ingested URLs (out of {len(urls)} checked)")
    except Exception as e:
        print(f"Warning: Could not check existing URLs (table may not exist): {e}")
        print(f"Returning all URLs")
        return urls

    new_urls = [url for url in urls if url not in ingested_urls]
    print(
        f"Filtered result: {len(new_urls)} new URLs to ingest (skipping {len(urls) - len(new_urls)} already ingested)")

    return new_urls