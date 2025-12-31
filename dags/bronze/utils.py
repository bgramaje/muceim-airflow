"""
MITMA-specific Utility functions for Bronze layer.
Contains helper functions for URL fetching and zoning data processing.
"""

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
from datetime import datetime
from shapely.validation import make_valid
import sys

# Ensure we can import from parent directory
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

LAKE_LAYER = 'bronze'
RUSTFS_RAW_BUCKET = 'mitma_raw'


def get_mitma_urls(dataset, zone_type, start_date, end_date):
    """
    Fetches MITMA URLs from RSS feed and filters by dataset, zone type, and date range.

    Parameters:
    - dataset: 'od', 'people_day', 'overnight_stay'
    - zone_type: 'distritos', 'municipios', 'gau'
    - start_date: 'YYYY-MM-DD'
    - end_date: 'YYYY-MM-DD'

    Returns:
    - List of URLs matching the criteria
    """
    rss_url = "https://movilidad-opendata.mitma.es/RSS.xml"

    # Simple mapping: dataset -> (url_path, file_prefix)
    dataset_map = {
        "od": ("viajes", "Viajes"),
        "people_day": ("personas", "Personas_dia"),
        "overnight_stay": ("pernoctaciones", "Pernoctaciones")
    }

    if zone_type not in ["distritos", "municipios", "gau"]:
        raise ValueError(
            f"Invalid zone_type: {zone_type}. Must be 'distritos', 'municipios', or 'gau'.")
    if dataset not in dataset_map:
        raise ValueError(
            f"Invalid dataset: {dataset}. Must be one of {list(dataset_map.keys())}.")

    dataset_path, file_prefix = dataset_map[dataset]

    # Construct file pattern: {Prefix}_{zone} (GAU is uppercase in files)
    zone_suffix = "GAU" if zone_type == "gau" else zone_type
    file_pattern = f"{file_prefix}_{zone_suffix}"

    # Build dynamic regex pattern
    # Pattern: https://.../por-{zone}/viajes/ficheros-diarios/YYYY-MM/YYYYMMDD_{FilePattern}.csv.gz
    pattern = rf'(https?://[^\s"<>]*/estudios_basicos/por-{zone_type}/{dataset_path}/ficheros-diarios/\d{{4}}-\d{{2}}/(\d{{8}})_{file_pattern}\.csv\.gz)'

    # Fetch RSS with User-Agent to avoid 403
    req = urllib.request.Request(
        rss_url, headers={"User-Agent": "MITMA-DuckLake-Loader"})
    txt = urllib.request.urlopen(req).read().decode("utf-8", "ignore")

    # Find all matches (case-insensitive for por-gau vs por-GAU)
    matches = re.findall(pattern, txt, re.I)

    # Remove duplicates using set (RSS often has duplicate entries)
    unique_matches = list(set(matches))

    # Convert date range to comparable format
    start_dt = datetime.strptime(start_date, "%Y-%m-%d")
    end_dt = datetime.strptime(end_date, "%Y-%m-%d")

    # Filter by date range and sort
    filtered_urls = []
    for url, date_str in unique_matches:
        file_date = datetime.strptime(date_str, "%Y%m%d")
        if start_dt <= file_date <= end_dt:
            filtered_urls.append((url, date_str))

    # Sort by date ascending
    filtered_urls.sort(key=lambda x: x[1])

    # Extract just the URLs
    urls = [url for url, _ in filtered_urls]

    print(
        f"Found {len(urls)} URLs for {dataset} {zone_type} from {start_date} to {end_date}")

    if not urls:
        print(f"WARNING: No URLs found. Check if data exists for the requested date range.")

    return urls


def download_url_to_rustfs(url: str, dataset: str, zone_type: str) -> str:
    """
    Descarga un archivo desde una URL y lo sube a RustFS bucket mitma_raw.
    
    Parameters:
    - url: URL del archivo a descargar
    - dataset: Dataset tipo ('od', 'people_day', 'overnight_stay')
    - zone_type: Tipo de zona ('distritos', 'municipios', 'gau')
    
    Returns:
    - Ruta S3 en formato s3://mitma_raw/{dataset}/{zone_type}/{filename}
    """
    from airflow.providers.amazon.aws.hooks.s3 import S3Hook
    from airflow.models import Variable
    
    print(f"[DOWNLOAD] Downloading {url} to RustFS...")
    
    # Extraer nombre de archivo de la URL
    parsed_url = urllib.parse.urlparse(url)
    filename = os.path.basename(parsed_url.path)
    
    if not filename:
        raise ValueError(f"Could not extract filename from URL: {url}")
    
    # Construir ruta S3: mitma_raw/{dataset}/{zone_type}/{filename}
    s3_key = f"{dataset}/{zone_type}/{filename}"
    s3_path = f"s3://{RUSTFS_RAW_BUCKET}/{s3_key}"
    
    print(f"[DOWNLOAD] Target S3 path: {s3_path}")
    
    # Conectar a RustFS
    s3_hook = S3Hook(aws_conn_id='rustfs_s3_conn')
    
    # Verificar si el archivo ya existe en RustFS
    if s3_hook.check_for_key(s3_key, bucket_name=RUSTFS_RAW_BUCKET):
        print(f"[DOWNLOAD] ✅ File already exists in RustFS: {s3_path}")
        return s3_path
    
    # Descargar archivo desde URL con User-Agent
    print(f"[DOWNLOAD] Downloading from URL...")
    req = urllib.request.Request(url, headers={"User-Agent": "MITMA-DuckLake-Loader"})
    
    try:
        with urllib.request.urlopen(req, timeout=300) as response:
            file_content = response.read()
            print(f"[DOWNLOAD] Downloaded {len(file_content)} bytes")
    except Exception as e:
        raise RuntimeError(f"Failed to download file from {url}: {e}")
    
    # Subir a RustFS
    print(f"[DOWNLOAD] Uploading to RustFS bucket '{RUSTFS_RAW_BUCKET}'...")
    try:
        s3_hook.load_bytes(
            bytes_data=file_content,
            key=s3_key,
            bucket_name=RUSTFS_RAW_BUCKET,
            replace=True
        )
        print(f"[DOWNLOAD] ✅ Successfully uploaded to {s3_path}")
    except Exception as e:
        raise RuntimeError(f"Failed to upload file to RustFS: {e}")
    
    return s3_path


def get_mitma_zoning_urls(zone_type):
    """
    Fetches MITMA Zoning URLs (Shapefiles + CSVs) from RSS feed using Regex.

    Parameters:
    - zone_type: 'distritos', 'municipios', 'gau'

    Returns:
    - Dictionary with shapefile components, nombres URL, and poblacion URL
    """
    rss_url = "https://movilidad-opendata.mitma.es/RSS.xml"

    if zone_type not in ["distritos", "municipios", "gau"]:
        raise ValueError(
            f"Invalid zone_type: {zone_type}. Must be 'distritos', 'municipios', or 'gau'.")

    folder_suffix = "GAU" if zone_type == "gau" else zone_type
    file_suffix = "gaus" if zone_type == "gau" else zone_type

    # --- REGEX PATTERNS ---
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
    """Normaliza ID a string limpio (sin .0, sin espacios)."""
    return series.astype(str).str.strip().str.replace(r'\.0$', '', regex=True)


def clean_poblacion(series):
    """Limpia enteros de población (quita puntos y decimales)."""
    return (series.astype(str)
            .str.replace('.', '', regex=False)
            .str.replace(r'\.0$', '', regex=True)
            .apply(pd.to_numeric, errors='coerce')
            .fillna(0).astype(int))


def get_mitma_zoning_dataset(zone_type='municipios'):
    """
    Orquesta la descarga, limpieza y fusión de datos maestros.
    Retorna un GeoDataFrame listo para ingesta.

    Parameters:
    - zone_type: 'distritos', 'municipios', 'gau'

    Returns:
    - GeoDataFrame with zoning data
    """
    urls = get_mitma_zoning_urls(zone_type)

    print(f"🚀 Generando dataset maestro para: {zone_type.upper()}")

    with tempfile.TemporaryDirectory() as tmp_dir:
        print("   ⬇️  Descargando geometrías...")
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
                print(f"      ⚠️ Error bajando {filename}: {e}")

        if not shp_local_path:
            print("❌ Error: No se pudo descargar el archivo .shp principal.")
            return None

        gdf = gpd.read_file(shp_local_path)

        id_col = next((c for c in gdf.columns if c.upper() in [
                      'ID', 'CODIGO', 'ZONA', 'COD_GAU']), 'ID')
        gdf['ID'] = clean_id(gdf[id_col])

        gdf['geometry'] = gdf['geometry'].apply(make_valid)
        if gdf.crs and gdf.crs.to_string() != "EPSG:4326":
            gdf = gdf.to_crs("EPSG:4326")

        print("   🔗 Integrando metadatos (Nombres y Población)...")
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
                    # Leer CSV crudo
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

                    print(f"      ✓ {cfg['type'].capitalize()} OK")
            except Exception as e:
                print(f"      ⚠️ Fallo procesando {cfg['type']}: {e}")

        # --- C. Merge Final ---
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

        print(f"✅ Dataset generado: {len(gdf)} registros.")
        return gdf


def load_zonificacion(con, zone_type, lake_layer='bronze'):
    """
    Load zonification data into DuckDB for the specified type.

    Parameters:
    - con: DuckDB connection
    - zone_type: 'distritos', 'municipios', 'gau'
    - lake_layer: layer name (default: 'bronze')
    """
    df = get_mitma_zoning_dataset(zone_type)

    if df is None or df.empty:
        print(f"No data to load for {zone_type}")
        return

    # Convert all columns to string (including geometry)
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


def _get_data_columns(table_name):
    """
    Introspección: Obtiene las columnas de negocio (excluyendo auditoría).
    """
    audit_cols = "('loaded_at', 'source_file', 'source_url')"

    con = get_ducklake_connection()
    df = con.execute(f"""
        SELECT column_name 
        FROM information_schema.columns 
        WHERE table_name = '{table_name}'
        AND column_name NOT IN {audit_cols}
        ORDER BY ordinal_position;
    """).fetchdf()
    return df['column_name'].tolist()


def _build_merge_condition(columns):
    """
    Construye una cláusula ON robusta que maneja NULLs correctamente.
    """
    return " AND ".join([
        f"target.{col} IS NOT DISTINCT FROM source.{col}"
        for col in columns
    ])


def _get_csv_source_query(urls):
    """
    Genera la subconsulta SELECT para leer los CSVs.
    Centraliza la configuración de read_csv.
    """
    url_list_str = "[" + ", ".join([f"'{u}'" for u in urls]) + "]"

    return f"""
        SELECT 
            * EXCLUDE (filename),
            CURRENT_TIMESTAMP AS loaded_at,
            filename AS source_file
        FROM read_csv(
            {url_list_str},
            filename = true,
            all_varchar = true
        )
    """


def create_table_from_csv(table_name, url):
    """
    Creates a DuckDB table from a single CSV URL if it doesn't exist.
    """
    full_table_name = f'{LAKE_LAYER}_{table_name}'
    con = get_ducklake_connection()

    source_sql = _get_csv_source_query([url])

    print(f"Verifying schema for {full_table_name} using first file...")
    con.execute(f"""
        CREATE TABLE IF NOT EXISTS {full_table_name} AS
        {source_sql}
        LIMIT 0;
    """)
    print(f"Table {full_table_name} is ready.")


def merge_from_csv(table_name, url):
    """
    Merges data from a single CSV URL into an existing DuckDB table.
    Raises exception if merge fails to ensure task failure in Airflow.
    Handles transaction errors by forcing a new connection.
    """
    full_table_name = f'{LAKE_LAYER}_{table_name}'
    con = get_ducklake_connection()

    merge_keys = _get_data_columns(full_table_name)
    on_clause = _build_merge_condition(merge_keys)

    source_sql = _get_csv_source_query([url])

    print(f"Merging data from {url} into {full_table_name}...")
    try:
        con.execute(f"""
            MERGE INTO {full_table_name} AS target
            USING ({source_sql}) AS source
            ON {on_clause}
            WHEN NOT MATCHED THEN
                INSERT *;
        """)
        print(f"  ✅ Merged successfully.")
    except Exception as e:
        error_str = str(e)

        is_transaction_error = (
            "TransactionContext" in error_str or
            "Failed to commit" in error_str or
            "Failed to execute query" in error_str
        )

        if is_transaction_error:
            print(
                f"  ⚠️ Transaction error detected - forcing new connection for next task")
            try:
                get_ducklake_connection(force_new=True)
            except:
                pass

        error_msg = f"  ❌ Error processing {url}: {e}"
        print(error_msg)
        # Re-raise exception to ensure Airflow task fails
        raise RuntimeError(error_msg) from e


def create_table_from_json(table_name, url):
    """
    Creates a DuckDB table from a single JSON URL if it doesn't exist.
    Updates statistics for query optimization (DuckDB alternative to indexes).
    """
    full_table_name = f'{LAKE_LAYER}_{table_name}'
    con = get_ducklake_connection()

    source_sql = _get_json_source_query(url)

    print(f"Verifying schema for {full_table_name} using first file...")
    con.execute(f"""
        CREATE TABLE IF NOT EXISTS {full_table_name} AS
        {source_sql}
        LIMIT 0;
    """)

    # Update statistics for query optimization (DuckDB alternative to indexes)
    # This helps optimize queries filtering by source_url
    try:
        con.execute(f"ANALYZE {full_table_name};")
        print(
            f"  ✅ Updated statistics for {full_table_name} (query optimization)")
    except Exception as analyze_error:
        print(f"  ⚠️ Could not analyze table (non-critical): {analyze_error}")

    print(f"Table {full_table_name} is ready.")


def merge_from_json(table_name, url, key_columns=None):
    """
    Merges data from a single JSON URL into an existing DuckDB table.
    Raises exception if merge fails to ensure task failure in Airflow.
    Handles transaction errors by forcing a new connection.
    """
    full_table_name = f'{LAKE_LAYER}_{table_name}'
    con = get_ducklake_connection()

    if key_columns is None:
        merge_keys = _get_data_columns(full_table_name)
    else:
        existing_cols = _get_data_columns(full_table_name)
        missing = [k for k in key_columns if k not in existing_cols]
        if missing:
            raise ValueError(
                f"Key columns {missing} not found in table metadata.")
        merge_keys = key_columns

    on_clause = _build_merge_condition(merge_keys)
    source_sql = _get_json_source_query(url)

    print(f"Merging data from {url} into {full_table_name}...")
    try:
        con.execute(f"""
            MERGE INTO {full_table_name} AS target
            USING ({source_sql}) AS source
            ON {on_clause}
            WHEN NOT MATCHED THEN
                INSERT *;
        """)
        print(f"  ✅ Merged successfully.")
    except Exception as e:
        error_str = str(e)
        # Check if it's a transaction error - DuckDB enters restricted mode
        is_transaction_error = (
            "TransactionContext" in error_str or
            "Failed to commit" in error_str or
            "Failed to execute query" in error_str
        )

        if is_transaction_error:
            print(
                f"  ⚠️ Transaction error detected - forcing new connection for next task")
            # Force new connection to avoid affecting other tasks
            try:
                get_ducklake_connection(force_new=True)
            except:
                pass  # Connection will be recreated on next use

        error_msg = f"  ❌ Error processing {url}: {e}"
        print(error_msg)
        # Re-raise exception to ensure Airflow task fails
        raise RuntimeError(error_msg) from e


def _get_json_source_query(url):
    """
    Genera la subconsulta SELECT para leer el JSON.
    """
    return f"""
        SELECT 
            *,
            CURRENT_TIMESTAMP AS loaded_at,
            '{url}' AS source_url
        FROM read_json('{url}', format='array')
    """


def ine_renta_filter_urls(urls: list[str]):
    """
    Filter INE Renta URLs to only include those not already ingested.
    Uses source_url column (JSON tables use source_url, not source_file).
    Optimized to use index on source_url for faster lookups.
    """
    table_name = 'bronze_ine_renta_municipio'
    return _filter_json_urls(table_name, urls)


def ine_municipios_filter_urls(urls: list[str]):
    """
    Filter INE Municipios URLs to only include those not already ingested.
    Uses source_url column (JSON tables use source_url, not source_file).
    """
    table_name = 'bronze_ine_municipios'
    return _filter_json_urls(table_name, urls)


def ine_empresas_filter_urls(urls: list[str]):
    """
    Filter INE Empresas URLs to only include those not already ingested.
    Uses source_url column (JSON tables use source_url, not source_file).
    """
    table_name = 'bronze_ine_empresas_municipio'
    return _filter_json_urls(table_name, urls)


def ine_poblacion_filter_urls(urls: list[str]):
    """
    Filter INE Poblacion URLs to only include those not already ingested.
    Uses source_url column (JSON tables use source_url, not source_file).
    """
    table_name = 'bronze_ine_poblacion_municipio'
    return _filter_json_urls(table_name, urls)


def mitma_create_table(dataset: str, zone_type: str, urls: list[str]):
    """
    Create the table for MITMA data if it doesn't exist.
    Takes the first URL from the list to create the table schema.
    """
    table_name = f'mitma_{dataset}_{zone_type}'

    if not urls:
        raise ValueError(f"No URLs provided to create table {table_name}")

    first_url = urls[0]
    print(
        f"[TASK] Creating table {table_name} if not exists, using first URL: {first_url}")

    create_table_from_csv(table_name, first_url)

    return {'status': 'success', 'table_name': table_name}


def mitma_filter_urls(dataset: str, zone_type: str, urls: list[str]):
    """
    Filter MITMA URLs to only include those not already ingested.
    Uses source_file column (CSV tables use source_file, not source_url).
    First checks if the table exists. If not, returns all URLs.
    If table exists, filters out already ingested URLs.
    """
    table_name = f'bronze_mitma_{dataset}_{zone_type}'
    return _filter_csv_urls(table_name, urls)


def mitma_ine_relations_filter_urls(urls: list[str]):
    """
    Filter MITMA-INE Relations URLs to only include those not already ingested.
    Uses source_file column (CSV tables use source_file, not source_url).
    """
    table_name = 'bronze_mitma_ine_relations'
    return _filter_csv_urls(table_name, urls)


def _filter_json_urls(table_name: str, urls: list[str]):
    """
    Generic function to filter JSON table URLs using source_url column.
    First checks if the table exists. If not, returns all URLs.
    If table exists, filters out already ingested URLs.
    """
    print(f"[TASK] Filtering URLs for {table_name}")
    print(f"[TASK] Total URLs to check: {len(urls)}")

    con = get_ducklake_connection()

    # Check if table exists
    try:
        table_exists = con.execute(f"""
            SELECT COUNT(*) as count 
            FROM information_schema.tables 
            WHERE table_name = '{table_name}'
        """).fetchone()[0] > 0

        if not table_exists:
            print(
                f"[TASK] Table {table_name} does not exist. Returning all URLs.")
            return urls

        print(f"[TASK] Table {table_name} exists. Filtering URLs...")
    except Exception as e:
        print(f"[TASK] Warning: Could not check if table exists: {e}")
        print(f"[TASK] Assuming table does not exist. Returning all URLs.")
        return urls

    # Table exists, filter URLs
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
            f"[TASK] Found {len(ingested_urls)} already ingested URLs (out of {len(urls)} checked)")
    except Exception as e:
        print(f"[TASK] Warning: Could not check existing URLs: {e}")
        print(f"[TASK] Proceeding as if no URLs are ingested")
        ingested_urls = set()

    new_urls = [url for url in urls if url not in ingested_urls]
    print(
        f"[TASK] Filtered result: {len(new_urls)} new URLs to ingest (skipping {len(urls) - len(new_urls)} already ingested)")

    if len(new_urls) == 0:
        print(
            f"[TASK] ⚠️  All URLs have already been ingested. No new data to process.")
    else:
        print(
            f"[TASK] ✅ URLs to ingest: {new_urls[:3]}{'...' if len(new_urls) > 3 else ''}")

    return new_urls


def _filter_csv_urls(table_name: str, urls: list[str]):
    """
    Generic function to filter CSV table URLs using source_file column.
    First checks if the table exists. If not, returns all URLs.
    If table exists, filters out already ingested URLs.
    """
    print(f"[TASK] Filtering URLs for {table_name}")
    print(f"[TASK] Total URLs to check: {len(urls)}")

    con = get_ducklake_connection()

    # Check if table exists
    try:
        table_exists = con.execute(f"""
            SELECT COUNT(*) as count 
            FROM information_schema.tables 
            WHERE table_name = '{table_name}'
        """).fetchone()[0] > 0

        if not table_exists:
            print(
                f"[TASK] Table {table_name} does not exist. Returning all URLs.")
            return urls

        print(f"[TASK] Table {table_name} exists. Filtering URLs...")
    except Exception as e:
        print(f"[TASK] Warning: Could not check if table exists: {e}")
        print(f"[TASK] Assuming table does not exist. Returning all URLs.")
        return urls

    # Table exists, filter URLs
    try:
        url_list_str = "[" + ", ".join([f"'{u}'" for u in urls]) + "]"

        ingested_df = con.execute(f"""
            WITH url_list AS (
                SELECT unnest({url_list_str}) AS url_to_check
            )
            SELECT DISTINCT source_file 
            FROM {table_name}
            WHERE source_file IS NOT NULL
              AND source_file IN (SELECT url_to_check FROM url_list)
        """).fetchdf()

        ingested_urls = set(
            ingested_df['source_file'].tolist()) if not ingested_df.empty else set()
        print(
            f"[TASK] Found {len(ingested_urls)} already ingested URLs (out of {len(urls)} checked)")
    except Exception as e:
        print(f"[TASK] Warning: Could not check existing URLs: {e}")
        print(f"[TASK] Proceeding as if no URLs are ingested")
        ingested_urls = set()

    new_urls = [url for url in urls if url not in ingested_urls]
    print(
        f"[TASK] Filtered result: {len(new_urls)} new URLs to ingest (skipping {len(urls) - len(new_urls)} already ingested)")

    if len(new_urls) == 0:
        print(
            f"[TASK] ⚠️  All URLs have already been ingested. No new data to process.")
    else:
        print(
            f"[TASK] ✅ URLs to ingest: {new_urls[:3]}{'...' if len(new_urls) > 3 else ''}")

    return new_urls
