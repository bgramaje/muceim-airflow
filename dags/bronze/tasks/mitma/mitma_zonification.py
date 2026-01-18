"""
Airflow task for loading MITMA zonification data into Bronze layer.
Handles zoning (zonificación) data including geometries, names, and population
for distritos, municipios, and GAU zone types.
"""

from airflow.sdk import task


@task
def BRONZE_mitma_zonification_urls(zone_type: str = 'distritos'):
    """
    Generate the list of URLs for MITMA Zonification data.
    Returns a dictionary with shapefile components, nombres URL, and poblacion URL.
    """
    from bronze.utils import get_mitma_zoning_urls

    urls = get_mitma_zoning_urls(zone_type)
    print(f"[TASK] Generated URLs for zonification {zone_type}: {len(urls.get('shp_components', []))} shapefile components")
    return urls


@task
def BRONZE_mitma_zonification(zone_type: str = 'distritos'):
    """
    Airflow task to load zonification data into DuckDB for the specified type.
    
    This function downloads shapefiles, CSVs with names and population,
    merges them, and loads into a bronze layer table.
    
    First checks if the table exists and has data. If it exists and is not empty,
    the task is skipped. Otherwise, it proceeds with the data load.
    
    Parameters:
    - zone_type: 'distritos', 'municipios', 'gau' (default: 'distritos')
    
    Returns:
    - Dict with task status and info
    """
    from bronze.utils import load_zonificacion
    from utils.utils import get_ducklake_connection

    print(f"[TASK] Starting zonification load for {zone_type}")
    
    # Get connection (singleton - will be reused)
    con = get_ducklake_connection()
    
    # Check if table exists and has data
    table_name = f'bronze_mitma_{zone_type}'
    
    try:
        # Check if table exists
        table_exists = con.execute(f"""
            SELECT COUNT(*) as count 
            FROM information_schema.tables 
            WHERE table_name = '{table_name}'
        """).fetchone()[0] > 0
        
        if table_exists:
            # Check if table has data
            count_result = con.execute(f"SELECT COUNT(*) as count FROM {table_name}").fetchdf()
            record_count = int(count_result['count'].iloc[0])
            
            if record_count > 0:
                msg = f"Table {table_name} already exists with {record_count:,} records. Skipping zonification load."
                print(f"[TASK] {msg}")
                return {
                    'status': 'skipped',
                    'message': msg,
                    'zone_type': zone_type,
                    'dataset': 'zonification',
                    'records': record_count,
                    'table_name': table_name
                }
            else:
                print(f"[TASK] Table {table_name} exists but is empty. Proceeding with load...")
        else:
            print(f"[TASK] Table {table_name} does not exist. Proceeding with load...")
    except Exception as e:
        print(f"[TASK] Warning: Could not check table status: {e}. Proceeding with load...")
    
    # Load zonification data using utility function
    load_zonificacion(con, zone_type)
    
    # Get count for verification
    count = con.execute(f"SELECT COUNT(*) as count FROM {table_name}").fetchdf()
    record_count = int(count['count'].iloc[0])
    
    msg = f"Successfully loaded zonification data for {zone_type}: {record_count:,} records"
    print(f"[TASK] {msg}")
    print(f"[TASK] Sample data from {table_name}:")
    print(con.execute(f"SELECT * FROM {table_name} LIMIT 10").fetchdf())
    
    return {
        'status': 'success',
        'message': msg,
        'zone_type': zone_type,
        'dataset': 'zonification',
        'records': record_count,
        'table_name': table_name
    }
