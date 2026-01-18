"""
Airflow task for building the unified MITMA OD Silver table.
Includes:
- Type casting
- Weekend / holiday flags
- Null filtering of critical fields
- zone_level standardization
- Batch processing with dynamic task mapping for large datasets
- Idempotent processing using DISTINCT on silver_mitma_od to track processed dates

"""

from airflow.sdk import task
from typing import List, Dict, Any

from utils.gcp import execute_sql_or_cloud_run
from utils.utils import get_ducklake_connection
from utils.logger import get_logger


@task
def SILVER_mitma_od_get_date_batches(
    batch_size: int = 2,
    start_date: str = None,
    end_date: str = None,
    **context
) -> List[Dict[str, Any]]:
    """
    Obtiene las fechas del rango [start_date, end_date] en bronze que aún no están en silver.
    Usa EXCEPT para calcular la diferencia de forma simple.

    Ambas tablas (bronze y silver) almacenan fecha como TIMESTAMP.

    Parameters:
    - batch_size: Número de fechas por batch (default: 2)
    - start_date: Fecha inicio (formato: YYYYMMDD o YYYY-MM-DD)
    - end_date: Fecha fin (formato: YYYYMMDD o YYYY-MM-DD)

    Returns:
    - Lista de diccionarios con 'fechas' y 'batch_index' para cada batch
    """
    batch_size = int(batch_size) if isinstance(batch_size, str) else batch_size

    con = get_ducklake_connection()

    # Limpiar y validar fechas
    start_clean = None
    if start_date and start_date not in ('None', '', '{{ params.start }}'):
        start_clean = start_date.replace('-', '')
    
    end_clean = None
    if end_date and end_date not in ('None', '', '{{ params.end }}'):
        end_clean = end_date.replace('-', '')

    # Construir filtro de fecha solo si ambas fechas están definidas
    if start_clean and end_clean:
        date_range = f"AND fecha BETWEEN strptime('{start_clean}', '%Y%m%d') AND strptime('{end_clean}', '%Y%m%d')"
    else:
        date_range = ""

    logger = get_logger(__name__, context)
    logger.info(f"Getting unprocessed dates (batch_size: {batch_size}, start: {start_date}, end: {end_date})")

    try:
        df = con.execute(f"""
            SELECT DISTINCT strftime(fecha, '%Y%m%d') AS fecha
            FROM bronze_mitma_od_municipios
            WHERE fecha IS NOT NULL {date_range}
            
            EXCEPT
            
            SELECT DISTINCT strftime(fecha, '%Y%m%d') AS fecha
            FROM silver_mitma_od
            WHERE fecha IS NOT NULL {date_range}
            
            ORDER BY fecha
        """).fetchdf()
    except Exception as e:
        logger.warning(f"table may not exist, getting all bronze dates: {e}")
        df = con.execute(f"""
            SELECT DISTINCT strftime(fecha, '%Y%m%d') AS fecha
            FROM bronze_mitma_od_municipios
            WHERE fecha IS NOT NULL {date_range}
            ORDER BY fecha
        """).fetchdf()

    if df.empty:
        logger.info("No unprocessed dates found")
        return []

    fechas = sorted(df['fecha'].tolist())
    logger.info(f"{len(fechas)} unprocessed dates to process")

    # Crear batches
    batches = [
        {'batch_index': i, 'fechas': fechas[j:j + batch_size]}
        for i, j in enumerate(range(0, len(fechas), batch_size))
    ]

    logger.info(f"Created {len(batches)} batches")
    return batches


@task.branch
def SILVER_mitma_od_check_batches(date_batches: list[dict], **context) -> Any:
    """
    Branch task que determina si hay batches para procesar o no.
    """
    if not date_batches or len(date_batches) == 0:
        return "mitma_od_batches.batches_skipped"
    else:
        return "mitma_od_batches.process_batch"


@task
def SILVER_mitma_od_create_table(**context) -> Dict:
    """
    Crea la tabla silver_mitma_od si no existe.
    silver_mitma_od está particionada por year/month/day de fecha para optimizar queries.
    El particionado solo se aplica a tablas nuevas (vacías) para evitar corrupción.

    Nota: Ya no se usa tabla de tracking. Se usa DISTINCT sobre silver_mitma_od.fecha
    para determinar qué fechas ya están procesadas.

    Returns:
    - Dict con status de la creación de la tabla
    """
    logger = get_logger(__name__, context)
    logger.info("Creating silver_mitma_od table if it doesn't exist")

    con = get_ducklake_connection()

    table_exists = False
    try:
        result = con.execute("""
            SELECT COUNT(*) as cnt FROM information_schema.tables 
            WHERE table_schema = 'main' AND table_name = 'silver_mitma_od'
        """).fetchone()
        table_exists = result[0] > 0
    except:
        pass

    con.execute("""
        SET preserve_insertion_order=false;
        SET enable_object_cache=true;
        
        CREATE TABLE IF NOT EXISTS silver_mitma_od (
            fecha TIMESTAMP,
            origen_zone_id VARCHAR,
            destino_zone_id VARCHAR,
            viajes DOUBLE,
            viajes_km DOUBLE,
            residencia VARCHAR,
            is_weekend BOOLEAN,
            is_holiday BOOLEAN
        );
    """)

    if not table_exists:
        con.execute(
            "ALTER TABLE silver_mitma_od SET PARTITIONED BY (year(fecha), month(fecha), day(fecha));")

    logger.info("Tables created/verified successfully")

    return {
        "status": "success",
        "table": "silver_mitma_od",
    }


@task
def SILVER_mitma_od_process_batch(date_batch: Dict[str, Any], **context) -> Dict:
    """
    Procesa un batch de fechas de la tabla bronze usando MERGE INTO en silver_mitma_od.
    Suma los valores de viajes y viajes_km cuando coinciden fecha, origen, destino y residencia,
    permitiendo agregar registros con diferentes segmentos demográficos (edad, renta, etc.).
    Cada batch se procesa en paralelo usando dynamic task mapping.

    Usa MERGE INTO para manejar duplicados: si ya existe el registro, suma los valores.
    La idempotencia también se garantiza mediante DISTINCT sobre silver_mitma_od.fecha en get_date_batches.

    Parameters:
    - date_batch: Dict con 'fechas' (lista de fechas) y 'batch_index'

    Returns:
    - Dict con status y metadata del batch procesado
    """
    logger = get_logger(__name__, context)
    logger.debug(f"date_batch type: {type(date_batch)}")
    logger.debug(f"date_batch content: {date_batch}")

    if isinstance(date_batch, dict):
        fechas = date_batch.get('fechas', [])
        batch_index = date_batch.get('batch_index', 0)
    elif isinstance(date_batch, list):
        fechas = date_batch
        batch_index = 0
    else:
        raise ValueError(
            f"Unexpected date_batch type: {type(date_batch)}, value: {date_batch}")

    if not fechas:
        raise ValueError(f"No fechas found in date_batch: {date_batch}")

    logger.info(f"Processing batch {batch_index}: {len(fechas)} dates (from {fechas[0]} to {fechas[-1]})")

    sql_query = f"""
        SET preserve_insertion_order=false;
        SET enable_object_cache=true;
        
        INSERT INTO silver_mitma_od (fecha, origen_zone_id, destino_zone_id, viajes, viajes_km, residencia, is_weekend, is_holiday)
        SELECT
            b.fecha + (b.periodo::INTEGER * INTERVAL 1 HOUR) AS fecha,
            b.origen AS origen_zone_id,
            b.destino AS destino_zone_id,
            SUM(b.viajes::DOUBLE) AS viajes,
            SUM(b.viajes_km::DOUBLE) AS viajes_km,
            b.residencia,
            MAX(EXTRACT(DOW FROM b.fecha) IN (0, 6)) AS is_weekend,
            MAX(h.date IS NOT NULL) AS is_holiday
        FROM bronze_mitma_od_municipios b
        LEFT JOIN bronze_spanish_holidays h ON DATE(b.fecha) = h.date
        WHERE strftime(b.fecha, '%Y%m%d') IN ('{"', '".join(str(f) for f in fechas)}')
            AND b.origen IS NOT NULL AND b.origen != 'externo'
            AND b.destino IS NOT NULL AND b.destino != 'externo'
            AND b.viajes IS NOT NULL AND b.viajes_km IS NOT NULL
        GROUP BY b.fecha + (b.periodo::INTEGER * INTERVAL 1 HOUR), b.origen, b.destino, b.residencia;
    """

    result = execute_sql_or_cloud_run(sql_query=sql_query, **context)

    logger.info(f"Batch {batch_index} processed successfully: {len(fechas)} dates (from {fechas[0]} to {fechas[-1]})")

    return {
        "status": "success",
        "batch": date_batch,
        "batch_index": batch_index,
        "execution_name": result.get("execution_name"),
        "execution_time_seconds": result.get("execution_time_seconds")
    }
