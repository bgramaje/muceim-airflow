"""
Airflow task for loading MITMA zonification data into Silver layer.
Handles zoning (zonificación) data including geometries, names, and population
for distritos, municipios, and GAU zone types.
"""

from airflow.sdk import task # type: ignore

from utils.utils import get_ducklake_connection, load_extension
from utils.logger import get_logger


@task
def SILVER_mitma_zonification(**context):
    """
    Airflow task to transform and standarize zonification data into DuckDB for the specified type.

    Returns:
    - Dict with task status and info
    """
    logger = get_logger(__name__, context)
    logger.info("Building unified silver zonification table")
    
    con = get_ducklake_connection()

    load_extension(con, 'spatial')
  
    con.execute("""
        CREATE OR REPLACE TABLE silver_zones AS
        SELECT DISTINCT
            base.id,
            base.nombre,
            base.geometry_obj,
            base.centroid
        FROM (
            SELECT
                ID as id,
                Nombre as nombre,
                ST_Multi(ST_GeomFromText(geometry))::GEOMETRY AS geometry_obj,
                ST_Centroid(ST_Multi(ST_GeomFromText(geometry)))::GEOMETRY AS centroid
            FROM bronze_mitma_municipios
            WHERE
                ID IS NOT NULL
                AND Nombre IS NOT NULL
                AND geometry IS NOT NULL
        ) base
        INNER JOIN silver_mitma_ine_mapping s
            ON base.id = s.municipio_mitma
    """)

    sample_df = con.execute("SELECT * FROM silver_zones LIMIT 10").fetchdf()
    logger.debug(f"Sample data: {sample_df}")

    return {
        "status": "success",
        "table": "silver_zones"
    }
