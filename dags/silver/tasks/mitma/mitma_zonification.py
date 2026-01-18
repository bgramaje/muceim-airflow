"""
Airflow task for loading MITMA zonification data into Silver layer.
Handles zoning (zonificación) data including geometries, names, and population
for distritos, municipios, and GAU zone types.
"""

from airflow.sdk import task # type: ignore

from utils.utils import get_ducklake_connection, load_extension


@task
def SILVER_mitma_zonification():
    """
    Airflow task to transform and standarize zonification data into DuckDB for the specified type.

    Returns:
    - Dict with task status and info
    """
    print("[TASK] Building unified silver zonification table")
    
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

    print(con.execute("SELECT * FROM silver_zones LIMIT 10").fetchdf())

    return {
        "status": "success",
        "table": "silver_zones"
    }
