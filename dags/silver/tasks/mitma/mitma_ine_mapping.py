
from utils.utils import get_ducklake_connection
from airflow.sdk import task  # type: ignore


@task
def SILVER_mitma_ine_mapping():
    """
    Airflow task to create silver_mitma_ine_mapping table.
    """
    con = get_ducklake_connection()

    query = """
    CREATE OR REPLACE TABLE silver_mitma_ine_mapping AS (
        SELECT DISTINCT
            replace(lower(strip_accents(
                TRIM(
                    COALESCE(
                        NULLIF(split_part(split_part(m.Nombre, '.', 1), '/', 2), ''),
                        split_part(split_part(m.Nombre, '.', 1), '/', 1)
                    )
                )
            )), '-', ' ') AS nombre,
            m.Codigo as codigo_ine,
            r.municipio_mitma
        FROM bronze_ine_municipios m
        JOIN bronze_mitma_ine_relations r
            ON m.Codigo = r.municipio_ine
        WHERE r.municipio_mitma IS NOT NULL
            AND m.Codigo IS NOT NULL
            AND nombre IS NOT NULL
        ORDER BY m.Codigo
    )
    """

    con.execute(query)

    show_df = con.execute(f"""
        SELECT * 
        FROM silver_mitma_ine_mapping 
        LIMIT 10
    """).fetchdf()
    print(show_df)

    return {
        "status": "success",
        "table": "silver_mitma_ine_mapping"
    }
