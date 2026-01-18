from airflow.sdk import task  # type: ignore

from utils.utils import get_ducklake_connection


@task
def SILVER_ine_empresas():
    """
    Airflow task to create silver_ine_empresas_municipio table.
    """
    print("[TASK] Building silver_ine_empresas_municipio table")

    con = get_ducklake_connection()

    # User provided SQL from notebook
    query = f"""
        CREATE OR REPLACE TABLE silver_ine_empresas_municipio AS (
        WITH empresas_parsed AS (
            SELECT 
                replace(lower(strip_accents(
                    TRIM(
                        COALESCE(
                            NULLIF(split_part(split_part(e.Nombre, '.', 1), '/', 2), ''),
                            split_part(split_part(e.Nombre, '.', 1), '/', 1)
                        )
                    )
                )), '-', ' ') AS nombre,
                COALESCE(CAST(data_item.Valor AS DOUBLE), 0) AS valor
            FROM bronze_ine_empresas_municipio e,
                UNNEST(e.Data) AS t(data_item)
            WHERE e.Nombre IS NOT NULL AND e.Nombre ILIKE '%CNAE%'
        ), 
        empresas_mitma AS (
            SELECT 
                m.municipio_mitma AS zone_id,
                ep.nombre,
                MAX(ep.valor) AS valor
            FROM silver_mitma_ine_mapping m 
            LEFT JOIN empresas_parsed ep
                ON ep.nombre ILIKE m.nombre
            GROUP BY zone_id, ep.nombre
        )
        SELECT 
            zone_id,
            COALESCE(SUM(valor), 0) AS empresas
        FROM empresas_mitma
        GROUP BY zone_id
    )
    """
    
    con.execute(query)

    count = con.execute("SELECT COUNT(*) as count FROM silver_ine_empresas_municipio").fetchdf()
    record_count = int(count['count'].iloc[0])
    
    print(f"[TASK] Created silver_ine_empresas_municipio with {record_count:,} records")

    return {
        "status": "success",
        "records": record_count,
        "table": "silver_ine_empresas_municipio"
    }
