"""
Airflow tasks for cleaning up Silver layer tables.
Includes task to drop all silver tables for maintenance or migration purposes.
"""

from airflow.sdk import task
from typing import Dict

from utils.gcp import execute_sql_or_cloud_run
from utils.utils import get_ducklake_connection


@task
def SILVER_drop_all_tables(**context) -> Dict:
    """
    Elimina todas las tablas que empiezan con 'silver_' de la base de datos.
    Útil para limpieza completa antes de recrear tablas con nueva estructura (ej: particionado).
    
    ⚠️ ADVERTENCIA: Esta tarea elimina TODOS los datos de las tablas silver.
    Solo debe ejecutarse cuando se quiera hacer una limpieza completa o migración.
    
    Returns:
    - Dict con status y lista de tablas eliminadas
    """
    print("[TASK] ⚠️  Starting DROP of all silver tables")
    print("[TASK] This will delete all data in silver tables!")
    
    con = get_ducklake_connection()
    
    # Obtener todas las tablas que empiezan con 'silver_'
    query = """
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'main'
            AND table_name LIKE 'silver_%'
        ORDER BY table_name;
    """
    
    df = con.execute(query).fetchdf()
    
    if df.empty:
        print("[TASK] No silver tables found to drop")
        return {
            "status": "success",
            "tables_dropped": [],
            "count": 0
        }
    
    tables = df['table_name'].tolist()
    print(f"[TASK] Found {len(tables)} silver tables to drop:")
    for table in tables:
        print(f"[TASK]   - {table}")
    
    # Construir SQL para hacer DROP de todas las tablas
    drop_statements = []
    for table in tables:
        drop_statements.append(f"DROP TABLE IF EXISTS {table};")
    
    sql_query = """
        -- DuckDB optimizations
        SET preserve_insertion_order=false;
        SET enable_object_cache=true;
        
        -- Drop all silver tables
        """ + "\n        ".join(drop_statements)
    
    print(f"[TASK] Executing DROP statements for {len(tables)} tables...")
    result = execute_sql_or_cloud_run(sql_query=sql_query, **context)
    
    print(f"[TASK] ✅ Successfully dropped {len(tables)} silver tables")
    print(f"[TASK] Tables dropped: {', '.join(tables)}")
    
    return {
        "status": "success",
        "tables_dropped": tables,
        "count": len(tables),
        "execution_name": result.get("execution_name"),
        "execution_time_seconds": result.get("execution_time_seconds")
    }

