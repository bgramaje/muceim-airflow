import sys
import os
from airflow.sdk import task  # type: ignore

# Add parent directory to path to import utils
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))))

from utils import get_ducklake_connection


@task.branch
def SILVER_verify_mapping_coverage(**context):
    """
    Verify if all zones in silver_zones are covered in silver_mitma_ine_mapping.
    The mapping table doesn't have a year column, so we check overall coverage.
    
    Returns task_ids to execute:
    - ['ine_all.business', 'ine_all.population', 'ine_all.income'] if coverage is incomplete (execute INE tasks)
    - 'done' if coverage is complete (skip all INE processing)
    
    If silver_zones doesn't exist, returns INE task IDs (execute all INE tasks).
    """
    print("[TASK] Verifying mapping coverage")

    con = get_ducklake_connection()

    try:
        zones_count = con.execute(f"""
            SELECT COUNT(*) AS count 
            FROM silver_ine_all 
            WHERE year = '{context['params']['start'][:4]}'
        """).fetchdf().iloc[0]['count']

        mapping_count = con.execute(f"""
            SELECT COUNT(DISTINCT(municipio_mitma)) AS count 
            FROM silver_mitma_ine_mapping
            WHERE year = '{context['params']['start'][:4]}'
        """).fetchdf().iloc[0]['count']
        
        print(f"[TASK] silver_zones count: {zones_count}")
        print(f"[TASK] silver_mitma_ine_mapping distinct municipios: {mapping_count}")
        
        coverage_complete = zones_count == mapping_count
        
        if coverage_complete:
            print("[TASK] Coverage is complete. Skipping all INE tasks and finishing")
            return 'done'
        else:
            print("[TASK] Coverage incomplete. Executing INE processing tasks")
            return ['ine_all.business', 'ine_all.population', 'ine_all.income']

    except Exception as e:
        print(f"[TASK] silver_zones table does not exist or error: {e}")
        print("[TASK] Returning INE task IDs to execute all INE tasks")
        return ['ine_all.business', 'ine_all.population', 'ine_all.income']
    
