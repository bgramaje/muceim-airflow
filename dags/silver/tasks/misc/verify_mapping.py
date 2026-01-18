from airflow.sdk import task  # type: ignore

from utils.utils import get_ducklake_connection


@task.branch
def SILVER_verify_mapping_coverage(**context):
    """
    Verify if all zones in silver_ine_all are covered in silver_mitma_ine_mapping.
    Performs a difference operation to find zones that are missing in the mapping.
    
    Returns task_ids to execute:
    - ['ine_processing.business', 'ine_processing.population', 'ine_processing.income'] if there are differences (execute INE tasks)
    - 'ine_processing.done' if no differences found (skip all INE processing)
    
    If silver_ine_all doesn't exist, returns INE task IDs (execute all INE tasks).
    """

    con = get_ducklake_connection()
    
    # Safely extract year from params.start, handling None case
    start_param = context['params'].get('start')
    year = start_param[:4] if start_param else None
    
    if not year:
        print("[TASK] No start date provided in params. Processing all INE tasks.")
        return ['ine_processing.business', 'ine_processing.population', 'ine_processing.income']

    try:
        # Find zones in silver_ine_all that are NOT 
        # in silver_mitma_ine_mapping for the given year
        difference_df = con.execute(f"""
            SELECT DISTINCT a.id
            FROM silver_ine_all a
            WHERE a.year = '{year}'
            AND a.id NOT IN (
                SELECT DISTINCT municipio_mitma 
                FROM silver_mitma_ine_mapping
            )
        """).fetchdf()
        
        difference_count = len(difference_df)
        
        print(f"[TASK] Zones in silver_ine_all (year={year}) not found in silver_mitma_ine_mapping: {difference_count}")
        
        if difference_count > 0:
            print(f"[TASK] Missing zones found. Processing INE tasks: {difference_df['id'].tolist()}")
            return ['ine_processing.business', 'ine_processing.population', 'ine_processing.income']
        else:
            print(f"[TASK] All zones are covered in mapping. Skipping INE processing.")
            return 'ine_processing.done'

    except Exception as e:
        print(f"silver_ine_all table does not exist or error: {e}")
        return ['ine_processing.business', 'ine_processing.population', 'ine_processing.income']
    finally:
        con.close()