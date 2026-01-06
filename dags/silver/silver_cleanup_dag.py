"""
DAG for cleaning up all Silver layer tables.
⚠️ WARNING: This DAG will delete ALL data in silver tables.

Use this DAG when you need to:
- Clean up all silver tables before recreating with new structure (e.g., partitioning)
- Reset the silver layer completely
- Perform a full migration

This DAG does NOT run automatically - it must be triggered manually.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.standard.operators.empty import EmptyOperator

from silver.misc.cleanup import SILVER_drop_all_tables


with DAG(
    dag_id="silver_cleanup",
    start_date=datetime(2025, 1, 1),
    schedule=None,  # Manual trigger only - no automatic execution
    catchup=False,
    tags=["silver", "cleanup", "maintenance"],
    description="⚠️ Cleanup DAG - Drops all silver tables. Manual trigger only.",
    default_args={
        "retries": 0,  # No retries for destructive operations
        "retry_delay": timedelta(seconds=0),
    },
    params={
        "confirmation": "I understand this will delete all silver tables"
    }
) as dag:

    start = EmptyOperator(
        task_id="start",
        doc_md="""
        ## Start Task
        
        ⚠️ **WARNING**: This DAG will delete ALL silver tables and their data.
        
        Make sure you have backups if needed before proceeding.
        """
    )
    
    drop_all_silver = SILVER_drop_all_tables.override(
        task_id="drop_all_silver_tables",
        doc_md="""
        ## Drop All Silver Tables
        
        This task will:
        1. Find all tables starting with `silver_`
        2. Execute `DROP TABLE IF EXISTS` for each table
        3. Return a summary of dropped tables
        
        **Tables that will be dropped:**
        - silver_mitma_od
        - silver_od_quality
        - silver_od_processed_dates
        - silver_overnight_stay
        - silver_people_day
        - silver_zones
        - silver_ine_all
        - silver_mitma_distances
        - silver_mitma_ine_mapping
        - And any other table starting with `silver_`
        
        ⚠️ **This action cannot be undone!**
        """
    )()
    
    done = EmptyOperator(
        task_id="done",
        doc_md="""
        ## Done
        
        All silver tables have been dropped.
        
        You can now recreate them with the new structure (e.g., with partitioning).
        """
    )
    
    start >> drop_all_silver >> done

