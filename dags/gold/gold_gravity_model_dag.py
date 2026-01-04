"""
Gold Layer DAG: Gravity Model Analysis

Business Question 2: Where is Transport Infrastructure Most Lacking?

This DAG implements a gravity model to:
1. Estimate potential demand between zones using the formula:
   T_ij = k * (P_i * E_j) / d_ij^2
   
2. Compare estimated demand vs actual MITMA trips

3. Identify underserved areas by computing mismatch ratios

4. Generate rankings and Kepler.gl visualization

Parameters:
- start_date: Start of analysis period (YYYY-MM-DD)
- end_date: End of analysis period (YYYY-MM-DD)

Example trigger:
    airflow dags trigger gold_gravity_model \\
        --conf '{"start_date": "2023-03-01", "end_date": "2023-03-31"}'

Output tables:
- gold_gravity_base_pairs: Base O-D pairs with distances and INE data
- gold_gravity_calibrated: Calibrated model with k constant
- gold_gravity_mismatch: Mismatch ratios per O-D pair
- gold_gravity_zone_ranking: Zone-level aggregated metrics
- gold_gravity_worst_zones: Top 50 worst-served zones
- gold_gravity_best_zones: Top 50 best-served zones
- gold_gravity_critical_corridors: Top 100 O-D pairs with highest unmet demand
- gold_gravity_export_*: Export tables for visualization

Output files (S3):
- keplergl_dashboard_*.html: Standalone HTML dashboard with embedded data
- zones_*.geojson: Zone polygons for Kepler.gl import
- corridors_*.geojson: O-D flow lines for Kepler.gl import
- *.parquet: Raw data files for BI tools
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Param
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import TaskGroup

from gold.tasks.gravity_model import (
    GOLD_gravity_create_base_pairs,
    GOLD_gravity_calibrate_model,
    GOLD_gravity_compute_mismatch,
    GOLD_gravity_zone_ranking,
    GOLD_gravity_export_results,
    GOLD_gravity_export_keplergl
)


# Default date range (last month)
DEFAULT_START = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
DEFAULT_END = datetime.now().strftime('%Y-%m-%d')


with DAG(
    dag_id="gold_gravity_model",
    start_date=datetime(2025, 1, 1),
    schedule=None,  # Manual trigger only
    catchup=False,
    tags=["gold", "analytics", "gravity-model", "transport-infrastructure"],
    description="Gravity model analysis to identify underserved transport areas",
    doc_md=__doc__,
    params={
        "start_date": Param(
            default=DEFAULT_START,
            type="string",
            format="date",
            title="Start Date",
            description="Start date for analysis period (YYYY-MM-DD)"
        ),
        "end_date": Param(
            default=DEFAULT_END,
            type="string",
            format="date", 
            title="End Date",
            description="End date for analysis period (YYYY-MM-DD)"
        )
    },
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=2),
        "execution_timeout": timedelta(hours=2),
    },
    render_template_as_native_obj=True,
) as dag:
    
    # ============ START ============
    start = EmptyOperator(task_id="start")
    
    # ============ GRAVITY MODEL PIPELINE ============
    with TaskGroup(group_id="gravity_model", dag=dag) as gravity_group:
        
        # Step 1: Create base O-D pairs table
        base_pairs = GOLD_gravity_create_base_pairs.override(
            task_id="create_base_pairs"
        )()
        
        # Step 2: Calibrate model (compute k)
        calibrate = GOLD_gravity_calibrate_model.override(
            task_id="calibrate_model"
        )()
        
        # Step 3: Compute mismatch ratios
        mismatch = GOLD_gravity_compute_mismatch.override(
            task_id="compute_mismatch"
        )()
        
        # Step 4: Zone-level ranking
        ranking = GOLD_gravity_zone_ranking.override(
            task_id="zone_ranking"
        )()
        
        # Define internal dependencies
        base_pairs >> calibrate >> mismatch >> ranking
    
    # ============ EXPORT PIPELINE ============
    with TaskGroup(group_id="export", dag=dag) as export_group:
        
        # Export to Parquet and tables
        export_results = GOLD_gravity_export_results.override(
            task_id="export_parquet"
        )()
        
        # Generate Kepler.gl HTML dashboard with embedded data
        export_keplergl = GOLD_gravity_export_keplergl.override(
            task_id="export_keplergl"
        )()
        
        # Kepler.gl export depends on parquet export (needs tables created)
        export_results >> export_keplergl
    
    # ============ DONE ============
    done = EmptyOperator(task_id="done")
    
    # ============ DEPENDENCIES ============
    start >> gravity_group >> export_group >> done

