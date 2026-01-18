"""
TaskGroup for Silver INE data transformation tasks.
Orchestrates verify_mapping_coverage, business, population, income, ine_all, and cleanup tasks.
"""

from types import SimpleNamespace

from airflow.sdk import task_group

from silver.tasks.ine import (
    SILVER_ine_empresas,
    SILVER_ine_poblacion_municipio,
    SILVER_ine_renta,
    SILVER_ine_all,
    CLEANUP_intermediate_ine_tables,
)
from silver.tasks.misc import SILVER_verify_mapping_coverage


@task_group(group_id="ine_all")
def ine_all():
    """
    TaskGroup for Silver INE data transformation.
    Includes verify_mapping_coverage, business, population, income, ine_all, and cleanup tasks.
    
    Returns:
    - SimpleNamespace with all INE tasks including coverage_check
    """
    coverage_check = SILVER_verify_mapping_coverage.override(
        task_id="verify_mapping_coverage"
    )()
    
    ine_emp = SILVER_ine_empresas.override(task_id="business")()
    ine_pob = SILVER_ine_poblacion_municipio.override(task_id="population")()
    ine_renta = SILVER_ine_renta.override(task_id="income")()
    
    ine_all = SILVER_ine_all.override(task_id="ine_all")()
    cleanup_ine = CLEANUP_intermediate_ine_tables.override(task_id="cleanup_intermediate_ine_tables")()
    
    coverage_check >> ine_emp
    coverage_check >> ine_pob
    coverage_check >> ine_renta

    [ine_emp, ine_pob, ine_renta] >> ine_all >> cleanup_ine
    
    return SimpleNamespace(
        coverage_check=coverage_check,
        business=ine_emp,
        population=ine_pob,
        income=ine_renta,
        ine_all=ine_all,
        cleanup=cleanup_ine,
    )

