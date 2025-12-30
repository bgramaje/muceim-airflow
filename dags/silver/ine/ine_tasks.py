"""
TaskGroup for Silver INE data transformation tasks.
Orchestrates business, population, income, ine_all, and cleanup tasks.
"""

from types import SimpleNamespace

from airflow.decorators import task_group

from silver.ine import (
    SILVER_ine_empresas,
    SILVER_ine_poblacion_municipio,
    SILVER_ine_renta,
    SILVER_ine_all,
    CLEANUP_intermediate_ine_tables,
)


@task_group(group_id="ine_all")
def ine_all():
    """
    TaskGroup for Silver INE data transformation.
    Includes business, population, income, ine_all, and cleanup tasks.
    
    Returns:
    - SimpleNamespace with all INE tasks
    """
    ine_emp = SILVER_ine_empresas.override(task_id="business")()
    ine_pob = SILVER_ine_poblacion_municipio.override(task_id="population")()
    ine_renta = SILVER_ine_renta.override(task_id="income")()
    
    ine_all = SILVER_ine_all.override(task_id="ine_all")()
    cleanup_ine = CLEANUP_intermediate_ine_tables.override(task_id="cleanup_intermediate_ine_tables")()
    
    # Business, population, and income can run in parallel
    # ine_all depends on all three completing (external dependency on mitma_zonif handled outside)
    # cleanup runs after ine_all
    [ine_emp, ine_pob, ine_renta] >> ine_all >> cleanup_ine
    
    return SimpleNamespace(
        business=ine_emp,
        population=ine_pob,
        income=ine_renta,
        ine_all=ine_all,
        cleanup=cleanup_ine,
    )

