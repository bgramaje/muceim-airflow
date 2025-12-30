from .ine_municipios import (
    BRONZE_ine_municipios_urls,
    BRONZE_ine_municipios_create_table,
    BRONZE_ine_municipios_filter_urls,
    BRONZE_ine_municipios_insert,
)
from .ine_empresas import (
    BRONZE_ine_empresas_municipio_urls,
    BRONZE_ine_empresas_municipio_create_table,
    BRONZE_ine_empresas_municipio_filter_urls,
    BRONZE_ine_empresas_municipio_insert,
)
from .ine_poblacion import (
    BRONZE_ine_poblacion_municipio_urls,
    BRONZE_ine_poblacion_municipio_create_table,
    BRONZE_ine_poblacion_municipio_filter_urls,
    BRONZE_ine_poblacion_municipio_insert,
)
from .ine_renta import (
    BRONZE_ine_renta_urls,
    BRONZE_ine_renta_create_table,
    BRONZE_ine_renta_filter_urls,
    BRONZE_ine_renta_insert,
)
from .mitma_ine_relations import (
    BRONZE_mitma_ine_relations_urls,
    BRONZE_mitma_ine_relations_create_table,
    BRONZE_mitma_ine_relations_filter_urls,
    BRONZE_mitma_ine_relations_insert,
)

__all__ = [
    "BRONZE_ine_municipios_urls",
    "BRONZE_ine_municipios_create_table",
    "BRONZE_ine_municipios_filter_urls",
    "BRONZE_ine_municipios_insert",
    "BRONZE_ine_empresas_municipio_urls",
    "BRONZE_ine_empresas_municipio_create_table",
    "BRONZE_ine_empresas_municipio_filter_urls",
    "BRONZE_ine_empresas_municipio_insert",
    "BRONZE_ine_poblacion_municipio_urls",
    "BRONZE_ine_poblacion_municipio_create_table",
    "BRONZE_ine_poblacion_municipio_filter_urls",
    "BRONZE_ine_poblacion_municipio_insert",
    "BRONZE_ine_renta_urls",
    "BRONZE_ine_renta_create_table",
    "BRONZE_ine_renta_filter_urls",
    "BRONZE_ine_renta_insert",
    "BRONZE_mitma_ine_relations_urls",
    "BRONZE_mitma_ine_relations_create_table",
    "BRONZE_mitma_ine_relations_filter_urls",
    "BRONZE_mitma_ine_relations_insert",
]
