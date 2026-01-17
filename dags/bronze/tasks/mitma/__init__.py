from .mitma_od import (
    BRONZE_mitma_od_urls,
    BRONZE_mitma_od_filter_urls,
    BRONZE_mitma_od_get_and_filter_urls,
    BRONZE_mitma_od_create_partitioned_table,
    BRONZE_mitma_od_download_batch,
    BRONZE_mitma_od_process_batch,
    BRONZE_mitma_od_cleanup_batch,
    BRONZE_mitma_od_finalize,
)
from .mitma_people_day import (
    BRONZE_mitma_people_day_urls,
    BRONZE_mitma_people_day_create_table,
    BRONZE_mitma_people_day_filter_urls,
    BRONZE_mitma_people_day_insert,
)
from .mitma_overnights import (
    BRONZE_mitma_overnight_stay_urls,
    BRONZE_mitma_overnight_stay_create_table,
    BRONZE_mitma_overnight_stay_filter_urls,
    BRONZE_mitma_overnight_stay_insert,
)
from .mitma_zonification import (
    BRONZE_mitma_zonification_urls,
    BRONZE_mitma_zonification,
)
from .mitma_ine_relations import BRONZE_mitma_ine_relations

__all__ = [
    "BRONZE_mitma_od_urls",
    "BRONZE_mitma_od_filter_urls",
    "BRONZE_mitma_od_get_and_filter_urls",
    "BRONZE_mitma_od_create_partitioned_table",
    "BRONZE_mitma_od_download_batch",
    "BRONZE_mitma_od_process_batch",
    "BRONZE_mitma_od_cleanup_batch",
    "BRONZE_mitma_od_finalize",
    # People Day tasks
    "BRONZE_mitma_people_day_urls",
    "BRONZE_mitma_people_day_create_table",
    "BRONZE_mitma_people_day_filter_urls",
    "BRONZE_mitma_people_day_insert",
    # Overnight Stay tasks
    "BRONZE_mitma_overnight_stay_urls",
    "BRONZE_mitma_overnight_stay_create_table",
    "BRONZE_mitma_overnight_stay_filter_urls",
    "BRONZE_mitma_overnight_stay_insert",
    # Zonification tasks
    "BRONZE_mitma_zonification_urls",
    "BRONZE_mitma_zonification",
    # MITMA-INE Relations task (local, lightweight)
    "BRONZE_mitma_ine_relations",
]
