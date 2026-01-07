"""
Gold Layer Tasks Package
"""

from .question_1.tables import GOLD_typical_day
from .question_1.reports import (
    GOLD_generate_typical_day_map,
    GOLD_generate_top_origins,
    GOLD_generate_hourly_distribution
)
from .question_2.tables import GOLD_gravity_model
from .question_2.reports import (
    GOLD_generate_mismatch_distribution,
    GOLD_generate_table,
    GOLD_generate_mismatch_map
)
from .question_3.tables import GOLD_functional_type
from .question_3.reports import (
    GOLD_generate_in_out_distribution,
    GOLD_generate_functional_type_map
)
from .verify_s3_connection import GOLD_verify_s3_connection

__all__ = [
    # Question 1 - Typical Day
    "GOLD_typical_day",
    "GOLD_generate_typical_day_map",
    "GOLD_generate_top_origins",
    "GOLD_generate_hourly_distribution",
    # Question 2 - Gravity Model
    "GOLD_gravity_model",
    "GOLD_generate_mismatch_distribution",
    "GOLD_generate_table",
    "GOLD_generate_mismatch_map",
    # Question 3 - Functional Type
    "GOLD_functional_type",
    "GOLD_generate_in_out_distribution",
    "GOLD_generate_functional_type_map",
    # Verification
    "GOLD_verify_s3_connection",
]

