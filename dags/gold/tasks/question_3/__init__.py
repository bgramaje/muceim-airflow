"""
Question 3: Functional Type Classification Tasks
"""

from .tables import GOLD_functional_type
from .reports import (
    GOLD_generate_in_out_distribution,
    GOLD_generate_functional_type_map
)

__all__ = [
    "GOLD_functional_type",
    "GOLD_generate_in_out_distribution",
    "GOLD_generate_functional_type_map",
]

