"""
Gravity Model Analysis Tasks.

Business Question 2: Where is Transport Infrastructure Most Lacking?

This module implements a gravity model to estimate potential demand between zones
and compares it to actual MITMA data to identify underserved areas.

Formula: T_ij = k * (P_i * E_j) / d_ij^2

Where:
- T_ij: Estimated trips between zones i and j
- P_i: Population at origin zone i
- E_j: Economic activity (companies) at destination zone j
- d_ij: Distance between zones (km)
- k: Calibration constant
"""

from gold.tasks.gravity_model.base_pairs import GOLD_gravity_create_base_pairs
from gold.tasks.gravity_model.calibration import GOLD_gravity_calibrate_model
from gold.tasks.gravity_model.mismatch import GOLD_gravity_compute_mismatch
from gold.tasks.gravity_model.zone_ranking import GOLD_gravity_zone_ranking
from gold.tasks.gravity_model.export import (
    GOLD_gravity_export_results,
    GOLD_gravity_export_keplergl
)

__all__ = [
    "GOLD_gravity_create_base_pairs",
    "GOLD_gravity_calibrate_model", 
    "GOLD_gravity_compute_mismatch",
    "GOLD_gravity_zone_ranking",
    "GOLD_gravity_export_results",
    "GOLD_gravity_export_keplergl"
]

