import sys
import os
from airflow.sdk import task

# Add parent directory to path to import utils
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))))

from utils import get_ducklake_connection


@task
def SILVER_mitma_distances():
    """
    Airflow task to calculate distances between zones
    and store them in silver_distances table.
    The distances are calculated between zones of the same type.
    """
    print("[TASK] Building silver_distances table")

    con = get_ducklake_connection()

    con.execute("""
        INSTALL spatial;
        LOAD spatial;
    """)

    con.execute("""
        CREATE OR REPLACE TABLE silver_mitma_distances AS
        SELECT
            o.id AS origin,
            d.id AS destination,
            ST_Distance_Sphere(o.centroid, d.centroid) / 1000.0 AS distance_km
        FROM silver_zones AS o
            CROSS JOIN silver_zones AS d
        WHERE o.id < d.id
    """)

    print("[TASK] silver_mitma_distances table built successfully")
    return {
        "status": "success",
        "records": 0,
        "table": "silver_mitma_distances"
    }
