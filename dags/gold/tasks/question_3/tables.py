"""
Question 3: Functional Type Classification - Table Creation Tasks

This module contains tasks for creating gold layer tables for functional type analysis.
The heavy SQL query is executed in Cloud Run, and the sklearn clustering runs in 
the post-processing function.
"""

import pandas as pd
from sklearn.impute import SimpleImputer
from sklearn.preprocessing import StandardScaler
from sklearn.cluster import KMeans
from airflow.sdk import task  # type: ignore

from utils.gcp import execute_sql_or_cloud_run

FUNCTIONAL_TYPE_SQL = """
    WITH trips_out AS (
        SELECT
            origen_zone_id AS zone_id,
            EXTRACT(DOW FROM fecha)::int AS dow,
            EXTRACT(HOUR FROM fecha)::int AS hour,
            AVG(viajes) AS avg_out_trips
        FROM silver_mitma_od
        GROUP BY 1,2,3
    ),
    trips_in AS (
        SELECT
            destino_zone_id AS zone_id,
            EXTRACT(DOW FROM fecha)::int AS dow,
            EXTRACT(HOUR FROM fecha)::int AS hour,
            AVG(viajes) AS avg_in_trips
        FROM silver_mitma_od
        GROUP BY 1,2,3
    ),
    all_hours AS (
        SELECT 
            COALESCE(o.zone_id, i.zone_id) AS zone_id,
            COALESCE(o.dow, i.dow) AS dow,
            COALESCE(o.hour, i.hour) AS hour,
            COALESCE(o.avg_out_trips,0) AS avg_out_trips,
            COALESCE(i.avg_in_trips,0) AS avg_in_trips
        FROM trips_out o
        FULL OUTER JOIN trips_in i
            ON o.zone_id = i.zone_id
            AND o.dow = i.dow
            AND o.hour = i.hour
    )
    SELECT *
    FROM all_hours
    ORDER BY zone_id, dow, hour;
"""


def _post_process_functional_type(df, con, result_dict):
    """
    Post-processing function for functional_type table.
    Receives the DataFrame from the SQL query and performs KMeans clustering
    to classify zones as Residential or Non-residential.

    Parameters:
    - df: DataFrame result from the SQL query
    - con: DuckDB connection (for creating the table)
    - result_dict: Result dictionary from SQL execution

    Returns:
    - Dict with table name and record count
    """
    # Imports necesarios (se incluyen en el código serializado)
    import pandas as pd
    from sklearn.impute import SimpleImputer
    from sklearn.preprocessing import StandardScaler
    from sklearn.cluster import KMeans
    
    print("[TASK] Processing functional type classification with KMeans clustering")

    # Si no tenemos DataFrame, no podemos continuar
    if df is None:
        raise ValueError(
            "DataFrame is required for functional type classification")

    temp_df = df.copy()

    dow_map = {0: 'sunday', 1: 'monday', 2: 'tuesday', 3: 'wednesday',
               4: 'thursday', 5: 'friday', 6: 'saturday'}
    temp_df['day'] = temp_df['dow'].map(dow_map)

    temp_df['is_weekend'] = temp_df['day'].isin(
        ['saturday', 'sunday']).astype(int)
    temp_df['diff_out_in'] = temp_df['avg_out_trips'] - temp_df['avg_in_trips']
    features = ['avg_out_trips', 'avg_in_trips',
                'dow', 'hour', 'is_weekend', 'diff_out_in']
    X_numeric = temp_df[features]

    # Handle Missing Values
    imputer = SimpleImputer(strategy='mean')
    X_imputed = imputer.fit_transform(X_numeric)

    # Clustering with KMeans
    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X_imputed)
    kmeans = KMeans(n_clusters=2, random_state=42)
    temp_df['cluster'] = kmeans.fit_predict(X_scaled)

    # Identify each cluster
    zone_clusters = temp_df.groupby('zone_id')['cluster'].agg(
        lambda x: x.value_counts().idxmax()).reset_index()
    zone_clusters.rename(columns={'cluster': 'final_cluster'}, inplace=True)
    cluster_stats = temp_df.groupby(
        'cluster')[['avg_out_trips', 'avg_in_trips']].mean()
    industrial_cluster_id = cluster_stats.apply(
        lambda row: row.avg_out_trips > row.avg_in_trips, axis=1
    ).idxmax()

    # Set cluster name
    zone_clusters['functional_type'] = zone_clusters['final_cluster'].apply(
        lambda x: 'Residential' if x == industrial_cluster_id else 'Non-residential'
    )

    # Add to DuckLake
    gold_zones = zone_clusters[['zone_id', 'functional_type']]
    insert_values = ",\n".join([
        f"('{row.zone_id}', '{row.functional_type}')"
        for idx, row in gold_zones.iterrows()
    ])
    con.execute(f"""
        CREATE OR REPLACE TABLE gold_zone_functional_type AS
        SELECT * FROM (VALUES
            {insert_values}
        ) AS t(zone_id, functional_type)
    """)

    print(f"[TASK] Created gold_zone_functional_type")

    return {
        "table": "gold_zone_functional_type",
    }


@task
def GOLD_functional_type(**context):
    """
    Airflow task to create gold_zone_functional_type table.

    The heavy SQL query is executed in Cloud Run (if available), and the
    sklearn-based clustering runs in the post-processing function.

    Returns:
    - Dict with task status and info
    """
    print("[TASK] Building gold_zone_functional_type table (Business Question 3)")

    # Execute SQL query with post-processing function
    # The post_process_func will run in Cloud Run (if available) with the DataFrame
    result = execute_sql_or_cloud_run(
        sql_query=FUNCTIONAL_TYPE_SQL,
        post_process_func=_post_process_functional_type,
        **context
    )

    print(f"[TASK] Execution: {result.get('execution_name', 'unknown')}")

    return {
        "status": "success",
        "table": result.get("table", "gold_zone_functional_type"),
        "execution_time_seconds": result.get('execution_time_seconds', 0)
    }
