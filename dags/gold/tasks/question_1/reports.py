"""
Question 1: Typical Day - Report Generation Tasks

This module contains tasks for generating visualizations and reports
for typical day analysis. These run in Cloud Run for better performance.
"""

from airflow.sdk import task  # type: ignore

from utils.gcp import execute_sql_or_cloud_run
from utils.logger import get_logger


def _post_process_typical_day_map(df, con, result_dict):
    """
    Post-processing function to generate Kepler.gl map and upload to S3.
    This function runs in Cloud Run and uses Airflow connection for S3 upload.
    """
    import os
    import pandas as pd
    from shapely import wkt
    from keplergl import KeplerGl
    from utils.s3 import upload_to_s3_rustfs
    
    # Get parameters from environment variables (set by execute_sql_or_cloud_run)
    save_id = os.environ.get('REPORT_SAVE_ID')
    polygon_wkt = os.environ.get('REPORT_POLYGON_WKT')
    logger = get_logger(__name__)
    bucket_name = os.environ.get('RUSTFS_BUCKET')
    
    if df is None or len(df) == 0:
        logger.warning("No data to visualize")
        return {'status': 'skipped', 'message': 'No data available'}
    
    # Process DataFrame
    df["origin_lon"] = df["origin_lon"].astype("float64")
    df["origin_lat"] = df["origin_lat"].astype("float64")
    df["dest_lon"] = df["dest_lon"].astype("float64")
    df["dest_lat"] = df["dest_lat"].astype("float64")

    input_polygon = wkt.loads(polygon_wkt)
    centroid = input_polygon.centroid
    center_lat, center_lon = centroid.y, centroid.x

    arc_config = {
        "version": "v1",
        "config": {
            "mapState": {
                "latitude": center_lat,
                "longitude": center_lon,
                "zoom": 10 
            },
            "visState": {
                "layers": [
                    {
                        "id": "arc_layer",
                        "type": "arc",
                        "config": {
                            "dataId": "trips",
                            "label": "Trips Arc",
                            "columns": {
                                "lat0": "origin_lat",
                                "lng0": "origin_lon",
                                "lat1": "dest_lat",
                                "lng1": "dest_lon"
                            },
                            "isVisible": True,
                            "visConfig": {
                                "thickness": 2,
                                "colorRange": {
                                    "name": "ColorBrewer Reds",
                                    "type": "sequential",
                                    "category": "Uber",
                                    "colors": ["#fee5d9","#fcae91","#fb6a4a","#de2d26","#a50f15"]
                                }
                            }
                        },
                        "visualChannels": {
                            "colorField": {"name": "avg_trips", "type": "real"},
                            "colorScale": "quantile"
                        }
                    }
                ],
                "interactionConfig": {
                    "tooltip": {
                        "fieldsToShow": {
                            "trips": ["origin", "destination", "avg_trips"]
                        },
                        "enabled": True
                    }
                }
            }
        }
    }
    
    map_ = KeplerGl(height=600, config=arc_config)
    map_.add_data(df, "trips")

    html_content = map_._repr_html_()
    if isinstance(html_content, bytes):
        html_content = html_content.decode("utf-8")
    
    # Upload to S3 using Airflow connection
    s3_key = f"gold/question1/{save_id}/typical_day_map.html"
    s3_path = upload_to_s3_rustfs(
        content=html_content,
        s3_key=s3_key,
        content_type='text/html',
        bucket_name=bucket_name
    )
    
    logger.info(f"Uploaded to {s3_path}")
    return {'s3_path': s3_path}


@task
def GOLD_generate_typical_day_map(
    save_id: str = None,
    start_date: str = None,
    end_date: str = None,
    polygon_wkt: str = None,
    bucket_name: str = None,
    **context
):
    """
    Generate a Kepler.gl map showing typical day OD flows.
    Executes in Cloud Run for better performance.
    
    Parameters:
    - save_id: Unique identifier for the report
    - start_date: Start date for filtering (YYYY-MM-DD)
    - end_date: End date for filtering (YYYY-MM-DD)
    - polygon_wkt: WKT polygon to filter geographic area
    
    Returns:
    - S3 path to the generated HTML map
    """
    logger = get_logger(__name__, context)
    logger.info("Generating typical day map (Cloud Run)")
    
    sql_query = f"""
        INSTALL spatial; LOAD spatial;
        
        -- Pre-calcular el polígono una sola vez
        WITH polygon_filter AS (
            SELECT ST_GeomFromText('{polygon_wkt}') AS polygon
        ),
        -- Pre-filtrar zonas que están dentro del polígono (más eficiente que hacerlo en cada JOIN)
        filtered_zones AS (
            SELECT 
                z.id,
                z.nombre,
                z.centroid,
                ST_X(z.centroid) AS lon,
                ST_Y(z.centroid) AS lat
            FROM silver_zones z
            CROSS JOIN polygon_filter p
            WHERE ST_Within(z.centroid, p.polygon)
        ),
        -- Filtrar datos de la tabla gold y hacer JOINs solo con zonas filtradas
        od_base AS (
            SELECT
                z1.nombre as origin,
                z2.nombre as destination,
                z1.lon AS origin_lon,
                z1.lat AS origin_lat,
                z2.lon AS dest_lon,
                z2.lat AS dest_lat,
                td.hour,
                td.avg_trips
            FROM gold_typical_day_od_hourly td
            JOIN filtered_zones z1 ON td.origin_id = z1.id
            JOIN filtered_zones z2 ON td.destination_id = z2.id
            WHERE td.date BETWEEN '{start_date}' AND '{end_date}'
        )
        SELECT
            origin, 
            destination,
            origin_lon,
            origin_lat,
            dest_lon,
            dest_lat,
            AVG(avg_trips) AS avg_trips
        FROM od_base
        GROUP BY origin, destination, origin_lon, origin_lat, dest_lon, dest_lat
        HAVING AVG(avg_trips) >= 10;
    """
    
    if 'extra_env_vars' not in context:
        context['extra_env_vars'] = {}
    context['extra_env_vars']['REPORT_SAVE_ID'] = save_id
    context['extra_env_vars']['REPORT_POLYGON_WKT'] = polygon_wkt
    context['extra_env_vars']['RUSTFS_BUCKET'] = bucket_name
    
    # Pass the function directly (not a closure) - parameters will be read from env vars inside the function
    result = execute_sql_or_cloud_run(sql_query=sql_query, post_process_func=_post_process_typical_day_map, **context)
    return result.get('s3_path', '')


def _post_process_top_origins(df, con, result_dict):
    """
    Post-processing function to generate bar chart and upload to S3.
    This function runs in Cloud Run and uses Airflow connection for S3 upload.
    """
    import os
    from io import BytesIO
    import matplotlib
    matplotlib.use('Agg')  # Use non-interactive backend
    import matplotlib.pyplot as plt
    from utils.s3 import upload_to_s3_rustfs
    
    # Get parameters from environment variables (set by execute_sql_or_cloud_run)
    logger = get_logger(__name__)
    save_id = os.environ.get('REPORT_SAVE_ID')
    bucket_name = os.environ.get('RUSTFS_BUCKET')
    
    if df is None or len(df) == 0:
        logger.warning("No data to visualize")
        return {'status': 'skipped', 'message': 'No data available'}
    
    plt.figure(figsize=(12, 5))
    plt.barh(
        df["origin"],
        df["total_trips"]
    )
    plt.gca().invert_yaxis()
    plt.title("Top 10 Origins with more Average Daily Trips")
    plt.xlabel("Average Trips")
    plt.ylabel("Origin")
    plt.tight_layout()

    buffer = BytesIO()
    plt.savefig(buffer, format="png", dpi=150, bbox_inches="tight")
    plt.close()
    buffer.seek(0)

    # Upload to S3 using Airflow connection
    s3_key = f"gold/question1/{save_id}/top_origins.png"
    s3_path = upload_to_s3_rustfs(
        content=buffer.getvalue(),
        s3_key=s3_key,
        content_type='image/png',
        bucket_name=bucket_name
    )
    
    logger.info(f"Uploaded to {s3_path}")
    return {'s3_path': s3_path}


@task
def GOLD_generate_top_origins(
    save_id: str = None,
    start_date: str = None,
    end_date: str = None,
    polygon_wkt: str = None,
    bucket_name: str = None,
    **context
):
    """
    Generate a bar chart showing top 10 origins by average daily trips.
    Executes in Cloud Run for better performance.
    
    Parameters:
    - save_id: Unique identifier for the report
    - start_date: Start date for filtering (YYYY-MM-DD)
    - end_date: End date for filtering (YYYY-MM-DD)
    - polygon_wkt: WKT polygon to filter geographic area
    
    Returns:
    - S3 path to the generated PNG image
    """
    logger = get_logger(__name__, context)
    logger.info("Generating typical day top origins (Cloud Run)")
    
    sql_query = f"""
        INSTALL spatial; LOAD spatial;
        
        -- Pre-calcular el polígono una sola vez
        WITH polygon_filter AS (
            SELECT ST_GeomFromText('{polygon_wkt}') AS polygon
        ),
        -- Pre-filtrar zonas que están dentro del polígono
        filtered_zones AS (
            SELECT z.id, z.nombre
            FROM silver_zones z
            CROSS JOIN polygon_filter p
            WHERE ST_Within(z.centroid, p.polygon)
        ),
        -- Filtrar datos y hacer JOIN solo con zonas filtradas
        od_filtered AS (
            SELECT
                z1.nombre AS origin,
                td.avg_trips
            FROM gold_typical_day_od_hourly td
            JOIN filtered_zones z1 ON td.origin_id = z1.id
            WHERE td.date BETWEEN '{start_date}' AND '{end_date}'
        )
        SELECT
            origin,
            AVG(avg_trips) AS total_trips
        FROM od_filtered
        GROUP BY origin
        ORDER BY total_trips DESC
        LIMIT 10;
    """
    
    # Store extra env vars in context
    if 'extra_env_vars' not in context:
        context['extra_env_vars'] = {}
    context['extra_env_vars']['REPORT_SAVE_ID'] = save_id
    context['extra_env_vars']['RUSTFS_BUCKET'] = bucket_name
    
    # Pass the function directly (not a closure) - parameters will be read from env vars inside the function
    result = execute_sql_or_cloud_run(sql_query=sql_query, post_process_func=_post_process_top_origins, **context)
    return result.get('s3_path', '')


def _post_process_hourly_distribution(df, con, result_dict):
    """
    Post-processing function to generate Plotly chart and upload to S3.
    This function runs in Cloud Run and uses Airflow connection for S3 upload.
    """
    import os
    import pandas as pd
    import plotly.graph_objects as go
    from utils.s3 import upload_to_s3_rustfs
    
    # Get parameters from environment variables (set by execute_sql_or_cloud_run)
    logger = get_logger(__name__)
    save_id = os.environ.get('REPORT_SAVE_ID')
    bucket_name = os.environ.get('RUSTFS_BUCKET')
    
    if df is None or len(df) == 0:
        logger.warning("No data to visualize")
        return {'status': 'skipped', 'message': 'No data available'}
    
    df_hourly_distribution = df.copy()
    
    df_all = (
        df_hourly_distribution
        .groupby("hour", as_index=False)["total_trips"]
        .sum()
    )
    df_all["municipality"] = "All municipalities"
    df_plot = pd.concat([df_all, df_hourly_distribution], ignore_index=True)
    municipalities = df_plot["municipality"].unique()

    fig = go.Figure()
    for i, mun in enumerate(municipalities):
        df_m = df_plot[df_plot["municipality"] == mun]
        fig.add_trace(
            go.Scatter(
                x=df_m["hour"],
                y=df_m["total_trips"],
                mode="lines+markers",
                name=mun,
                visible=(i == 0),
                hovertemplate=(
                    "Hour: %{x:02d}<br>"
                    "Average total trips: %{y:.2f}"
                    "<extra></extra>"
                )
            )
        )

    buttons = []
    for i, mun in enumerate(municipalities):
        visibility = [False] * len(municipalities)
        visibility[i] = True

        buttons.append(
            dict(
                label=mun,
                method="update",
                args=[
                    {"visible": visibility},
                    {"title": f"Hourly Distribution of Average Daily Trips – {mun}"}
                ]
            )
        )
        
    fig.update_layout(
        title="Hourly Distribution of Average Daily Trips - All municipalities",
        xaxis_title="Hour",
        yaxis_title="Average Trips",
        updatemenus=[
            dict(
                buttons=buttons,
                direction="down",
                showactive=True,
                x=0.0,
                xanchor="left",
                y=1.15,
                yanchor="top"
            )
        ],
        xaxis=dict(tickmode="linear"),
        template="plotly_white"
    )

    html_content = fig.to_html(full_html=True)
    
    # Upload to S3 using Airflow connection
    s3_key = f"gold/question1/{save_id}/hourly_distribution.html"
    s3_path = upload_to_s3_rustfs(
        content=html_content,
        s3_key=s3_key,
        content_type='text/html',
        bucket_name=bucket_name
    )
    
    logger.info(f"Uploaded to {s3_path}")
    return {'s3_path': s3_path}


@task
def GOLD_generate_hourly_distribution(
    save_id: str = None,
    start_date: str = None,
    end_date: str = None,
    polygon_wkt: str = None,
    bucket_name: str = None,
    **context
):
    """
    Generate an interactive Plotly chart showing hourly trip distribution.
    Executes in Cloud Run for better performance.
    
    Parameters:
    - save_id: Unique identifier for the report
    - start_date: Start date for filtering (YYYY-MM-DD)
    - end_date: End date for filtering (YYYY-MM-DD)
    - polygon_wkt: WKT polygon to filter geographic area
    
    Returns:
    - S3 path to the generated HTML chart
    """
    logger = get_logger(__name__, context)
    logger.info("Generating typical day hourly distribution (Cloud Run)")
    
    sql_query = f"""
        INSTALL spatial; LOAD spatial;
        
        -- Pre-calcular el polígono una sola vez
        WITH polygon_filter AS (
            SELECT ST_GeomFromText('{polygon_wkt}') AS polygon
        ),
        -- Pre-filtrar zonas que están dentro del polígono
        filtered_zones AS (
            SELECT z.id, z.nombre
            FROM silver_zones z
            CROSS JOIN polygon_filter p
            WHERE ST_Within(z.centroid, p.polygon)
        ),
        -- Filtrar datos y hacer JOIN solo con zonas filtradas
        od_filtered AS (
            SELECT
                z1.nombre AS municipality,
                td.hour,
                td.avg_trips
            FROM gold_typical_day_od_hourly td
            JOIN filtered_zones z1 ON td.origin_id = z1.id
            WHERE td.date BETWEEN '{start_date}' AND '{end_date}'
        )
        SELECT
            municipality,
            hour,
            AVG(avg_trips) AS total_trips
        FROM od_filtered
        GROUP BY municipality, hour
        ORDER BY municipality, hour;
    """
    
    # Store extra env vars in context
    if 'extra_env_vars' not in context:
        context['extra_env_vars'] = {}
    context['extra_env_vars']['REPORT_SAVE_ID'] = save_id
    context['extra_env_vars']['RUSTFS_BUCKET'] = bucket_name
    
    # Pass the function directly (not a closure) - parameters will be read from env vars inside the function
    result = execute_sql_or_cloud_run(sql_query=sql_query, post_process_func=_post_process_hourly_distribution, **context)
    return result.get('s3_path', '')

