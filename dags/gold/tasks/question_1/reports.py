"""
Question 1: Typical Day - Report Generation Tasks

This module contains tasks for generating visualizations and reports
for typical day analysis. These run in Cloud Run for better performance.
"""

import sys
import os
from io import BytesIO
from airflow.sdk import task  # type: ignore
from airflow.sdk import Variable

# Add parent directory to path to import utils
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))))

from utils.utils import get_ducklake_connection
from utils.gcp import execute_sql_or_cloud_run


def _post_process_typical_day_map(df, con, result_dict):
    """
    Post-processing function to generate Kepler.gl map and upload to S3.
    This function runs in Cloud Run and uses boto3 for S3 upload.
    """
    import os
    import boto3
    from botocore.config import Config
    import pandas as pd
    from shapely import wkt
    from keplergl import KeplerGl
    
    # Get parameters from environment variables (set by execute_sql_or_cloud_run)
    save_id = os.environ.get('REPORT_SAVE_ID')
    polygon_wkt = os.environ.get('REPORT_POLYGON_WKT')
    
    if df is None or len(df) == 0:
        print("[WARNING] No data to visualize")
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
    
    # Get S3 credentials from environment variables (available in Cloud Run)
    s3_endpoint = os.environ.get("S3_ENDPOINT", "rustfs:9000")
    rustfs_user = os.environ.get("RUSTFS_USER")
    rustfs_password = os.environ.get("RUSTFS_PASSWORD")
    rustfs_ssl = os.environ.get("RUSTFS_SSL", "false").lower() == "true"
    bucket_name = os.environ.get("RUSTFS_BUCKET", "mitma")
    
    # Configure boto3 S3 client
    endpoint_url = f"{'https' if rustfs_ssl else 'http'}://{s3_endpoint}"
    s3_client = boto3.client(
        's3',
        endpoint_url=endpoint_url,
        aws_access_key_id=rustfs_user,
        aws_secret_access_key=rustfs_password,
        config=Config(signature_version='s3v4')
    )
    
    # Upload to S3
    s3_key = f"gold/question1/{save_id}/typical_day_map.html"
    s3_client.put_object(
        Bucket=bucket_name,
        Key=s3_key,
        Body=html_content.encode('utf-8'),
        ContentType='text/html'
    )
    
    s3_path = f"s3://{bucket_name}/{s3_key}"
    print(f"[SUCCESS] Uploaded to {s3_path}")
    return {'s3_path': s3_path}


@task
def GOLD_generate_typical_day_map(
    save_id: str = None,
    start_date: str = None,
    end_date: str = None,
    polygon_wkt: str = None,
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
    print("[TASK] Generating typical day map (Cloud Run)")
    
    sql_query = f"""
        INSTALL spatial; LOAD spatial;
        
        WITH od_base AS (
            SELECT
                z1.nombre as origin,
                z2.nombre as destination,
                ST_AsGeoJSON(z1.centroid) AS origin_geojson,
                ST_AsGeoJSON(z2.centroid) AS dest_geojson,
                hour,
                avg_trips
            FROM gold_typical_day_od_hourly td
            JOIN silver_zones z1 ON td.origin_id = z1.id
            JOIN silver_zones z2 ON td.destination_id = z2.id
            WHERE date BETWEEN '{start_date}' AND '{end_date}'
                AND ST_Within(
                    z1.centroid,
                    ST_GeomFromText('{polygon_wkt}')
                )
                AND ST_Within(
                    z2.centroid,
                    ST_GeomFromText('{polygon_wkt}')
                )
        )
        SELECT
            origin, 
            destination,
            (origin_geojson::JSON->'coordinates')[0] AS origin_lon,
            (origin_geojson::JSON->'coordinates')[1] AS origin_lat,
            (dest_geojson::JSON->'coordinates')[0] AS dest_lon,
            (dest_geojson::JSON->'coordinates')[1] AS dest_lat,
            AVG(avg_trips) AS avg_trips
        FROM od_base
        WHERE avg_trips >= 10
        GROUP BY origin, destination, origin_lon, origin_lat, dest_lon, dest_lat;
    """
    
    # Store extra env vars in context for exec_gcp_ducklake_executor
    if 'extra_env_vars' not in context:
        context['extra_env_vars'] = {}
    context['extra_env_vars']['REPORT_SAVE_ID'] = save_id
    context['extra_env_vars']['REPORT_POLYGON_WKT'] = polygon_wkt
    
    # Pass the function directly (not a closure) - parameters will be read from env vars inside the function
    result = execute_sql_or_cloud_run(sql_query=sql_query, post_process_func=_post_process_typical_day_map, **context)
    return result.get('s3_path', '')


def _post_process_top_origins(df, con, result_dict):
    """
    Post-processing function to generate bar chart and upload to S3.
    This function runs in Cloud Run and uses boto3 for S3 upload.
    """
    import os
    from io import BytesIO
    import boto3
    from botocore.config import Config
    import matplotlib
    matplotlib.use('Agg')  # Use non-interactive backend
    import matplotlib.pyplot as plt
    
    # Get parameters from environment variables (set by execute_sql_or_cloud_run)
    save_id = os.environ.get('REPORT_SAVE_ID')
    
    if df is None or len(df) == 0:
        print("[WARNING] No data to visualize")
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

    # Get S3 credentials from environment variables
    s3_endpoint = os.environ.get("S3_ENDPOINT", "rustfs:9000")
    rustfs_user = os.environ.get("RUSTFS_USER")
    rustfs_password = os.environ.get("RUSTFS_PASSWORD")
    rustfs_ssl = os.environ.get("RUSTFS_SSL", "false").lower() == "true"
    bucket_name = os.environ.get("RUSTFS_BUCKET", "mitma")
    
    # Configure boto3 S3 client
    endpoint_url = f"{'https' if rustfs_ssl else 'http'}://{s3_endpoint}"
    s3_client = boto3.client(
        's3',
        endpoint_url=endpoint_url,
        aws_access_key_id=rustfs_user,
        aws_secret_access_key=rustfs_password,
        config=Config(signature_version='s3v4')
    )
    
    # Upload to S3
    s3_key = f"gold/question1/{save_id}/top_origins.png"
    s3_client.put_object(
        Bucket=bucket_name,
        Key=s3_key,
        Body=buffer.getvalue(),
        ContentType='image/png'
    )
    
    s3_path = f"s3://{bucket_name}/{s3_key}"
    print(f"[SUCCESS] Uploaded to {s3_path}")
    return {'s3_path': s3_path}


@task
def GOLD_generate_top_origins(
    save_id: str = None,
    start_date: str = None,
    end_date: str = None,
    polygon_wkt: str = None,
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
    print("[TASK] Generating typical day top origins (Cloud Run)")
    
    sql_query = f"""
        INSTALL spatial; LOAD spatial;
        
        WITH od_filtered AS (
            SELECT
                z1.nombre AS origin,
                avg_trips
            FROM gold_typical_day_od_hourly td
            JOIN silver_zones z1 ON td.origin_id = z1.id
            WHERE date BETWEEN '{start_date}' AND '{end_date}'
            AND ST_Within(
                    z1.centroid,
                    ST_GeomFromText('{polygon_wkt}')
            )
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
    
    # Pass the function directly (not a closure) - parameters will be read from env vars inside the function
    result = execute_sql_or_cloud_run(sql_query=sql_query, post_process_func=_post_process_top_origins, **context)
    return result.get('s3_path', '')


def _post_process_hourly_distribution(df, con, result_dict):
    """
    Post-processing function to generate Plotly chart and upload to S3.
    This function runs in Cloud Run and uses boto3 for S3 upload.
    """
    import os
    import boto3
    from botocore.config import Config
    import pandas as pd
    import plotly.graph_objects as go
    
    # Get parameters from environment variables (set by execute_sql_or_cloud_run)
    save_id = os.environ.get('REPORT_SAVE_ID')
    
    if df is None or len(df) == 0:
        print("[WARNING] No data to visualize")
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
    
    # Get S3 credentials from environment variables
    s3_endpoint = os.environ.get("S3_ENDPOINT", "rustfs:9000")
    rustfs_user = os.environ.get("RUSTFS_USER")
    rustfs_password = os.environ.get("RUSTFS_PASSWORD")
    rustfs_ssl = os.environ.get("RUSTFS_SSL", "false").lower() == "true"
    bucket_name = os.environ.get("RUSTFS_BUCKET", "mitma")
    
    # Configure boto3 S3 client
    endpoint_url = f"{'https' if rustfs_ssl else 'http'}://{s3_endpoint}"
    s3_client = boto3.client(
        's3',
        endpoint_url=endpoint_url,
        aws_access_key_id=rustfs_user,
        aws_secret_access_key=rustfs_password,
        config=Config(signature_version='s3v4')
    )
    
    # Upload to S3
    s3_key = f"gold/question1/{save_id}/hourly_distribution.html"
    s3_client.put_object(
        Bucket=bucket_name,
        Key=s3_key,
        Body=html_content.encode('utf-8'),
        ContentType='text/html'
    )
    
    s3_path = f"s3://{bucket_name}/{s3_key}"
    print(f"[SUCCESS] Uploaded to {s3_path}")
    return {'s3_path': s3_path}


@task
def GOLD_generate_hourly_distribution(
    save_id: str = None,
    start_date: str = None,
    end_date: str = None,
    polygon_wkt: str = None,
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
    print("[TASK] Generating typical day hourly distribution (Cloud Run)")
    
    sql_query = f"""
        INSTALL spatial; LOAD spatial;
        
        WITH od_filtered AS (
            SELECT
                z1.nombre AS municipality,
                hour,
                avg_trips
            FROM gold_typical_day_od_hourly td
                JOIN silver_zones z1 ON td.origin_id = z1.id
            WHERE date BETWEEN '{start_date}' AND '{end_date}'
            AND ST_Within(
                z1.centroid,
                ST_GeomFromText('{polygon_wkt}')
            )
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
    
    # Pass the function directly (not a closure) - parameters will be read from env vars inside the function
    result = execute_sql_or_cloud_run(sql_query=sql_query, post_process_func=_post_process_hourly_distribution, **context)
    return result.get('s3_path', '')

