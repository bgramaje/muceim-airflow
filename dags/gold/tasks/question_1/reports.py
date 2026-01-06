"""
Question 1: Typical Day - Report Generation Tasks

This module contains tasks for generating visualizations and reports
for typical day analysis. These run locally as they require 
pandas/matplotlib/plotly processing.
"""

import sys
import os
from io import BytesIO
import pandas as pd
from shapely import wkt
from keplergl import KeplerGl
import matplotlib.pyplot as plt
import plotly.graph_objects as go
from airflow.sdk import task  # type: ignore
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

# Add parent directory to path to import utils
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))))

from utils.utils import get_ducklake_connection


@task
def GOLD_generate_typical_day_map(
    save_id: str = None,
    start_date: str = None,
    end_date: str = None,
    polygon_wkt: str = None
):
    """
    Generate a Kepler.gl map showing typical day OD flows.
    
    Parameters:
    - save_id: Unique identifier for the report
    - start_date: Start date for filtering (YYYY-MM-DD)
    - end_date: End date for filtering (YYYY-MM-DD)
    - polygon_wkt: WKT polygon to filter geographic area
    
    Returns:
    - S3 path to the generated HTML map
    """
    print("[TASK] Generating typical day map")
    con = get_ducklake_connection()
    df = con.execute(f"""
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
    """).fetchdf()
    
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
        
    s3 = S3Hook(aws_conn_id="rustfs_s3_conn")
    s3_key = f"gold/{save_id}/question_1/typical_day_map.html"
    s3.load_string(
        string_data=html_content,
        key=s3_key,
        bucket_name="mitma",
        replace=True
    )

    print(f"[SUCCESS] Uploaded to s3://mitma/{s3_key}")
    return f"s3://mitma/{s3_key}"


@task
def GOLD_generate_top_origins(
    save_id: str = None,
    start_date: str = None,
    end_date: str = None,
    polygon_wkt: str = None
):
    """
    Generate a bar chart showing top 10 origins by average daily trips.
    
    Parameters:
    - save_id: Unique identifier for the report
    - start_date: Start date for filtering (YYYY-MM-DD)
    - end_date: End date for filtering (YYYY-MM-DD)
    - polygon_wkt: WKT polygon to filter geographic area
    
    Returns:
    - S3 path to the generated PNG image
    """
    print("[TASK] Generating typical day top origins")
    con = get_ducklake_connection()
    df_top_origins = con.execute(f"""
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
    """).fetchdf()

    plt.figure(figsize=(12, 5))
    plt.barh(
        df_top_origins["origin"],
        df_top_origins["total_trips"]
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

    s3 = S3Hook(aws_conn_id="rustfs_s3_conn")
    s3_key = f"gold/{save_id}/question_1/top_origins.png"
    s3.load_bytes(
        bytes_data=buffer.getvalue(),
        key=s3_key,
        bucket_name="mitma",
        replace=True
    )
    print(f"[SUCCESS] Uploaded to s3://mitma/{s3_key}")

    return f"s3://mitma/{s3_key}"


@task
def GOLD_generate_hourly_distribution(
    save_id: str = None,
    start_date: str = None,
    end_date: str = None,
    polygon_wkt: str = None
):
    """
    Generate an interactive Plotly chart showing hourly trip distribution.
    
    Parameters:
    - save_id: Unique identifier for the report
    - start_date: Start date for filtering (YYYY-MM-DD)
    - end_date: End date for filtering (YYYY-MM-DD)
    - polygon_wkt: WKT polygon to filter geographic area
    
    Returns:
    - S3 path to the generated HTML chart
    """
    print("[TASK] Generating typical day hourly distribution")
    con = get_ducklake_connection()
    df_hourly_distribution = con.execute(f"""
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
    """).fetchdf()

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
    s3 = S3Hook(aws_conn_id="rustfs_s3_conn")
    s3_key = f"gold/{save_id}/question_1/hourly_distribution.html"
    s3.load_string(
        string_data=html_content,
        key=s3_key,
        bucket_name="mitma",
        replace=True
    )

    print(f"[SUCCESS] Uploaded to s3://mitma/{s3_key}")
    return f"s3://mitma/{s3_key}"

