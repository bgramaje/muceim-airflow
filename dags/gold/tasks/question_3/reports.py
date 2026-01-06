"""
Question 3: Functional Type Classification - Report Generation Tasks

This module contains tasks for generating visualizations and reports
for functional type analysis. These run locally as they require 
pandas/plotly/kepler processing.
"""

import sys
import os
import json
import pandas as pd
import plotly.graph_objects as go
from shapely import wkt
from keplergl import KeplerGl
from airflow.sdk import task  # type: ignore
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

# Add parent directory to path to import utils
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))))

from utils.utils import get_ducklake_connection


@task
def GOLD_generate_in_out_distribution(
    save_id: str = None,
    start_date: str = None,
    end_date: str = None,
    polygon_wkt: str = None
):
    """
    Generate an interactive Plotly chart showing in/out trip distribution by hour.
    
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
    df_types = con.execute(f"""
        SELECT 
            z.id AS zone_id,
            z.nombre AS municipality,
            ft.functional_type
        FROM gold_zone_functional_type ft
        JOIN silver_zones z 
        ON ft.zone_id = z.id
        WHERE ST_Within(z.centroid, ST_GeomFromText('{polygon_wkt}'));
    """).fetchdf()

    # Get trips in/out
    temp_df = con.execute(f"""
    WITH trips_out AS (
        SELECT
            origen_zone_id AS zone_id,
            EXTRACT(DOW FROM fecha)::int AS dow,
            EXTRACT(HOUR FROM fecha)::int AS hour,
            AVG(viajes) AS avg_out_trips
        FROM silver_mitma_od
        WHERE fecha BETWEEN '{start_date}' AND '{end_date}'
        GROUP BY 1,2,3
    ),
    trips_in AS (
        SELECT
            destino_zone_id AS zone_id,
            EXTRACT(DOW FROM fecha)::int AS dow,
            EXTRACT(HOUR FROM fecha)::int AS hour,
            AVG(viajes) AS avg_in_trips
        FROM silver_mitma_od
        WHERE fecha BETWEEN '{start_date}' AND '{end_date}'
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
    FROM all_hours;
    """).fetchdf()

    temp_df = temp_df.merge(df_types, on='zone_id', how='inner')
    df_grouped = temp_df.groupby(['municipality','functional_type','hour'], as_index=False).agg(
        avg_out_trips=('avg_out_trips','sum'),
        avg_in_trips=('avg_in_trips','sum')
    )
    df_all = df_grouped.groupby('hour', as_index=False).agg(
        avg_out_trips=('avg_out_trips','sum'),
        avg_in_trips=('avg_in_trips','sum')
    )
    df_all['municipality'] = 'All'
    df_all['functional_type'] = 'All'
    df_plot = pd.concat([df_all, df_grouped], ignore_index=True)
    municipalities = df_plot[df_plot['municipality']!='All'][['municipality','functional_type']].drop_duplicates()
    municipalities = pd.concat([pd.DataFrame({'municipality':['All'], 'functional_type':['All']}), municipalities], ignore_index=True)

    fig = go.Figure()

    for _, row in municipalities.iterrows():
        mun = row['municipality']
        ftype = row['functional_type']
        label = f"{mun}" if mun=='All' else f"{mun} ({ftype})"
        
        df_m = df_plot[df_plot['municipality']==mun].sort_values('hour')
        
        fig.add_trace(go.Scatter(
            x=df_m['hour'],
            y=df_m['avg_in_trips'],
            mode='lines+markers',
            name=f"In trips",
            visible=(mun=='All'),
            line=dict(dash='solid'),
            hovertemplate=(
                "<b>In</b><br>"
                "Hour: %{x:02d}<br>"
                "Average total trips: %{y:.2f}"
                "<extra></extra>"
            )
        ))
        fig.add_trace(go.Scatter(
            x=df_m['hour'],
            y=df_m['avg_out_trips'],
            mode='lines+markers',
            name=f"Out trips",
            visible=(mun=='All'),
            line=dict(dash='dash'),
            hovertemplate=(
                "<b>Out</b><br>"
                "Hour: %{x:02d}<br>"
                "Average total trips: %{y:.2f}"
                "<extra></extra>"
            )
        ))

    buttons = []
    for i, row in municipalities.iterrows():
        mun = row['municipality']
        ftype = row['functional_type']
        label = f"{mun}" if mun=='All' else f"{mun} ({ftype})"
        
        visibility = [False]*(2*len(municipalities))
        visibility[2*i] = True
        visibility[2*i+1] = True
        
        buttons.append(dict(
            label=label,
            method='update',
            args=[{'visible': visibility},
                {'title':f"Hourly trips - {label}",
                'yaxis': {'title':'Total trips'}}]
        ))

    fig.update_layout(
        title="Hourly trips - All",
        xaxis_title="Hour of day",
        yaxis_title="Total trips",
        xaxis=dict(
            tickmode='linear',
            tick0=0,
            dtick=1,
            range=[0,23] 
        ),
        updatemenus=[dict(
            active=0,
            buttons=buttons,
            x=0.0,
            xanchor="left",
            y=1.15,
            yanchor="top",
            direction="down",
            showactive=True
        )],
        template="plotly_white"
    )

    html_content = fig.to_html(full_html=True)
    s3 = S3Hook(aws_conn_id="rustfs_s3_conn")
    s3_key = f"gold/{save_id}/question_3/in_out_distribution.html"
    s3.load_string(
        string_data=html_content,
        key=s3_key,
        bucket_name="mitma",
        replace=True
    )

    print(f"[SUCCESS] Uploaded to s3://mitma/{s3_key}")
    return f"s3://mitma/{s3_key}"


@task
def GOLD_generate_functional_type_map(
    save_id: str = None,
    start_date: str = None,
    end_date: str = None,
    polygon_wkt: str = None
):
    """
    Generate a Kepler.gl choropleth map showing functional type classification.
    
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
    input_polygon = wkt.loads(polygon_wkt)
    centroid = input_polygon.centroid
    center_lat, center_lon = centroid.y, centroid.x

    # Query: each row contains the municipality geometry
    df = con.execute(f"""
        SELECT 
            z.nombre AS name,
            ST_AsGeoJSON(ST_Multi(z.geometry_obj)) as geometry,
            ft.functional_type
        FROM gold_zone_functional_type ft
            JOIN silver_zones z ON ft.zone_id = z.id
        WHERE ST_Within(
            z.centroid,
            ST_GeomFromText('{polygon_wkt}')
        )
    """).fetchdf()

    # Convert geometries to JSON (Polygon or MultiPolygon)
    df['geometry'] = df['geometry'].apply(lambda x: json.loads(x) if isinstance(x, str) else x)

    # Map configuration with fixed colors and center
    choropleth_config = {
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
                        "id": "municipios_layer",
                        "type": "geojson",
                        "config": {
                            "dataId": "municipios",
                            "label": "Functional Type",
                            "columns": {"geojson": "geometry"},
                            "isVisible": True,
                            "visConfig": {
                                "filled": True,
                                "strokeColor": [0, 0, 0],
                                "opacity": 0.6
                            }
                        },
                        "visualChannels": {
                            "colorField": {"name": "functional_type", "type": "string"},
                            "colorScale": "ordinal"
                        }
                    }
                ],
                "interactionConfig": {
                    "tooltip": {
                        "fieldsToShow": {
                            "municipios": ["name", "functional_type"]
                        },
                        "enabled": True
                    }
                }
            }
        }
    }

    # Create map
    map_ = KeplerGl(height=600, config=choropleth_config)
    map_.add_data(df, "municipios")

    html_content = map_._repr_html_()
    if isinstance(html_content, bytes):
        html_content = html_content.decode("utf-8")
        
    s3 = S3Hook(aws_conn_id="rustfs_s3_conn")
    s3_key = f"gold/{save_id}/question_3/functional_type_map.html"
    s3.load_string(
        string_data=html_content,
        key=s3_key,
        bucket_name="mitma",
        replace=True
    )

    print(f"[SUCCESS] Uploaded to s3://mitma/{s3_key}")
    return f"s3://mitma/{s3_key}"

