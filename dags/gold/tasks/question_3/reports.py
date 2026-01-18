"""
Question 3: Functional Type Classification - Report Generation Tasks

This module contains tasks for generating visualizations and reports
for functional type analysis. These run in Cloud Run for better performance.
"""

from airflow.sdk import task  # type: ignore

from utils.gcp import execute_sql_or_cloud_run
from utils.logger import get_logger


def _post_process_in_out_distribution(df, con, result_dict):
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
    
    temp_df = df.copy()
    
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
    
    # Upload to S3 using Airflow connection
    s3_key = f"gold/question3/{save_id}/in_out_distribution.html"
    s3_path = upload_to_s3_rustfs(
        content=html_content,
        s3_key=s3_key,
        content_type='text/html',
        bucket_name=bucket_name
    )
    
    logger.info(f"Uploaded to {s3_path}")
    return {'s3_path': s3_path}


@task
def GOLD_generate_in_out_distribution(
    save_id: str = None,
    start_date: str = None,
    end_date: str = None,
    polygon_wkt: str = None,
    bucket_name: str = None,
    **context
):
    """
    Generate an interactive Plotly chart showing in/out trip distribution by hour.
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
    logger.info("Generating in/out distribution (Cloud Run)")
    
    # Combined SQL query that gets both functional types and trips data
    sql_query = f"""
        INSTALL spatial; LOAD spatial;
        
        -- Pre-calcular el polígono una sola vez
        WITH polygon_filter AS (
            SELECT ST_GeomFromText('{polygon_wkt}') AS polygon
        ),
        -- Pre-filtrar zonas que están dentro del polígono (más eficiente que hacerlo en cada JOIN)
        filtered_zones AS (
            SELECT z.id, z.nombre
            FROM silver_zones z
            CROSS JOIN polygon_filter p
            WHERE ST_Within(z.centroid, p.polygon)
        ),
        functional_types AS (
            SELECT 
                z.id AS zone_id,
                z.nombre AS municipality,
                ft.functional_type
            FROM gold_zone_functional_type ft
            JOIN filtered_zones z ON ft.zone_id = z.id
        ),
        trips_out AS (
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
        SELECT 
            ah.zone_id,
            ah.hour,
            ah.avg_out_trips,
            ah.avg_in_trips,
            ft.municipality,
            ft.functional_type
        FROM all_hours ah
        INNER JOIN functional_types ft ON ah.zone_id = ft.zone_id;
    """
    
    # Store extra env vars in context
    if 'extra_env_vars' not in context:
        context['extra_env_vars'] = {}
    context['extra_env_vars']['REPORT_SAVE_ID'] = save_id
    context['extra_env_vars']['RUSTFS_BUCKET'] = bucket_name
    
    # Pass the function directly (not a closure) - parameters will be read from env vars inside the function
    result = execute_sql_or_cloud_run(sql_query=sql_query, post_process_func=_post_process_in_out_distribution, **context)
    return result.get('s3_path', '')


def _post_process_functional_type_map(df, con, result_dict):
    """
    Post-processing function to generate Kepler.gl choropleth map and upload to S3.
    This function runs in Cloud Run and uses Airflow connection for S3 upload.
    """
    import os
    import json
    from shapely import wkt
    from keplergl import KeplerGl
    from utils.s3 import upload_to_s3_rustfs
    
    # Get parameters from environment variables (set by execute_sql_or_cloud_run)
    save_id = os.environ.get('REPORT_SAVE_ID')
    logger = get_logger(__name__)
    polygon_wkt = os.environ.get('REPORT_POLYGON_WKT')
    bucket_name = os.environ.get('RUSTFS_BUCKET')
    
    if df is None or len(df) == 0:
        logger.warning("No data to visualize")
        return {'status': 'skipped', 'message': 'No data available'}
    
    input_polygon = wkt.loads(polygon_wkt)
    centroid = input_polygon.centroid
    center_lat, center_lon = centroid.y, centroid.x

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
    
    # Upload to S3 using Airflow connection
    s3_key = f"gold/question3/{save_id}/functional_type_map.html"
    s3_path = upload_to_s3_rustfs(
        content=html_content,
        s3_key=s3_key,
        content_type='text/html',
        bucket_name=bucket_name
    )
    
    logger.info(f"Uploaded to {s3_path}")
    return {'s3_path': s3_path}


@task
def GOLD_generate_functional_type_map(
    save_id: str = None,
    start_date: str = None,
    end_date: str = None,
    polygon_wkt: str = None,
    bucket_name: str = None,
    **context
):
    """
    Generate a Kepler.gl choropleth map showing functional type classification.
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
    logger.info("Generating functional type map (Cloud Run)")
    
    sql_query = f"""
        INSTALL spatial; LOAD spatial;
        
        -- Pre-calcular el polígono una sola vez
        WITH polygon_filter AS (
            SELECT ST_GeomFromText('{polygon_wkt}') AS polygon
        ),
        -- Pre-filtrar zonas que están dentro del polígono (más eficiente que hacerlo en cada JOIN)
        filtered_zones AS (
            SELECT z.id, z.nombre, z.geometry_obj
            FROM silver_zones z
            CROSS JOIN polygon_filter p
            WHERE ST_Within(z.centroid, p.polygon)
        )
        SELECT 
            z.nombre AS name,
            ST_AsGeoJSON(ST_Multi(z.geometry_obj)) as geometry,
            ft.functional_type
        FROM gold_zone_functional_type ft
        JOIN filtered_zones z ON ft.zone_id = z.id
    """
    
    # Store extra env vars in context
    if 'extra_env_vars' not in context:
        context['extra_env_vars'] = {}
    context['extra_env_vars']['REPORT_SAVE_ID'] = save_id
    context['extra_env_vars']['REPORT_POLYGON_WKT'] = polygon_wkt
    context['extra_env_vars']['RUSTFS_BUCKET'] = bucket_name
    
    # Pass the function directly (not a closure) - parameters will be read from env vars inside the function
    result = execute_sql_or_cloud_run(sql_query=sql_query, post_process_func=_post_process_functional_type_map, **context)
    return result.get('s3_path', '')

