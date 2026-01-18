"""
Question 2: Gravity Model - Report Generation Tasks

This module contains tasks for generating visualizations and reports
for gravity model analysis. These run in Cloud Run for better performance.
"""

from airflow.sdk import task  # type: ignore

from utils.gcp import execute_sql_or_cloud_run


def _post_process_mismatch_distribution(df, con, result_dict):
    """
    Post-processing function to generate histograms and upload to S3.
    This function runs in Cloud Run and uses boto3 for S3 upload.
    """
    import os
    from io import BytesIO
    import boto3
    from botocore.config import Config
    import numpy as np
    import matplotlib
    matplotlib.use('Agg')  # Use non-interactive backend
    import matplotlib.pyplot as plt
    import seaborn as sns
    
    # Get parameters from environment variables (set by execute_sql_or_cloud_run)
    save_id = os.environ.get('REPORT_SAVE_ID')
    
    if df is None or len(df) == 0:
        print("[WARNING] No data to visualize")
        return {'status': 'skipped', 'message': 'No data available'}
    
    mm_ratios = np.asarray(df["mm_ratio"].to_list())

    fig, axs = plt.subplots(1, 2, figsize=(12, 5))

    # Full distribution
    sns.histplot(mm_ratios, bins=50, kde=True, ax=axs[0])
    axs[0].set_xlabel("Mismatch Ratio (Actual / Estimated)")
    axs[0].set_ylabel("Frequency")
    axs[0].set_title("Distribution of Mismatch Ratios Between Zones")

    # Truncated distribution (≤ 10)
    sns.histplot(mm_ratios[mm_ratios <= 10], bins=50, kde=True, ax=axs[1])
    axs[1].axvline(1, linestyle='--', label='Ratio = 1')
    axs[1].set_xlabel("Mismatch Ratio (Actual / Estimated)")
    axs[1].set_ylabel("Frequency")
    axs[1].set_title("Distribution of Mismatch Ratios Between Zones (≤ 10)")
    axs[1].legend()

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
    s3_key = f"gold/question2/{save_id}/mismatch_distribution.png"
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
def GOLD_generate_mismatch_distribution(
    save_id: str = None,
    start_date: str = None,
    end_date: str = None,
    polygon_wkt: str = None,
    **context
):
    """
    Generate histograms showing the distribution of mismatch ratios.
    Executes in Cloud Run for better performance.
    
    Parameters:
    - save_id: Unique identifier for the report
    - start_date: Start date for filtering (YYYY-MM-DD)
    - end_date: End date for filtering (YYYY-MM-DD)
    - polygon_wkt: WKT polygon to filter geographic area
    
    Returns:
    - S3 path to the generated PNG image
    """
    print("[TASK] Generating mismatch distribution (Cloud Run)")
    
    sql_query = f"""
        INSTALL spatial; LOAD spatial;
        
        -- Pre-calcular el polígono una sola vez
        WITH polygon_filter AS (
            SELECT ST_GeomFromText('{polygon_wkt}') AS polygon
        ),
        -- Pre-filtrar zonas que están dentro del polígono (más eficiente que hacerlo en cada JOIN)
        filtered_zones AS (
            SELECT z.id
            FROM silver_zones z
            CROSS JOIN polygon_filter p
            WHERE ST_Within(z.centroid, p.polygon)
        ),
        -- Filtrar datos de la tabla gold y hacer JOINs solo con zonas filtradas
        mismatch_filtered AS (
            SELECT
                AVG(mismatch_ratio) AS mm_ratio
            FROM gold_gravity_mismatch gmm
            JOIN filtered_zones z ON gmm.origin_id = z.id
            WHERE date BETWEEN '{start_date}' AND '{end_date}'
            GROUP BY gmm.origin_id, gmm.destination_id
        )
        SELECT
            mm_ratio
        FROM mismatch_filtered;
    """
    
    # Store extra env vars in context
    if 'extra_env_vars' not in context:
        context['extra_env_vars'] = {}
    context['extra_env_vars']['REPORT_SAVE_ID'] = save_id
    
    # Pass the function directly (not a closure) - parameters will be read from env vars inside the function
    result = execute_sql_or_cloud_run(sql_query=sql_query, post_process_func=_post_process_mismatch_distribution, **context)
    return result.get('s3_path', '')


def _post_process_table(df, con, result_dict):
    """
    Post-processing function to generate HTML table and upload to S3.
    This function runs in Cloud Run and uses boto3 for S3 upload.
    """
    import os
    import json
    import boto3
    from botocore.config import Config
    import pandas as pd
    
    # Get parameters from environment variables (set by execute_sql_or_cloud_run)
    save_id = os.environ.get('REPORT_SAVE_ID')
    
    if df is None or len(df) == 0:
        print("[WARNING] No data to visualize")
        return {'status': 'skipped', 'message': 'No data available'}
    
    df["actual_trips"] = df["actual_trips"].round(2)
    df["estimated_trips"] = df["estimated_trips"].round(2)
    df["mm_ratio"] = df["mm_ratio"].round(4)

    mm_min = float(df["mm_ratio"].min())
    mm_max = float(df["mm_ratio"].max())

    data_json = df.to_dict(orient="records")

    html = f"""
    <!DOCTYPE html>
    <html>
    <head>
    <meta charset="utf-8">
    <title>Mismatch Ratios</title>

    <link rel="stylesheet"
        href="https://cdn.jsdelivr.net/npm/nouislider@15.7.0/dist/nouislider.min.css">
    <link rel="stylesheet"
        href="https://cdn.datatables.net/1.13.8/css/jquery.dataTables.min.css">

    <style>
        body {{
            font-family: Arial, sans-serif;
            margin: 30px;
        }}
        #slider {{
            margin: 30px 0;
        }}
    </style>
    </head>

    <body>

    <h2>Origin-Destination Mismatch Ratios</h2>

    <p>
    Mismatch ratio range:
    <strong><span id="min-val"></span></strong> -
    <strong><span id="max-val"></span></strong>
    </p>

    <div id="slider"></div>

    <table id="mm-table" class="display" style="width:100%">
    <thead>
    <tr>
        <th>Origin</th>
        <th>Destination</th>
        <th>Actual trips</th>
        <th>Estimated trips</th>
        <th>Mismatch ratio</th>
    </tr>
    </thead>
    <tbody></tbody>
    </table>

    <script src="https://code.jquery.com/jquery-3.7.1.min.js"></script>
    <script src="https://cdn.jsdelivr.net/npm/nouislider@15.7.0/dist/nouislider.min.js"></script>
    <script src="https://cdn.datatables.net/1.13.8/js/jquery.dataTables.min.js"></script>

    <script>
    // Data from Python
    const data = {json.dumps(data_json)};

    // Initialize DataTable
    const table = $('#mm-table').DataTable({{
        data: data,
        columns: [
            {{ data: 'origin_name' }},
            {{ data: 'destination_name' }},
            {{ data: 'actual_trips' }},
            {{ data: 'estimated_trips' }},
            {{ data: 'mm_ratio' }}
        ],
        order: [[4, 'asc']],
        pageLength: 25
    }});

    // Initialize slider
    const slider = document.getElementById('slider');

    noUiSlider.create(slider, {{
        start: [{mm_min}, {mm_max}],
        connect: true,
        range: {{
            min: {mm_min},
            max: {mm_max}
        }},
        tooltips: true
    }});

    // Filtering logic
    function filterTable(min, max) {{
        table.rows().every(function () {{
            const mm = this.data().mm_ratio;
            if (mm >= min && mm <= max) {{
                $(this.node()).show();
            }} else {{
                $(this.node()).hide();
            }}
        }});
    }}

    slider.noUiSlider.on('update', function (values) {{
        const min = parseFloat(values[0]);
        const max = parseFloat(values[1]);

        document.getElementById('min-val').innerText = min.toFixed(3);
        document.getElementById('max-val').innerText = max.toFixed(3);

        filterTable(min, max);
    }});
    </script>

    </body>
    </html>
    """
    
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
    s3_key = f"gold/question2/{save_id}/mismatch_table.html"
    s3_client.put_object(
        Bucket=bucket_name,
        Key=s3_key,
        Body=html.encode('utf-8'),
        ContentType='text/html'
    )
    
    s3_path = f"s3://{bucket_name}/{s3_key}"
    print(f"[SUCCESS] Uploaded to {s3_path}")
    return {'s3_path': s3_path}


@task
def GOLD_generate_table(
    save_id: str = None,
    start_date: str = None,
    end_date: str = None,
    polygon_wkt: str = None,
    **context
):
    """
    Generate an interactive HTML table with mismatch ratios and filtering.
    Executes in Cloud Run for better performance.
    
    Parameters:
    - save_id: Unique identifier for the report
    - start_date: Start date for filtering (YYYY-MM-DD)
    - end_date: End date for filtering (YYYY-MM-DD)
    - polygon_wkt: WKT polygon to filter geographic area
    
    Returns:
    - S3 path to the generated HTML table
    """
    print("[TASK] Generating table of gravity model (Cloud Run)")
    
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
        -- Filtrar datos de la tabla gold y hacer JOINs solo con zonas filtradas
        mismatch_filtered AS (
            SELECT
                z1.nombre AS origin_name,
                z2.nombre AS destination_name,
                AVG(actual_trips) AS actual_trips,
                AVG(estimated_trips) AS estimated_trips,
                AVG(mismatch_ratio) AS mm_ratio
            FROM gold_gravity_mismatch gmm
            JOIN filtered_zones z1 ON gmm.origin_id = z1.id
            JOIN filtered_zones z2 ON gmm.destination_id = z2.id
            WHERE date BETWEEN '{start_date}' AND '{end_date}'
            GROUP BY 1, 2
        )
        SELECT *
        FROM mismatch_filtered
        ORDER BY mm_ratio;
    """
    
    # Store extra env vars in context
    if 'extra_env_vars' not in context:
        context['extra_env_vars'] = {}
    context['extra_env_vars']['REPORT_SAVE_ID'] = save_id
    
    # Pass the function directly (not a closure) - parameters will be read from env vars inside the function
    result = execute_sql_or_cloud_run(sql_query=sql_query, post_process_func=_post_process_table, **context)
    return result.get('s3_path', '')


def _post_process_mismatch_map(df, con, result_dict):
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
    
    def classify_mm(x):
        if x < 0.25:
            return "very_low"
        elif x < 0.75:
            return "low"
        elif x <= 1.25:
            return "balanced"
        elif x <= 2.5:
            return "high"
        else:
            return "very_high"
    
    df["origin_lon"] = df["origin_lon"].astype("float64")
    df["origin_lat"] = df["origin_lat"].astype("float64")
    df["dest_lon"] = df["dest_lon"].astype("float64")
    df["dest_lat"] = df["dest_lat"].astype("float64")
    df["service_class"] = df["mm_ratio"].apply(classify_mm)
    category_order = ["very_low", "low", "balanced", "high", "very_high"]
    df["service_class"] = pd.Categorical(df["service_class"], categories=category_order, ordered=True)

    input_polygon = wkt.loads(polygon_wkt)
    centroid = input_polygon.centroid
    center_lat, center_lon = centroid.y, centroid.x

    arc_config_v2 = {
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
                                    "name": "Service Level",
                                    "type": "ordinal",
                                    "category": "Custom",
                                    "colors": [
                                        "#a50f15",  # very_low  -> rojo
                                        "#fcae91",  # low       -> amarillo
                                        "#2ca25f",  # balanced  -> verde
                                        "#fcae91",  # high      -> amarillo
                                        "#a50f15"   # very_high -> rojo
                                    ]
                                }
                            }
                        },
                        "visualChannels": {
                            "colorField": {
                                "name": "service_class",
                                "type": "string"
                            },
                            "colorScale": "ordinal"
                        }
                    }
                ],
                "interactionConfig": {
                    "tooltip": {
                        "fieldsToShow": {
                            "trips": ["origin", "destination", "actual_trips", "estimated_trips"]
                        },
                        "enabled": True
                    }
                }
            }
        }
    }

    map_ = KeplerGl(height=600, config=arc_config_v2)
    map_.add_data(df, "trips")

    html_content = map_._repr_html_()
    if isinstance(html_content, bytes):
        html_content = html_content.decode("utf-8")
    
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
    s3_key = f"gold/question2/{save_id}/mismatch_map.html"
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
def GOLD_generate_mismatch_map(
    save_id: str = None,
    start_date: str = None,
    end_date: str = None,
    polygon_wkt: str = None,
    **context
):
    """
    Generate a Kepler.gl map showing mismatch ratios between zones.
    Executes in Cloud Run for better performance.
    
    Parameters:
    - save_id: Unique identifier for the report
    - start_date: Start date for filtering (YYYY-MM-DD)
    - end_date: End date for filtering (YYYY-MM-DD)
    - polygon_wkt: WKT polygon to filter geographic area
    
    Returns:
    - S3 path to the generated HTML map
    """
    print("[TASK] Generating mismatch map (Cloud Run)")
    
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
        mismatch_base AS (
            SELECT
                z1.nombre as origin,
                z2.nombre as destination,
                date,
                z1.lon AS origin_lon,
                z1.lat AS origin_lat,
                z2.lon AS dest_lon,
                z2.lat AS dest_lat,
                actual_trips,
                estimated_trips,
                mismatch_ratio
            FROM gold_gravity_mismatch gmm
            JOIN filtered_zones z1 ON gmm.origin_id = z1.id
            JOIN filtered_zones z2 ON gmm.destination_id = z2.id
            WHERE date BETWEEN '{start_date}' AND '{end_date}'
        )
        SELECT
            origin, 
            destination,
            origin_lon,
            origin_lat,
            dest_lon,
            dest_lat,
            AVG(actual_trips) AS actual_trips,
            AVG(estimated_trips) AS estimated_trips,
            AVG(mismatch_ratio) AS mm_ratio
        FROM mismatch_base
        GROUP BY origin, destination, origin_lon, origin_lat, dest_lon, dest_lat;
    """
    
    # Store extra env vars in context
    if 'extra_env_vars' not in context:
        context['extra_env_vars'] = {}
    context['extra_env_vars']['REPORT_SAVE_ID'] = save_id
    context['extra_env_vars']['REPORT_POLYGON_WKT'] = polygon_wkt
    
    # Pass the function directly (not a closure) - parameters will be read from env vars inside the function
    result = execute_sql_or_cloud_run(sql_query=sql_query, post_process_func=_post_process_mismatch_map, **context)
    return result.get('s3_path', '')

