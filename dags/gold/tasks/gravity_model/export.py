"""
Task: Export Results for Visualization

Exports gravity model results to Kepler.gl HTML format for interactive visualization.
Generates standalone HTML files that can be opened directly in a browser.
"""

from airflow.sdk import task
from typing import Dict, Any, List
import json

from utils.gcp import execute_sql_or_cloud_run
from utils.utils import get_ducklake_connection


# Kepler.gl HTML template
KEPLER_HTML_TEMPLATE = '''
<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8"/>
    <title>{title}</title>
    <link rel="stylesheet" href="https://unpkg.com/kepler.gl@3.0.0/dist/stylesheet.css"/>
    <style>
        body {{margin: 0; padding: 0; overflow: hidden;}}
        #app {{position: absolute; width: 100%; height: 100%;}}
    </style>
</head>
<body>
    <div id="app"></div>
    <script src="https://unpkg.com/react@18.2.0/umd/react.production.min.js"></script>
    <script src="https://unpkg.com/react-dom@18.2.0/umd/react-dom.production.min.js"></script>
    <script src="https://unpkg.com/kepler.gl@3.0.0/dist/kepler.gl.min.js"></script>
    <script>
        const config = {config};
        const datasets = {datasets};
        
        const app = (function createApp(config, datasets) {{
            const KeplerGl = window.KeplerGl;
            
            // Convert datasets to Kepler format
            const keplerDatasets = datasets.map(d => ({{
                info: {{id: d.id, label: d.label}},
                data: {{
                    fields: d.fields.map(f => ({{name: f.name, type: f.type, format: f.format || ''}})),
                    rows: d.rows
                }}
            }}));
            
            const loadedData = KeplerGl.KeplerGlSchema.getDatasetFromData(keplerDatasets);
            
            const store = KeplerGl.createStore({{
                mapboxApiAccessToken: ''
            }});
            
            // Add data and config
            store.dispatch(KeplerGl.addDataToMap({{
                datasets: keplerDatasets,
                config: config,
                options: {{
                    centerMap: true,
                    readOnly: false
                }}
            }}));
            
            const App = React.createElement(
                KeplerGl.KeplerGl,
                {{
                    id: "map",
                    mapboxApiAccessToken: '',
                    store: store,
                    width: window.innerWidth,
                    height: window.innerHeight
                }}
            );
            
            ReactDOM.render(App, document.getElementById('app'));
        }})(config, datasets);
        
        window.addEventListener('resize', function() {{
            location.reload();
        }});
    </script>
</body>
</html>
'''

# Simpler Kepler HTML that works without mapbox token
KEPLER_SIMPLE_HTML_TEMPLATE = '''
<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8"/>
    <title>{title}</title>
    <style>
        body {{
            margin: 0;
            padding: 20px;
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
            background: #1a1a2e;
            color: #eee;
        }}
        .header {{
            text-align: center;
            margin-bottom: 30px;
        }}
        .header h1 {{
            color: #00d4ff;
            margin-bottom: 10px;
        }}
        .summary {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
            gap: 20px;
            margin-bottom: 30px;
        }}
        .card {{
            background: #16213e;
            border-radius: 12px;
            padding: 20px;
            text-align: center;
        }}
        .card h3 {{
            color: #888;
            font-size: 14px;
            margin: 0 0 10px 0;
            text-transform: uppercase;
        }}
        .card .value {{
            font-size: 32px;
            font-weight: bold;
        }}
        .critical {{ color: #ff4757; }}
        .underserved {{ color: #ffa502; }}
        .balanced {{ color: #2ed573; }}
        .well-served {{ color: #1e90ff; }}
        .instructions {{
            background: #16213e;
            border-radius: 12px;
            padding: 20px;
            margin-bottom: 30px;
        }}
        .instructions h2 {{
            color: #00d4ff;
            margin-top: 0;
        }}
        .instructions code {{
            background: #0f0f23;
            padding: 2px 8px;
            border-radius: 4px;
            color: #ffa502;
        }}
        .data-section {{
            background: #16213e;
            border-radius: 12px;
            padding: 20px;
            margin-bottom: 30px;
        }}
        .data-section h2 {{
            color: #00d4ff;
            margin-top: 0;
        }}
        table {{
            width: 100%;
            border-collapse: collapse;
            margin-top: 15px;
        }}
        th, td {{
            padding: 12px;
            text-align: left;
            border-bottom: 1px solid #2a2a4a;
        }}
        th {{
            background: #0f0f23;
            color: #00d4ff;
            font-weight: 600;
        }}
        tr:hover {{
            background: #1f1f3a;
        }}
        .badge {{
            padding: 4px 12px;
            border-radius: 20px;
            font-size: 12px;
            font-weight: bold;
        }}
        .badge.critical {{ background: rgba(255,71,87,0.2); color: #ff4757; }}
        .badge.underserved {{ background: rgba(255,165,2,0.2); color: #ffa502; }}
        .badge.balanced {{ background: rgba(46,213,115,0.2); color: #2ed573; }}
        .badge.well_served {{ background: rgba(30,144,255,0.2); color: #1e90ff; }}
        .badge.over_served {{ background: rgba(156,39,176,0.2); color: #ab47bc; }}
        .download-btn {{
            display: inline-block;
            background: #00d4ff;
            color: #1a1a2e;
            padding: 12px 24px;
            border-radius: 8px;
            text-decoration: none;
            font-weight: bold;
            margin: 10px 5px;
            cursor: pointer;
            border: none;
        }}
        .download-btn:hover {{
            background: #00b8e6;
        }}
    </style>
</head>
<body>
    <div class="header">
        <h1>🚗 Gravity Model Analysis</h1>
        <p>Transport Infrastructure Service Level Assessment</p>
        <p><strong>Period:</strong> {start_date} to {end_date}</p>
    </div>
    
    <div class="summary">
        <div class="card">
            <h3>Total Zones</h3>
            <div class="value">{total_zones}</div>
        </div>
        <div class="card">
            <h3>Critical Zones</h3>
            <div class="value critical">{zones_critical}</div>
        </div>
        <div class="card">
            <h3>Underserved Zones</h3>
            <div class="value underserved">{zones_underserved}</div>
        </div>
        <div class="card">
            <h3>Balanced Zones</h3>
            <div class="value balanced">{zones_balanced}</div>
        </div>
        <div class="card">
            <h3>Well Served Zones</h3>
            <div class="value well-served">{zones_well_served}</div>
        </div>
        <div class="card">
            <h3>Calibration (k)</h3>
            <div class="value" style="font-size: 18px;">{k_constant}</div>
        </div>
    </div>
    
    <div class="instructions">
        <h2>📊 How to Visualize in Kepler.gl</h2>
        <ol>
            <li>Go to <a href="https://kepler.gl/demo" target="_blank" style="color: #00d4ff;">kepler.gl/demo</a></li>
            <li>Click "Get Started"</li>
            <li>Download the GeoJSON files below and drag them into Kepler.gl</li>
            <li>Configure layers:
                <ul>
                    <li><strong>Zones:</strong> Use <code>mismatch_global</code> for color (diverging scale, center at 1.0)</li>
                    <li><strong>Corridors:</strong> Use Arc layer with <code>demand_gap</code> for stroke width</li>
                </ul>
            </li>
        </ol>
        <button class="download-btn" onclick="downloadGeoJSON('zones')">⬇️ Download Zones GeoJSON</button>
        <button class="download-btn" onclick="downloadGeoJSON('corridors')">⬇️ Download Corridors GeoJSON</button>
        <button class="download-btn" onclick="downloadCSV('worst')">⬇️ Download Worst Zones CSV</button>
    </div>
    
    <div class="data-section">
        <h2>🔴 Top 20 Worst-Served Zones (Priority for Investment)</h2>
        <table>
            <thead>
                <tr>
                    <th>Rank</th>
                    <th>Zone</th>
                    <th>Population</th>
                    <th>Companies</th>
                    <th>Mismatch</th>
                    <th>Demand Gap</th>
                    <th>Service Level</th>
                </tr>
            </thead>
            <tbody id="worst-zones-table">
            </tbody>
        </table>
    </div>
    
    <div class="data-section">
        <h2>🟢 Top 20 Best-Served Zones</h2>
        <table>
            <thead>
                <tr>
                    <th>Rank</th>
                    <th>Zone</th>
                    <th>Population</th>
                    <th>Companies</th>
                    <th>Mismatch</th>
                    <th>Service Level</th>
                </tr>
            </thead>
            <tbody id="best-zones-table">
            </tbody>
        </table>
    </div>
    
    <div class="data-section">
        <h2>🛤️ Top 20 Critical Corridors (Highest Unmet Demand)</h2>
        <table>
            <thead>
                <tr>
                    <th>Rank</th>
                    <th>Origin</th>
                    <th>Destination</th>
                    <th>Distance (km)</th>
                    <th>Real Trips</th>
                    <th>Estimated Trips</th>
                    <th>Demand Gap</th>
                </tr>
            </thead>
            <tbody id="corridors-table">
            </tbody>
        </table>
    </div>
    
    <script>
        // Data embedded in the HTML
        const zonesGeoJSON = {zones_geojson};
        const corridorsGeoJSON = {corridors_geojson};
        const worstZones = {worst_zones};
        const bestZones = {best_zones};
        const criticalCorridors = {critical_corridors};
        
        // Populate worst zones table
        const worstTable = document.getElementById('worst-zones-table');
        worstZones.slice(0, 20).forEach((z, i) => {{
            worstTable.innerHTML += `
                <tr>
                    <td>${{i + 1}}</td>
                    <td>${{z.zone_name || z.zone_id}}</td>
                    <td>${{(z.poblacion_total || 0).toLocaleString()}}</td>
                    <td>${{(z.empresas || 0).toLocaleString()}}</td>
                    <td>${{(z.mismatch_global || 0).toFixed(3)}}</td>
                    <td>${{(z.total_demand_gap || 0).toLocaleString()}}</td>
                    <td><span class="badge ${{z.service_level}}">${{z.service_level}}</span></td>
                </tr>
            `;
        }});
        
        // Populate best zones table
        const bestTable = document.getElementById('best-zones-table');
        bestZones.slice(0, 20).forEach((z, i) => {{
            bestTable.innerHTML += `
                <tr>
                    <td>${{i + 1}}</td>
                    <td>${{z.zone_name || z.zone_id}}</td>
                    <td>${{(z.poblacion_total || 0).toLocaleString()}}</td>
                    <td>${{(z.empresas || 0).toLocaleString()}}</td>
                    <td>${{(z.mismatch_global || 0).toFixed(3)}}</td>
                    <td><span class="badge ${{z.service_level}}">${{z.service_level}}</span></td>
                </tr>
            `;
        }});
        
        // Populate corridors table
        const corridorsTable = document.getElementById('corridors-table');
        criticalCorridors.slice(0, 20).forEach((c, i) => {{
            corridorsTable.innerHTML += `
                <tr>
                    <td>${{i + 1}}</td>
                    <td>${{c.nombre_i || c.zone_i}}</td>
                    <td>${{c.nombre_j || c.zone_j}}</td>
                    <td>${{(c.distance_km || 0).toFixed(1)}}</td>
                    <td>${{(c.viajes_reales || 0).toLocaleString()}}</td>
                    <td>${{(c.T_estimado || 0).toLocaleString()}}</td>
                    <td>${{(c.demand_gap || 0).toLocaleString()}}</td>
                </tr>
            `;
        }});
        
        // Download functions
        function downloadGeoJSON(type) {{
            const data = type === 'zones' ? zonesGeoJSON : corridorsGeoJSON;
            const blob = new Blob([JSON.stringify(data, null, 2)], {{type: 'application/json'}});
            const url = URL.createObjectURL(blob);
            const a = document.createElement('a');
            a.href = url;
            a.download = `gravity_model_${{type}}_{start_date}_{end_date}.geojson`;
            a.click();
            URL.revokeObjectURL(url);
        }}
        
        function downloadCSV(type) {{
            const data = type === 'worst' ? worstZones : bestZones;
            const headers = Object.keys(data[0] || {{}});
            const csv = [
                headers.join(','),
                ...data.map(row => headers.map(h => JSON.stringify(row[h] ?? '')).join(','))
            ].join('\\n');
            const blob = new Blob([csv], {{type: 'text/csv'}});
            const url = URL.createObjectURL(blob);
            const a = document.createElement('a');
            a.href = url;
            a.download = `gravity_model_${{type}}_zones_{start_date}_{end_date}.csv`;
            a.click();
            URL.revokeObjectURL(url);
        }}
    </script>
</body>
</html>
'''


def _build_zones_geojson(zones_df) -> dict:
    """Build GeoJSON FeatureCollection for zones."""
    features = []
    for _, row in zones_df.iterrows():
        try:
            geometry = json.loads(row['geometry_geojson']) if row['geometry_geojson'] else None
            if geometry:
                feature = {
                    "type": "Feature",
                    "geometry": geometry,
                    "properties": {
                        "zone_id": str(row['zone_id']),
                        "zone_name": row.get('zone_name', ''),
                        "poblacion_total": float(row['poblacion_total']) if row.get('poblacion_total') else 0,
                        "empresas": float(row['empresas']) if row.get('empresas') else 0,
                        "renta_media": float(row['renta_media']) if row.get('renta_media') else 0,
                        "mismatch_global": float(row['mismatch_global']) if row.get('mismatch_global') else None,
                        "service_level": row.get('service_level', 'unknown'),
                        "total_demand_gap": float(row['total_demand_gap']) if row.get('total_demand_gap') else 0,
                        "priority_score": float(row['priority_score']) if row.get('priority_score') else 0
                    }
                }
                features.append(feature)
        except Exception:
            continue
    
    return {
        "type": "FeatureCollection",
        "features": features
    }


def _build_corridors_geojson(corridors_df) -> dict:
    """Build GeoJSON FeatureCollection for corridors (as LineStrings)."""
    features = []
    for _, row in corridors_df.iterrows():
        try:
            if all(row.get(c) is not None for c in ['origin_lon', 'origin_lat', 'destination_lon', 'destination_lat']):
                feature = {
                    "type": "Feature",
                    "geometry": {
                        "type": "LineString",
                        "coordinates": [
                            [float(row['origin_lon']), float(row['origin_lat'])],
                            [float(row['destination_lon']), float(row['destination_lat'])]
                        ]
                    },
                    "properties": {
                        "origin_id": str(row.get('origin_id', '')),
                        "origin_name": row.get('origin_name', ''),
                        "destination_id": str(row.get('destination_id', '')),
                        "destination_name": row.get('destination_name', ''),
                        "distance_km": float(row['distance_km']) if row.get('distance_km') else 0,
                        "viajes_reales": float(row['viajes_reales']) if row.get('viajes_reales') else 0,
                        "T_estimado": float(row['T_estimado']) if row.get('T_estimado') else 0,
                        "demand_gap": float(row['demand_gap']) if row.get('demand_gap') else 0,
                        "mismatch_ratio": float(row['mismatch_ratio']) if row.get('mismatch_ratio') else None,
                        "service_level": row.get('service_level', 'unknown')
                    }
                }
                features.append(feature)
        except Exception:
            continue
    
    return {
        "type": "FeatureCollection", 
        "features": features
    }


@task
def GOLD_gravity_export_results(**context) -> Dict[str, Any]:
    """
    Exports gravity model results to tables and Parquet files.
    Creates the base export tables needed for Kepler.gl visualization.
    """
    print("[TASK] Exporting gravity model results")
    
    start_date = context['params'].get('start_date', '2023-01-01')
    end_date = context['params'].get('end_date', '2023-12-31')
    
    print(f"[TASK] Date range: {start_date} to {end_date}")
    
    sql_query = f"""
    SET preserve_insertion_order=false;
    SET enable_object_cache=true;
    
    -- Export zones with geometry for mapping
    CREATE OR REPLACE TABLE gold_gravity_export_zones AS
    SELECT 
        zr.zone_id,
        zr.zone_name,
        zr.poblacion_total,
        zr.empresas,
        zr.renta_media,
        zr.total_connections,
        zr.total_viajes,
        zr.total_T_estimado,
        zr.total_demand_gap,
        zr.mismatch_global,
        zr.mismatch_ponderado_salida,
        zr.mismatch_ponderado_entrada,
        zr.service_level,
        zr.priority_score,
        sz.geometry_obj,
        ST_AsGeoJSON(sz.geometry_obj) AS geometry_geojson,
        ST_X(sz.centroid) AS centroid_lon,
        ST_Y(sz.centroid) AS centroid_lat,
        '{start_date}' AS analysis_start,
        '{end_date}' AS analysis_end
    FROM gold_gravity_zone_ranking zr
    LEFT JOIN silver_zones sz ON zr.zone_id = sz.id;
    
    -- Export corridors for flow visualization
    CREATE OR REPLACE TABLE gold_gravity_export_corridors AS
    SELECT 
        gc.zone_i AS origin_id,
        gc.nombre_i AS origin_name,
        gc.zone_j AS destination_id,
        gc.nombre_j AS destination_name,
        gc.distance_km,
        gc.viajes_reales,
        gc.T_estimado,
        gc.demand_gap,
        gc.mismatch_ratio,
        gc.service_level,
        ST_X(so.centroid) AS origin_lon,
        ST_Y(so.centroid) AS origin_lat,
        ST_X(sd.centroid) AS destination_lon,
        ST_Y(sd.centroid) AS destination_lat,
        LOG(GREATEST(gc.viajes_reales, 1)) AS flow_weight_real,
        LOG(GREATEST(gc.T_estimado, 1)) AS flow_weight_estimated,
        '{start_date}' AS analysis_start,
        '{end_date}' AS analysis_end
    FROM gold_gravity_mismatch gc
    LEFT JOIN silver_zones so ON gc.zone_i = so.id
    LEFT JOIN silver_zones sd ON gc.zone_j = sd.id
    WHERE gc.mismatch_ratio IS NOT NULL
      AND gc.viajes_reales > 0;
    
    -- Summary statistics table
    CREATE OR REPLACE TABLE gold_gravity_export_summary AS
    SELECT 
        '{start_date}' AS analysis_start,
        '{end_date}' AS analysis_end,
        (SELECT k FROM gold_gravity_calibration_stats LIMIT 1) AS k_constant,
        (SELECT COUNT(*) FROM gold_gravity_zone_ranking) AS total_zones,
        (SELECT COUNT(*) FROM gold_gravity_mismatch WHERE viajes_reales > 0) AS od_pairs_with_data,
        (SELECT SUM(viajes_reales) FROM gold_gravity_mismatch) AS total_real_trips,
        (SELECT SUM(T_estimado) FROM gold_gravity_mismatch) AS total_estimated_trips,
        (SELECT SUM(demand_gap) FROM gold_gravity_mismatch WHERE demand_gap > 0) AS total_unmet_demand,
        (SELECT COUNT(*) FROM gold_gravity_zone_ranking WHERE service_level = 'critical') AS zones_critical,
        (SELECT COUNT(*) FROM gold_gravity_zone_ranking WHERE service_level = 'underserved') AS zones_underserved,
        (SELECT COUNT(*) FROM gold_gravity_zone_ranking WHERE service_level = 'balanced') AS zones_balanced,
        (SELECT COUNT(*) FROM gold_gravity_zone_ranking WHERE service_level = 'well_served') AS zones_well_served,
        (SELECT COUNT(*) FROM gold_gravity_zone_ranking WHERE service_level = 'over_served') AS zones_over_served,
        CURRENT_TIMESTAMP AS generated_at;
    
    -- Export to Parquet files
    COPY gold_gravity_export_zones TO 's3://mitma/gold/gravity_model/zones_{start_date}_{end_date}.parquet' 
        (FORMAT PARQUET, COMPRESSION 'zstd');
    
    COPY gold_gravity_export_corridors TO 's3://mitma/gold/gravity_model/corridors_{start_date}_{end_date}.parquet'
        (FORMAT PARQUET, COMPRESSION 'zstd');
    
    COPY gold_gravity_worst_zones TO 's3://mitma/gold/gravity_model/worst_zones_{start_date}_{end_date}.parquet'
        (FORMAT PARQUET, COMPRESSION 'zstd');
    
    COPY gold_gravity_best_zones TO 's3://mitma/gold/gravity_model/best_zones_{start_date}_{end_date}.parquet'
        (FORMAT PARQUET, COMPRESSION 'zstd');
    
    COPY gold_gravity_critical_corridors TO 's3://mitma/gold/gravity_model/critical_corridors_{start_date}_{end_date}.parquet'
        (FORMAT PARQUET, COMPRESSION 'zstd');
    """
    
    result = execute_sql_or_cloud_run(sql_query=sql_query, **context)
    
    print(f"[TASK] Export tables and Parquet files created successfully")
    print(f"[TASK] Execution time: {result.get('execution_time_seconds', 0)} seconds")
    
    return {
        "status": "success",
        "tables": ["gold_gravity_export_zones", "gold_gravity_export_corridors", "gold_gravity_export_summary"],
        "execution_name": result.get("execution_name"),
        "execution_time_seconds": result.get("execution_time_seconds")
    }


@task
def GOLD_gravity_export_keplergl(**context) -> Dict[str, Any]:
    """
    Generates a standalone Kepler.gl HTML file with embedded data and visualization.
    The HTML file can be opened directly in any browser without needing a server.
    
    Exports:
    - Interactive dashboard with summary statistics
    - Downloadable GeoJSON files for Kepler.gl
    - Tables with worst/best zones and critical corridors
    """
    print("[TASK] Generating Kepler.gl HTML export")
    
    start_date = context['params'].get('start_date', '2023-01-01')
    end_date = context['params'].get('end_date', '2023-12-31')
    
    try:
        con = get_ducklake_connection()
        
        # Fetch all required data
        print("[TASK] Fetching zones data...")
        zones_df = con.execute("""
            SELECT * FROM gold_gravity_export_zones
            WHERE geometry_geojson IS NOT NULL
        """).fetchdf()
        
        print("[TASK] Fetching corridors data...")
        corridors_df = con.execute("""
            SELECT * FROM gold_gravity_export_corridors
            LIMIT 5000
        """).fetchdf()
        
        print("[TASK] Fetching worst zones...")
        worst_zones_df = con.execute("SELECT * FROM gold_gravity_worst_zones").fetchdf()
        
        print("[TASK] Fetching best zones...")
        best_zones_df = con.execute("SELECT * FROM gold_gravity_best_zones").fetchdf()
        
        print("[TASK] Fetching critical corridors...")
        critical_corridors_df = con.execute("SELECT * FROM gold_gravity_critical_corridors LIMIT 100").fetchdf()
        
        print("[TASK] Fetching summary...")
        summary_df = con.execute("SELECT * FROM gold_gravity_export_summary").fetchdf()
        
        # Build GeoJSON
        zones_geojson = _build_zones_geojson(zones_df)
        corridors_geojson = _build_corridors_geojson(corridors_df)
        
        # Convert DataFrames to list of dicts
        worst_zones = worst_zones_df.to_dict('records') if not worst_zones_df.empty else []
        best_zones = best_zones_df.to_dict('records') if not best_zones_df.empty else []
        critical_corridors = critical_corridors_df.to_dict('records') if not critical_corridors_df.empty else []
        
        # Get summary stats
        summary = summary_df.iloc[0].to_dict() if not summary_df.empty else {}
        
        # Format k constant for display
        k_constant = summary.get('k_constant', 0)
        k_display = f"{k_constant:.2e}" if k_constant else "N/A"
        
        # Generate HTML
        html_content = KEPLER_SIMPLE_HTML_TEMPLATE.format(
            title=f"Gravity Model Analysis ({start_date} to {end_date})",
            start_date=start_date,
            end_date=end_date,
            total_zones=summary.get('total_zones', 0),
            zones_critical=summary.get('zones_critical', 0),
            zones_underserved=summary.get('zones_underserved', 0),
            zones_balanced=summary.get('zones_balanced', 0),
            zones_well_served=summary.get('zones_well_served', 0),
            k_constant=k_display,
            zones_geojson=json.dumps(zones_geojson),
            corridors_geojson=json.dumps(corridors_geojson),
            worst_zones=json.dumps(worst_zones),
            best_zones=json.dumps(best_zones),
            critical_corridors=json.dumps(critical_corridors)
        )
        
        # Save HTML to S3
        # Escape single quotes for SQL
        html_escaped = html_content.replace("'", "''")
        
        con.execute(f"""
            COPY (SELECT '{html_escaped}' AS html)
            TO 's3://mitma/gold/gravity_model/keplergl_dashboard_{start_date}_{end_date}.html'
            (FORMAT CSV, HEADER FALSE, QUOTE '', DELIMITER '|');
        """)
        
        # Also save pure GeoJSON files
        zones_geojson_str = json.dumps(zones_geojson).replace("'", "''")
        corridors_geojson_str = json.dumps(corridors_geojson).replace("'", "''")
        
        con.execute(f"""
            COPY (SELECT '{zones_geojson_str}' AS geojson)
            TO 's3://mitma/gold/gravity_model/zones_{start_date}_{end_date}.geojson'
            (FORMAT CSV, HEADER FALSE, QUOTE '', DELIMITER '|');
        """)
        
        con.execute(f"""
            COPY (SELECT '{corridors_geojson_str}' AS geojson)
            TO 's3://mitma/gold/gravity_model/corridors_{start_date}_{end_date}.geojson'
            (FORMAT CSV, HEADER FALSE, QUOTE '', DELIMITER '|');
        """)
        
        print(f"[TASK] Kepler.gl HTML dashboard generated successfully")
        print(f"[TASK]   Zones: {len(zones_geojson['features'])}")
        print(f"[TASK]   Corridors: {len(corridors_geojson['features'])}")
        print(f"[TASK]   Critical zones: {summary.get('zones_critical', 0)}")
        print(f"[TASK]   Underserved zones: {summary.get('zones_underserved', 0)}")
        
        return {
            "status": "success",
            "files": {
                "dashboard": f"s3://mitma/gold/gravity_model/keplergl_dashboard_{start_date}_{end_date}.html",
                "zones_geojson": f"s3://mitma/gold/gravity_model/zones_{start_date}_{end_date}.geojson",
                "corridors_geojson": f"s3://mitma/gold/gravity_model/corridors_{start_date}_{end_date}.geojson"
            },
            "stats": {
                "zones_count": len(zones_geojson['features']),
                "corridors_count": len(corridors_geojson['features']),
                "worst_zones_count": len(worst_zones),
                "best_zones_count": len(best_zones)
            }
        }
        
    except Exception as e:
        print(f"[TASK] Error generating Kepler.gl export: {e}")
        import traceback
        traceback.print_exc()
        return {
            "status": "error",
            "error": str(e)
        }
