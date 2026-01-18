from airflow.models import Param

def get_params():
    return {
        "start_date": Param(
            type=["string", "null"],
            default=None,
            description="Start date (YYYY-MM-DD) - Required",
            schema={"type": ["string", "null"], "format": "date"}
        ),
        "end_date": Param(
            type=["string", "null"],
            default=None,
            description="End date (YYYY-MM-DD) - Required",
            schema={"type": ["string", "null"], "format": "date"}
        ),
        "polygon_wkt": Param(
            type=["string", "null"],
            default=None,
            description="Polygon WKT - Required"
        )
    }