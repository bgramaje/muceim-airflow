"""
Airflow task for loading Spanish nationwide holidays into a temporary DuckDB table.
Fetches ES public holidays from OpenHolidays API (via HTTPFS) and loads them into
a session-scoped TEMP table called bronze_spanish_holidays.
"""

from airflow.sdk import task  # type: ignore


@task
def BRONZE_load_spanish_holidays(year: int = 2023):
    """
    Loads Spanish nationwide public holidays into a TEMP DuckDB table.
    The table is session-scoped and therefore idempotent per run.

    Returns:
    - Dict with task status and info
    """
    from utils import get_ducklake_connection

    print("[TASK] Loading Spanish public holidays into DuckDB table")

    con = get_ducklake_connection()

    # Create TEMP table with holidays
    con.execute(f"""
        CREATE OR REPLACE TABLE bronze_spanish_holidays AS
        SELECT 
            startDate AS date,
            name[1].text AS name
        FROM read_json(
            'https://openholidaysapi.org/PublicHolidays?countryIsoCode=ES&languageIsoCode=ES&validFrom={year}-01-01&validTo={year}-12-31',
            format='array'
        )
        WHERE nationwide = true;
    """
    )
    df = con.execute("SELECT COUNT(*) AS count FROM bronze_spanish_holidays").fetchdf()

    print(f"[TASK] Loaded bronze_spanish_holidays table with {df.iloc[0]['count']:,} records")
    print(con.execute("SELECT * FROM bronze_spanish_holidays LIMIT 10").fetchdf())

    return {
        "status": "success",
        "records": int(df.iloc[0]["count"]),
        "table": "bronze_spanish_holidays"
    }
