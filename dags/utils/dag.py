"""
Common DAG utilities and helper functions.
"""

from datetime import datetime
from airflow.sdk import task  # type: ignore


@task
def validate_dates(start_date: str = None, end_date: str = None, **context):
    """
    Validate that end_date is greater than start_date.
    
    Parameters:
    - start_date: Start date (YYYY-MM-DD)
    - end_date: End date (YYYY-MM-DD)
    
    Raises:
    - AirflowException: If dates are invalid or end_date <= start_date
    """
    from airflow.exceptions import AirflowException
    
    if not start_date or not end_date:
        raise AirflowException("Both 'start' and 'end' parameters are required")
    
    try:
        start_dt = datetime.strptime(start_date, '%Y-%m-%d')
        end_dt = datetime.strptime(end_date, '%Y-%m-%d')
    except ValueError as e:
        raise AirflowException(f"Invalid date format. Expected YYYY-MM-DD. Error: {str(e)}")
    
    if end_dt <= start_dt:
        raise AirflowException(f"End date '{end_date}' must be greater than start date '{start_date}'")
    
    print(f"[VALIDATION] Date range validated: {start_date} to {end_date}")
    return True
