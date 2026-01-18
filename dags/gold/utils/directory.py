"""
Directory utility functions for Gold layer reports.
"""

from datetime import datetime
import uuid


def build_dir_name(execution_date=None):
    """
    Build a unique directory name for reports.
    
    Format: DDMMYYYY_6digitsUUID
    
    Parameters:
    - execution_date: Optional execution date (datetime object or string in YYYY-MM-DD format)
                      If None, uses current date
    
    Returns:
    - str: Directory name in format DDMMYYYY_6digitsUUID
    """
    if execution_date:
        if isinstance(execution_date, str):
            exec_dt = datetime.strptime(execution_date, '%Y-%m-%d')
        else:
            exec_dt = execution_date
        date_str = exec_dt.strftime('%d%m%Y')
    else:
        date_str = datetime.now().strftime('%d%m%Y')
    
    report_uuid = str(uuid.uuid4()).replace('-', '')
    uuid_suffix = report_uuid[-6:]
    
    directory_name = f"{date_str}_{uuid_suffix}"
    return directory_name
