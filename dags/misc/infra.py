"""
TaskGroup for infrastructure setup tasks.
Orchestrates connection verification and S3 bucket setup.
"""

from types import SimpleNamespace

from airflow.sdk import task_group
from airflow.providers.standard.operators.empty import EmptyOperator

from misc.tasks.verify_connections import PRE_verify_connections
from misc.tasks.ensure_s3_bucket import PRE_s3_bucket


@task_group(group_id="infra")
def tg_infra():
    """
    TaskGroup for infrastructure setup.
    Verifies connections and ensures S3 bucket exists.
    
    Returns:
    - SimpleNamespace with verify and bucket tasks
    """
    from airflow.sdk import Variable
    
    verify = PRE_verify_connections()
    bucket = PRE_s3_bucket(bucket_name=Variable.get('RUSTFS_BUCKET', default='mitma'))
    
    # Verify connections first, then ensure bucket exists
    verify >> bucket
    
    return SimpleNamespace(
        verify=verify,
        bucket=bucket,
    )

