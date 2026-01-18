"""Google Cloud Platform utilities for Airflow DAGs."""

from typing import Dict
import time
from airflow.sdk import Variable  # type: ignore
from google.cloud import run_v2  # type: ignore
from utils.logger import get_logger


def _get_cloud_run_logs(execution_name: str) -> None:
    """Retrieves and logs the last logs from a Cloud Run execution."""
    logger = get_logger(__name__)
    try:
        from google.cloud import logging as cloud_logging
        
        execution_id = execution_name.split('/')[-1]
        logging_client = cloud_logging.Client()
        
        log_filter = f'resource.type="cloud_run_job" AND (textPayload=~".*{execution_id}.*" OR jsonPayload.execution_name="{execution_id}" OR labels.execution_name="{execution_id}")'
        
        entries = logging_client.list_entries(
            filter_=log_filter,
            max_results=20,
            order_by=cloud_logging.DESCENDING
        )
        
        cloud_run_logs = []
        for entry in entries:
            log_text = None
            if hasattr(entry, 'text_payload') and entry.text_payload:
                log_text = entry.text_payload
            elif hasattr(entry, 'payload'):
                log_text = str(entry.payload) if isinstance(entry.payload, str) else str(entry.payload)
            
            if log_text:
                cloud_run_logs.append(log_text[:1000])
        
        if cloud_run_logs:
            logger.info("Last logs from Cloud Run execution:")
            for log in cloud_run_logs[:10]:
                logger.info(f"  {log}")
    except Exception as log_error:
        logger.warning(f"Could not retrieve Cloud Run logs: {str(log_error)}")


def _is_execution_succeeded(state) -> bool:
    """Checks if Cloud Run execution state indicates success."""
    state_str = str(state).upper()
    return 'SUCCEEDED' in state_str or (hasattr(state, '__int__') and int(state) == 2)


def _is_execution_failed(state) -> bool:
    """Checks if Cloud Run execution state indicates failure."""
    state_str = str(state).upper()
    return 'FAILED' in state_str or (hasattr(state, '__int__') and int(state) == 3)


def _wait_for_cloud_run_execution(
    executions_client,
    execution_name: str,
    max_wait_time: int = 3600,
    poll_interval: int = 10
) -> None:
    """Waits for Cloud Run execution to complete and verifies its status. Raises RuntimeError on failure or timeout."""
    logger = get_logger(__name__)
    start_time = time.time()
    last_log_time = start_time
    log_interval = 60  # Log status every 60 seconds
    
    logger.info(f"Waiting for execution to complete: {execution_name}")
    
    while True:
        elapsed = time.time() - start_time
        if elapsed > max_wait_time:
            raise RuntimeError(f"Job execution timed out after {max_wait_time} seconds")
        
        try:
            execution = executions_client.get_execution(name=execution_name)
            
            # Check completion_time first - if set, execution is done
            completion_time = getattr(execution, 'completion_time', None)
            if completion_time:
                # Execution has completed, check final status
                logger.info(f"Execution completed at {completion_time}")
                
                # Check task counts
                succeeded_count = getattr(execution, 'succeeded_count', 0)
                failed_count = getattr(execution, 'failed_count', 0)
                running_count = getattr(execution, 'running_count', 0)
                
                logger.info(f"Task counts - Succeeded: {succeeded_count}, Failed: {failed_count}, Running: {running_count}")
                
                # Find Ready condition to get final status
                ready_condition = None
                conditions = getattr(execution, 'conditions', []) or []
                for cond in conditions:
                    cond_type = getattr(cond, 'type', None)
                    if cond_type == 'Ready' or (isinstance(cond_type, str) and 'Ready' in cond_type):
                        ready_condition = cond
                        break
                
                if ready_condition:
                    state = getattr(ready_condition, 'state', None)
                    state_str = str(state).upper() if state else "UNKNOWN"
                    
                    if _is_execution_succeeded(state):
                        logger.info("Execution succeeded")
                        return
                    
                    if _is_execution_failed(state):
                        error_msg = getattr(ready_condition, 'message', None) or f"Failed: {getattr(ready_condition, 'reason', 'Unknown error')}"
                        
                        error_details = []
                        if hasattr(execution, 'containers') and execution.containers:
                            for container in execution.containers:
                                exit_code = getattr(container, 'exit_code', None)
                                if exit_code and exit_code != 0:
                                    error_details.append(f"Container exit code: {exit_code}")
                        
                        if hasattr(execution, 'log_uri') and execution.log_uri:
                            error_details.append(f"Logs available at: {execution.log_uri}")
                        
                        _get_cloud_run_logs(execution_name)
                        if error_details:
                            error_msg += f" | {' | '.join(error_details)}"
                        
                        raise RuntimeError(f"{error_msg}. Execution: {execution_name}")
                    
                    # If we have completion_time but state is not succeeded/failed, check counts
                    if failed_count > 0:
                        raise RuntimeError(f"Execution failed: {failed_count} task(s) failed. Execution: {execution_name}")
                    
                    if succeeded_count > 0 and running_count == 0:
                        logger.info(f"Execution completed successfully ({succeeded_count} task(s) succeeded)")
                        return
                
                # If completion_time is set but we can't determine status, assume success if no failures
                if failed_count == 0:
                    logger.info("Execution completed (no failures detected)")
                    return
                else:
                    raise RuntimeError(f"Execution completed with failures: {failed_count} task(s) failed. Execution: {execution_name}")
            
            # Execution still running - check conditions for progress
            conditions = getattr(execution, 'conditions', []) or []
            ready_condition = None
            for cond in conditions:
                cond_type = getattr(cond, 'type', None)
                if cond_type == 'Ready' or (isinstance(cond_type, str) and 'Ready' in cond_type):
                    ready_condition = cond
                    break
            
            # Log status periodically
            if time.time() - last_log_time >= log_interval:
                running_count = getattr(execution, 'running_count', 0)
                succeeded_count = getattr(execution, 'succeeded_count', 0)
                failed_count = getattr(execution, 'failed_count', 0)
                logger.info(f"Still waiting... Elapsed: {int(elapsed)}s | Running: {running_count}, Succeeded: {succeeded_count}, Failed: {failed_count}")
                last_log_time = time.time()
            
            # Check if execution failed while still running
            if ready_condition:
                state = getattr(ready_condition, 'state', None)
                if _is_execution_failed(state):
                    error_msg = getattr(ready_condition, 'message', None) or f"Failed: {getattr(ready_condition, 'reason', 'Unknown error')}"
                    _get_cloud_run_logs(execution_name)
                    raise RuntimeError(f"{error_msg}. Execution: {execution_name}")
            
            time.sleep(poll_interval)
                
        except RuntimeError:
            raise
        except Exception as e:
            # Log the error but continue polling (might be transient)
            if time.time() - last_log_time >= log_interval:
                logger.warning(f"Error checking execution status (will retry): {str(e)}")
                last_log_time = time.time()
            time.sleep(poll_interval)

def _get_cloud_run_connection():
    """Returns Cloud Run connection tuple (credentials, region, project_id) if available, None otherwise."""
    try:
        from airflow.providers.google.cloud.hooks.cloud_run import CloudRunHook  # type: ignore
        
        region = Variable.get('GCP_CLOUD_RUN_REGION', default=None)
        project_id = Variable.get('GCP_PROJECT_ID', default=None)
        gcp_conn_id = Variable.get('GCP_CONNECTION_ID', default='google_cloud_default')
        
        if not region or not project_id:
            return None
        try:
            hook = CloudRunHook(gcp_conn_id=gcp_conn_id)
            credentials = hook.get_credentials()
            return (credentials, region, project_id)
        except Exception:
            return None
    except (ImportError, Exception):
        return None


def _load_cloud_env() -> Dict[str, str]:
    """
    Loads environment variables needed for Cloud Run execution from Airflow connections.
    These variables will be passed to the Cloud Run Job container.
    
    Returns:
        Dict with environment variable names as keys and values as values.
        Keys: S3_ENDPOINT, RUSTFS_USER, RUSTFS_PASSWORD, RUSTFS_SSL, RUSTFS_BUCKET
    """
    from airflow.sdk import Connection
    
    try:
        s3_conn = Connection.get('rustfs_s3_conn')
        s3_extra = s3_conn.extra_dejson
        endpoint_url = s3_extra.get('endpoint_url', 'http://rustfs:9000')
        s3_endpoint = endpoint_url.replace('http://', '').replace('https://', '')
        rustfs_user = s3_extra.get('aws_access_key_id', 'admin')
        rustfs_password = s3_extra.get('aws_secret_access_key', 'muceim-duckduck.2025!')
        rustfs_ssl = 'true' if 'https' in endpoint_url else 'false'
        rustfs_bucket = Variable.get('RUSTFS_BUCKET', default='mitma')
    except Exception as e:
        raise RuntimeError(
            f"Failed to load Cloud Run environment variables from Airflow connection 'rustfs_s3_conn': {str(e)}\n"
            "Make sure the connection is properly configured."
        ) from e
    
    return {
        'S3_ENDPOINT': s3_endpoint,
        'RUSTFS_USER': rustfs_user,
        'RUSTFS_PASSWORD': rustfs_password,
        'RUSTFS_SSL': rustfs_ssl,
        'RUSTFS_BUCKET': rustfs_bucket,
    }


def _serialize_post_process_func(post_process_func) -> Dict[str, str] | None:
    """
    Serializes a post-processing function for Cloud Run execution.
    Includes helper functions from utils.s3 if imported.
    
    Args:
        post_process_func: The function to serialize
        
    Returns:
        Dict with 'POST_PROCESS_CODE' and 'POST_PROCESS_FUNC_NAME' keys, or None if serialization fails
    """
    import inspect
    import base64
    import zlib
    import textwrap
    import os
    
    try:
        func_code = inspect.getsource(post_process_func)
        func_code = textwrap.dedent(func_code)
        
        func_globals = post_process_func.__globals__ if hasattr(post_process_func, '__globals__') else {}
        
        constants = []
        for name, value in func_globals.items():
            if name.endswith('_SQL') and isinstance(value, str):
                constants.append(f"{name} = {repr(value)}")
        
        # Check if upload_to_s3_rustfs is imported from utils.s3
        # If so, include its code in the serialization
        helper_code = ""
        if 'upload_to_s3_rustfs' in func_code and 'from utils.s3 import' in func_code:
            try:
                from utils.s3 import upload_to_s3_rustfs
                helper_code = inspect.getsource(upload_to_s3_rustfs)
                helper_code = textwrap.dedent(helper_code)
                # Replace the import with the actual function definition
                func_code = func_code.replace(
                    'from utils.s3 import upload_to_s3_rustfs',
                    '# upload_to_s3_rustfs function included below'
                )
                helper_code = helper_code + '\n\n'
            except Exception as e:
                logger = get_logger(__name__)
                logger.warning(f"Could not include upload_to_s3_rustfs helper: {str(e)}")
        
        if constants:
            full_code = '\n'.join(constants) + '\n\n' + helper_code + func_code
        else:
            full_code = helper_code + func_code
        
        logger = get_logger(__name__)
        logger.info("Serialized post_process_func (imports are inside the function)")
        
        compressed = zlib.compress(full_code.encode('utf-8'))
        encoded = base64.b64encode(compressed).decode('utf-8')
        
        return {
            'POST_PROCESS_CODE': encoded,
            'POST_PROCESS_FUNC_NAME': post_process_func.__name__
        }
    except Exception as e:
        logger = get_logger(__name__)
        logger.warning(f"Could not serialize post_process_func: {str(e)}")
        logger.warning("Post-processing will be skipped in Cloud Run")
        return None


def exec_gcp_ducklake_executor(
    sql_query: str,
    post_process_func=None,
    **context
) -> Dict:
    """Executes Cloud Run Job to run SQL query in DuckDB. Optionally runs post_process_func after execution. Returns dict with results."""    
    if not sql_query or not sql_query.strip():
        raise ValueError("SQL query cannot be empty")
    
    conn_info = _get_cloud_run_connection()
    if not conn_info:
        raise ValueError("Cloud Run connection not available. Check Airflow Variables: GCP_CLOUD_RUN_REGION, GCP_PROJECT_ID")
    
    credentials, region, project_id = conn_info
    job_name = Variable.get('GCP_CLOUD_RUN_EXECUTOR_JOB_NAME', default='ducklake-executor')
    
    # Load environment variables for Cloud Run container
    cloud_env = _load_cloud_env()
    
    env_vars_list = [
        {'name': 'SQL_QUERY', 'value': sql_query},
        {'name': 'S3_ENDPOINT', 'value': cloud_env['S3_ENDPOINT']},
        {'name': 'RUSTFS_USER', 'value': cloud_env['RUSTFS_USER']},
        {'name': 'RUSTFS_PASSWORD', 'value': cloud_env['RUSTFS_PASSWORD']},
        {'name': 'RUSTFS_SSL', 'value': cloud_env['RUSTFS_SSL']},
        {'name': 'RUSTFS_BUCKET', 'value': cloud_env['RUSTFS_BUCKET']},
    ]
    
    extra_env_vars = context.get('extra_env_vars', {})
    for key, value in extra_env_vars.items():
        env_vars_list.append({'name': key, 'value': str(value)})
    
    if post_process_func:
        serialized_func = _serialize_post_process_func(post_process_func)
        if not serialized_func:
            raise RuntimeError(
                "Failed to serialize post_process_func for Cloud Run execution. "
                "The function cannot be sent to Cloud Run. Check the function definition and ensure it can be serialized."
            )
        env_vars_list.append({'name': 'POST_PROCESS_CODE', 'value': serialized_func['POST_PROCESS_CODE']})
        env_vars_list.append({'name': 'POST_PROCESS_FUNC_NAME', 'value': serialized_func['POST_PROCESS_FUNC_NAME']})
    
    try:
        client = run_v2.JobsClient(credentials=credentials)
        
        job_path = f"projects/{project_id}/locations/{region}/jobs/{job_name}"
        
        request = run_v2.RunJobRequest(
            name=job_path,
            overrides=run_v2.RunJobRequest.Overrides(
                container_overrides=[
                    run_v2.RunJobRequest.Overrides.ContainerOverride(
                        env=[
                            run_v2.EnvVar(name=env['name'], value=env['value'])
                            for env in env_vars_list
                        ]
                    )
                ]
            )
        )
        
        operation = client.run_job(request=request)
        execution = operation.result()
        execution_name = execution.name
        
        executions_client = run_v2.ExecutionsClient(credentials=credentials)
        
        start_time = time.time()
        _wait_for_cloud_run_execution(executions_client, execution_name)
        elapsed_time = time.time() - start_time
        
        # Mostrar logs de Cloud Run después de ejecución exitosa
        _get_cloud_run_logs(execution_name)
        
        result = {
            'status': 'success',
            'message': 'SQL query executed successfully',
            'execution_name': execution_name,
            'execution_time_seconds': int(elapsed_time)
        }
        
        return result
        
    except Exception as e:
        error_msg = f"Failed to execute Cloud Run Job: {str(e)}"
        raise RuntimeError(error_msg) from e


def execute_sql_or_cloud_run(sql_query: str, post_process_func=None, **context) -> Dict:
    """Executes SQL query using Cloud Run if available, or locally using Airflow connections. Returns dict with results."""
    if _get_cloud_run_connection():
        return exec_gcp_ducklake_executor(sql_query=sql_query, post_process_func=post_process_func, **context)
    else:
        import time
        from utils.utils import get_ducklake_connection
        
        start_time = time.time()
        
        try:
            con = get_ducklake_connection()
            
            result_obj = con.execute(sql_query)

            df_result = None
            try:
                df_result = result_obj.fetchdf()
            except Exception:
                df_result = None
            
            elapsed_time = time.time() - start_time
            
            result = {
                'status': 'success',
                'message': 'SQL query executed successfully (local)',
                'execution_name': 'local',
                'execution_time_seconds': int(elapsed_time)
            }
            
            if post_process_func:
                try:
                    post_result = post_process_func(df_result, con, result)
                    if post_result and isinstance(post_result, dict):
                        result.update(post_result)
                except Exception as e:
                    logger = get_logger(__name__)
                    logger.error(f"post-processing failed: {str(e)}", exc_info=True)
            
            return result
            
        except Exception as e:
            error_msg = (
                f"Failed to execute SQL locally: {str(e)}\n"
                "Make sure Airflow connections are configured:\n"
                "  - 'rustfs_s3_conn' (S3/RustFS connection)\n"
                "  - 'postgres_datos_externos' (PostgreSQL connection)\n"
                "  - Variable 'RUSTFS_BUCKET' (optional, defaults to 'mitma')"
            )
            raise RuntimeError(error_msg) from e


