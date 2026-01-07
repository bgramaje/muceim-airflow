"""
Google Cloud Platform utilities for Airflow DAGs.

This module provides functions to interact with Google Cloud services
using Airflow's native hooks and operators.

## Configuración Requerida

### 1. Instalar el provider de Google Cloud
```bash
pip install apache-airflow-providers-google
```

### 2. Configurar Variables de Airflow

Necesitas configurar las siguientes Variables en Airflow:

- **GCP_CLOUD_RUN_JOB_NAME**: Nombre de tu Cloud Run Job
  - Ejemplo: `insert-ducklake`
  
- **GCP_CLOUD_RUN_REGION**: Región donde está desplegado el Job
  - Ejemplo: `europe-southwest1` (Madrid)
  
- **GCP_PROJECT_ID**: ID de tu proyecto de GCP
  - Ejemplo: `muceim-bigdata`
  
- **GCP_CONNECTION_ID** (opcional): ID de la conexión de Google Cloud
  - Por defecto: `google_cloud_default`

### 3. Configurar Conexión de Google Cloud en Airflow

Ve a Admin > Connections y crea/edita la conexión:

- **Connection Id**: `google_cloud_default` (o el que especifiques en GCP_CONNECTION_ID)
- **Connection Type**: `Google Cloud Platform`
- **Project Id**: Tu proyecto de GCP (ej: `muceim-bigdata`)
- **Keyfile JSON**: Contenido completo del archivo JSON de la Service Account
  - O alternativamente, usa **Keyfile Path**: ruta al archivo JSON en el servidor

### 4. Permisos de la Service Account

La Service Account necesita los siguientes permisos:
- `roles/run.invoker` (Cloud Run Invoker) - para ejecutar el Job
- `roles/dataScientist` (o permisos para acceder a DuckDB/PostgreSQL/RustFS)

**Asignar permisos:**
```bash
gcloud projects add-iam-policy-binding PROJECT_ID \
  --member="serviceAccount:TU_SERVICE_ACCOUNT@PROJECT.iam.gserviceaccount.com" \
  --role="roles/run.invoker"
```
"""

from typing import Dict
import time
from airflow.models import Variable  # type: ignore
from google.cloud import run_v2  # type: ignore


def _wait_for_cloud_run_execution(
    executions_client,
    execution_name: str,
    max_wait_time: int = 3600,
    poll_interval: int = 10
) -> None:
    """
    Espera a que un Cloud Run execution termine y verifica su estado.
    
    Parameters:
    - executions_client: Cliente de Executions de Cloud Run
    - execution_name: Nombre del execution a monitorear
    - max_wait_time: Tiempo máximo de espera en segundos (default: 3600)
    - poll_interval: Intervalo entre verificaciones en segundos (default: 10)
    
    Raises:
    - RuntimeError: Si el execution falla o se excede el tiempo máximo
    """
    start_time = time.time()
    
    while True:
        elapsed_time = time.time() - start_time
        if elapsed_time > max_wait_time:
            raise RuntimeError(f"Job execution timed out after {max_wait_time} seconds")
        
        try:
            execution = executions_client.get_execution(name=execution_name)
            
            conditions = getattr(execution, 'conditions', None)
            if not conditions or len(conditions) == 0:
                time.sleep(poll_interval)
                continue
            
            latest_condition = conditions[-1]
            state = latest_condition.state
            state_name = state.name if hasattr(state, 'name') else str(state)
            
            try:
                state_value = int(state) if hasattr(state, '__int__') else None
            except:
                state_value = None
            
            if state_name == 'SUCCEEDED' or 'SUCCEEDED' in str(state_name) or state_value == 2:
                return
            elif state_name == 'FAILED' or 'FAILED' in str(state_name) or state_value == 3:
                error_msg = "Job execution failed"
                
                # Intentar obtener mensaje de error de la condición
                if hasattr(latest_condition, 'message') and latest_condition.message:
                    error_msg = latest_condition.message
                elif hasattr(latest_condition, 'reason') and latest_condition.reason:
                    error_msg = f"Failed: {latest_condition.reason}"
                
                # Intentar obtener información adicional del execution
                error_details = []
                
                # Obtener información de los contenedores si está disponible
                if hasattr(execution, 'containers') and execution.containers:
                    for container in execution.containers:
                        if hasattr(container, 'exit_code') and container.exit_code and container.exit_code != 0:
                            error_details.append(f"Container exit code: {container.exit_code}")
                
                # Obtener log_uri si está disponible (para referencia a los logs)
                if hasattr(execution, 'log_uri') and execution.log_uri:
                    error_details.append(f"Logs available at: {execution.log_uri}")
                
                # Intentar obtener logs recientes usando Cloud Logging si está disponible
                try:
                    from google.cloud import logging as cloud_logging
                    logging_client = cloud_logging.Client()
                    
                    # Extraer información del execution_name (formato: projects/.../locations/.../jobs/.../executions/...)
                    # Para Cloud Run Jobs, los logs tienen resource.type="cloud_run_job"
                    execution_id = execution_name.split('/')[-1]
                    job_name = execution_name.split('/jobs/')[-1].split('/executions/')[0]
                    
                    # Filtrar logs del execution específico
                    # Los logs de Cloud Run Jobs incluyen el execution ID en los labels o en el texto
                    log_filter = f'resource.type="cloud_run_job" AND (textPayload=~".*{execution_id}.*" OR jsonPayload.execution_name="{execution_id}" OR labels.execution_name="{execution_id}") AND severity>=ERROR'
                    
                    entries = logging_client.list_entries(
                        filter_=log_filter,
                        max_results=3,
                        order_by=cloud_logging.DESCENDING
                    )
                    
                    error_logs = []
                    for entry in entries:
                        if hasattr(entry, 'payload'):
                            if isinstance(entry.payload, str):
                                # Buscar líneas que contengan "ERROR"
                                if 'ERROR' in entry.payload or 'Error' in entry.payload:
                                    error_logs.append(entry.payload[:500])  # Limitar longitud
                            elif hasattr(entry.payload, 'get'):
                                payload_str = str(entry.payload)
                                if 'ERROR' in payload_str or 'Error' in payload_str:
                                    error_logs.append(payload_str[:500])
                        elif hasattr(entry, 'text_payload') and entry.text_payload:
                            if 'ERROR' in entry.text_payload or 'Error' in entry.text_payload:
                                error_logs.append(entry.text_payload[:500])
                    
                    if error_logs:
                        # Tomar solo los primeros 2 logs más relevantes
                        error_details.append(f"Container errors: {' | '.join(error_logs[:2])}")
                except Exception as log_error:
                    pass
                
                full_error_msg = error_msg
                if error_details:
                    full_error_msg += f" | {' | '.join(error_details)}"
                
                raise RuntimeError(f"{full_error_msg}. Execution: {execution_name}")
            else:
                time.sleep(poll_interval)
                
        except RuntimeError:
            raise
        except Exception:
            time.sleep(poll_interval)

def _get_cloud_run_connection():
    """
    Obtiene la conexión de Cloud Run si está disponible.
    
    Returns:
    - Tuple (credentials, region, project_id) si hay conexión válida
    - None en caso contrario
    """
    try:
        from airflow.providers.google.cloud.hooks.cloud_run import CloudRunHook  # type: ignore
        
        region = Variable.get('GCP_CLOUD_RUN_REGION', default_var=None)
        project_id = Variable.get('GCP_PROJECT_ID', default_var=None)
        gcp_conn_id = Variable.get('GCP_CONNECTION_ID', default_var='google_cloud_default')
        
        if region and project_id:
            try:
                hook = CloudRunHook(gcp_conn_id=gcp_conn_id)
                credentials = hook.get_credentials()
                return (credentials, region, project_id)
            except Exception:
                return None
        return None
    except (ImportError, Exception):
        return None


def exec_gcp_ducklake_ingestor(
    table_name: str,
    url: str,
    is_s3_path: bool = True,
    original_url: str = None,
    **context
) -> Dict:
    """
    Ejecuta un Cloud Run Job para mergear datos CSV en DuckDB.
    
    Esta función ejecuta un Cloud Run Job que:
    1. Lee el CSV desde RustFS S3 (s3://...)
    2. Mergea los datos en la tabla DuckDB especificada
    3. Se ejecuta y termina automáticamente
    
    Parameters:
    - table_name: Nombre de la tabla (sin prefijo 'bronze_')
    - url: Ruta S3 del archivo CSV (s3://bucket/key)
    - is_s3_path: Siempre True (mantenido para compatibilidad, pero siempre debe ser S3)
    - original_url: URL original para logging/auditing (opcional)
    - **context: Contexto de Airflow (se pasa automáticamente)
    
    Returns:
    - Dict con información del resultado
    
    Raises:
    - ValueError: Si falta configuración requerida
    - RuntimeError: Si el job falla
    """    
    if not url.startswith("s3://"):
        raise ValueError(
            f"Invalid S3 path: {url}. "
            "Expected format: s3://bucket/key."
        )
    
    conn_info = _get_cloud_run_connection()
    if not conn_info:
        raise ValueError("Cloud Run connection not available. Check Airflow Variables: GCP_CLOUD_RUN_REGION, GCP_PROJECT_ID")
    
    credentials, region, project_id = conn_info
    job_name = Variable.get('GCP_CLOUD_RUN_JOB_NAME', default_var=None)
    
    if not job_name:
        raise ValueError("GCP_CLOUD_RUN_JOB_NAME Airflow Variable must be set")
    
    env_vars_list = [
        {'name': 'TABLE_NAME', 'value': table_name},
        {'name': 'URL', 'value': url},
    ]
    
    if original_url:
        env_vars_list.append({'name': 'ORIGINAL_URL', 'value': original_url})
    
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
        
        return {
            'status': 'success',
            'message': f'Data merged successfully into {table_name}',
            'table_name': table_name,
            'url': url,
            'execution_name': execution_name,
            'execution_time_seconds': int(elapsed_time)
        }
        
    except Exception as e:
        error_msg = f"Failed to execute Cloud Run Job: {str(e)}"
        raise RuntimeError(error_msg) from e


def exec_gcp_ducklake_executor(
    sql_query: str,
    post_process_func=None,
    **context
) -> Dict:
    """
    Ejecuta un Cloud Run Job para ejecutar una consulta SQL en DuckDB.
    
    Esta función ejecuta un Cloud Run Job que:
    1. Se conecta a DuckLake
    2. Ejecuta la consulta SQL proporcionada
    3. Se ejecuta y termina automáticamente
    4. Opcionalmente ejecuta una función de postprocesamiento localmente
    
    Parameters:
    - sql_query: Consulta SQL a ejecutar (puede contener múltiples sentencias separadas por ;)
    - post_process_func: Función opcional que se ejecuta después de la SQL.
                        Recibe (con, result_dict) donde:
                        - con: conexión DuckDB (para queries adicionales)
                        - result_dict: dict con el resultado de la ejecución
                        Debe retornar un dict que se mergeará con el resultado.
    - **context: Contexto de Airflow (se pasa automáticamente)
    
    Returns:
    - Dict con información del resultado (mergeado con el resultado de post_process_func si existe)
    
    Raises:
    - ValueError: Si falta configuración requerida
    - RuntimeError: Si el job falla
    """    
    if not sql_query or not sql_query.strip():
        raise ValueError("SQL query cannot be empty")
    
    conn_info = _get_cloud_run_connection()
    if not conn_info:
        raise ValueError("Cloud Run connection not available. Check Airflow Variables: GCP_CLOUD_RUN_REGION, GCP_PROJECT_ID")
    
    credentials, region, project_id = conn_info
    job_name = Variable.get('GCP_CLOUD_RUN_EXECUTOR_JOB_NAME', default_var='ducklake-executor')
    
    # Get S3 credentials from Airflow connection to pass to Cloud Run
    try:
        from airflow.sdk.bases.hook import BaseHook
        s3_conn = BaseHook.get_connection('rustfs_s3_conn')
        s3_extra = s3_conn.extra_dejson
        endpoint_url = s3_extra.get('endpoint_url', 'http://rustfs:9000')
        s3_endpoint = endpoint_url.replace('http://', '').replace('https://', '')
        rustfs_user = s3_extra.get('aws_access_key_id', 'admin')
        rustfs_password = s3_extra.get('aws_secret_access_key', 'muceim-duckduck.2025!')
        rustfs_ssl = 'true' if 'https' in endpoint_url else 'false'
        rustfs_bucket = Variable.get('RUSTFS_BUCKET', default_var='mitma')
    except Exception as e:
        print(f"[WARNING] Could not get S3 credentials from Airflow connection: {str(e)}")
        # Use defaults if connection not available
        s3_endpoint = "rustfs:9000"
        rustfs_user = "admin"
        rustfs_password = "muceim-duckduck.2025!"
        rustfs_ssl = "false"
        rustfs_bucket = Variable.get('RUSTFS_BUCKET', default_var='mitma')
    
    env_vars_list = [
        {'name': 'SQL_QUERY', 'value': sql_query},
        {'name': 'S3_ENDPOINT', 'value': s3_endpoint},
        {'name': 'RUSTFS_USER', 'value': rustfs_user},
        {'name': 'RUSTFS_PASSWORD', 'value': rustfs_password},
        {'name': 'RUSTFS_SSL', 'value': rustfs_ssl},
        {'name': 'RUSTFS_BUCKET', 'value': rustfs_bucket},
    ]
    
    # Add extra environment variables from context if provided
    extra_env_vars = context.get('extra_env_vars', {})
    for key, value in extra_env_vars.items():
        env_vars_list.append({'name': key, 'value': str(value)})
    
    # Si hay función de postprocesamiento, serializarla como código Python
    if post_process_func:
        import inspect
        import base64
        import zlib
        
        # Obtener el código fuente de la función
        try:
            import textwrap
            func_code = inspect.getsource(post_process_func)
            
            # Eliminar la indentación común del código (textwrap.dedent)
            # Esto asegura que el código se ejecute correctamente sin problemas de indentación
            func_code = textwrap.dedent(func_code)
            
            # Intentar obtener las variables globales que la función necesita
            # (como constantes SQL definidas en el módulo)
            func_globals = post_process_func.__globals__ if hasattr(post_process_func, '__globals__') else {}
            
            # Incluir constantes SQL si existen (buscar patrones como *_SQL)
            # Los imports ya están dentro de la función, no necesitamos detectarlos
            constants = []
            for name, value in func_globals.items():
                if name.endswith('_SQL') and isinstance(value, str):
                    constants.append(f"{name} = {repr(value)}")
            
            # Combinar constantes con el código de la función
            # Los imports ya están dentro de la función
            if constants:
                full_code = '\n'.join(constants) + '\n\n' + func_code
            else:
                full_code = func_code
            
            print(f"[INFO] Serialized post_process_func (imports are inside the function)")
            
            # Comprimir y codificar en base64 para pasarlo como variable de entorno
            compressed = zlib.compress(full_code.encode('utf-8'))
            encoded = base64.b64encode(compressed).decode('utf-8')
            env_vars_list.append({'name': 'POST_PROCESS_CODE', 'value': encoded})
            env_vars_list.append({'name': 'POST_PROCESS_FUNC_NAME', 'value': post_process_func.__name__})
        except Exception as e:
            print(f"[WARNING] Could not serialize post_process_func: {str(e)}")
            print("[WARNING] Post-processing will be skipped in Cloud Run")
    
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
        
        result = {
            'status': 'success',
            'message': 'SQL query executed successfully',
            'execution_name': execution_name,
            'execution_time_seconds': int(elapsed_time)
        }
        
        # Intentar recuperar el resultado del post-procesamiento desde los logs de Cloud Run
        if post_process_func and 'POST_PROCESS_CODE' in [e['name'] for e in env_vars_list]:
            try:
                from google.cloud import logging as cloud_logging
                import json
                
                logging_client = cloud_logging.Client()
                execution_id = execution_name.split('/')[-1]
                
                # Buscar el resultado en los logs
                log_filter = f'resource.type="cloud_run_job" AND textPayload=~".*POST_PROCESS_RESULT_JSON.*"'
                entries = logging_client.list_entries(
                    filter_=log_filter,
                    max_results=10,
                    order_by=cloud_logging.DESCENDING
                )
                
                for entry in entries:
                    if hasattr(entry, 'text_payload') and entry.text_payload:
                        # Buscar la línea con el resultado
                        for line in entry.text_payload.split('\n'):
                            if 'POST_PROCESS_RESULT_JSON:' in line:
                                result_json_str = line.split('POST_PROCESS_RESULT_JSON:', 1)[1].strip()
                                post_result = json.loads(result_json_str)
                                if post_result and isinstance(post_result, dict):
                                    result.update(post_result)
                                    print(f"[INFO] Post-processing result retrieved from Cloud Run logs")
                                    break
                        if 'best_k_value' in result or 'records' in result or 'table' in result:
                            break
            except Exception as log_error:
                # Si no se pueden leer los logs, no es crítico - el post-procesamiento se ejecutó
                print(f"[INFO] Post-processing executed in Cloud Run (could not retrieve result from logs: {str(log_error)})")
        
        # Si no hay Cloud Run o la función no se pudo serializar, ejecutar localmente
        if post_process_func and 'POST_PROCESS_CODE' not in [e['name'] for e in env_vars_list]:
            from utils.utils import get_ducklake_connection
            con = get_ducklake_connection()
            try:
                # Detectar si es SELECT para capturar el DataFrame
                sql_upper = sql_query.strip().upper()
                is_select = sql_upper.startswith('SELECT') or sql_upper.startswith('WITH')
                
                if is_select:
                    df_result = con.execute(sql_query).fetchdf()
                    post_result = post_process_func(df_result, con, result)
                else:
                    post_result = post_process_func(None, con, result)
                    
                if post_result and isinstance(post_result, dict):
                    result.update(post_result)
            except Exception as e:
                print(f"[WARNING] Post-processing function failed: {str(e)}")
        
        return result
        
    except Exception as e:
        error_msg = f"Failed to execute Cloud Run Job: {str(e)}"
        raise RuntimeError(error_msg) from e


def execute_sql_or_cloud_run(sql_query: str, post_process_func=None, **context) -> Dict:
    """
    Ejecuta una consulta SQL usando Cloud Run si está disponible, o localmente como fallback.
    Esta función decide automáticamente qué método usar.
    
    Parameters:
    - sql_query: Consulta SQL a ejecutar
    - post_process_func: Función opcional que se ejecuta después de la SQL.
                        Recibe (con, result_dict) donde:
                        - con: conexión DuckDB (para queries adicionales)
                        - result_dict: dict con el resultado de la ejecución
                        Debe retornar un dict que se mergeará con el resultado.
    - **context: Contexto de Airflow
    
    Returns:
    - Dict con información del resultado (mergeado con el resultado de post_process_func si existe)
    """
    if _get_cloud_run_connection():
        return exec_gcp_ducklake_executor(sql_query=sql_query, post_process_func=post_process_func, **context)
    else:
        # Ejecutar localmente
        import time
        from utils.utils import get_ducklake_connection
        
        start_time = time.time()
        
        try:
            con = get_ducklake_connection()
            
            # Detectar si es SELECT para capturar el DataFrame
            sql_upper = sql_query.strip().upper()
            is_select = sql_upper.startswith('SELECT') or sql_upper.startswith('WITH')
            
            df_result = None
            if is_select and post_process_func:
                # Si es SELECT y hay post-procesamiento, capturar el DataFrame
                df_result = con.execute(sql_query).fetchdf()
            else:
                # Si no es SELECT o no hay post-procesamiento, ejecutar normalmente
                con.execute(sql_query)
            
            elapsed_time = time.time() - start_time
            
            result = {
                'status': 'success',
                'message': 'SQL query executed successfully (local)',
                'execution_name': 'local',
                'execution_time_seconds': int(elapsed_time)
            }
            
            # Ejecutar función de postprocesamiento si existe
            if post_process_func:
                try:
                    # Pasar DataFrame como primer parámetro si existe
                    if df_result is not None:
                        post_result = post_process_func(df_result, con, result)
                    else:
                        post_result = post_process_func(None, con, result)
                    
                    if post_result and isinstance(post_result, dict):
                        result.update(post_result)
                except Exception as e:
                    print(f"[WARNING] Post-processing function failed: {str(e)}")
                    # No fallamos la tarea si el post-procesamiento falla, solo logueamos
            
            return result
            
        except Exception as e:
            error_msg = f"Failed to execute SQL locally: {str(e)}"
            raise RuntimeError(error_msg) from e


def merge_csv_or_cloud_run(
    table_name: str,
    url: str,
    original_url: str = None,
    **context
) -> Dict:
    """
    Mergea datos CSV usando Cloud Run si está disponible, o localmente como fallback.
    Esta función decide automáticamente qué método usar.
    Detecta automáticamente si la URL es una ruta S3 (empieza con s3://) o una URL HTTP.
    
    Parameters:
    - table_name: Nombre de la tabla (sin prefijo 'bronze_')
    - url: Ruta S3 del archivo CSV (s3://bucket/key) o URL HTTP
    - original_url: URL original para logging/auditing (opcional)
    - **context: Contexto de Airflow
    
    Returns:
    - Dict con información del resultado
    """
    is_s3_path = url.startswith("s3://")
    
    if _get_cloud_run_connection() and is_s3_path:
        return exec_gcp_ducklake_ingestor(
            table_name=table_name,
            url=url,
            original_url=original_url,
            **context
        )
    else:
        # Ejecutar localmente usando merge_from_csv de bronze.utils
        from bronze.utils import merge_from_csv
        import time
        
        start_time = time.time()
        
        try:
            # merge_from_csv espera el nombre sin prefijo, y la URL puede ser S3 o HTTP
            merge_from_csv(table_name, url)
            
            elapsed_time = time.time() - start_time
            
            return {
                'status': 'success',
                'message': f'Data merged successfully into {table_name} (local)',
                'table_name': table_name,
                'url': url,
                'execution_name': 'local',
                'execution_time_seconds': int(elapsed_time)
            }
        except Exception as e:
            error_msg = f"Failed to merge CSV locally: {str(e)}"
            raise RuntimeError(error_msg) from e
