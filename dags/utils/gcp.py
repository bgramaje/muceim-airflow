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
from airflow.models import Variable  # type: ignore
from airflow.providers.google.cloud.hooks.cloud_run import CloudRunHook  # type: ignore
from google.cloud import run_v2  # type: ignore

def execute_cloud_run_job_merge_csv(
    table_name: str,
    url: str,
    zone_type: str = 'distritos',
    **context
) -> Dict:
    """
    Ejecuta un Cloud Run Job para mergear datos CSV en DuckDB.
    
    Esta función ejecuta un Cloud Run Job que:
    1. Descarga el CSV desde la URL (usando el ancho de banda de GCP)
    2. Mergea los datos en la tabla DuckDB especificada
    3. Se ejecuta y termina automáticamente
    
    Parameters:
    - table_name: Nombre de la tabla (sin prefijo 'bronze_')
    - url: URL del archivo CSV a descargar y mergear
    - zone_type: Tipo de zona (ej: 'distritos', 'municipios', 'gau')
    - **context: Contexto de Airflow (se pasa automáticamente)
    
    Returns:
    - Dict con información del resultado
    
    Raises:
    - ValueError: Si falta configuración requerida
    - RuntimeError: Si el job falla
    
    Required Airflow Variables:
    - GCP_CLOUD_RUN_JOB_NAME: Nombre del Cloud Run Job
    - GCP_CLOUD_RUN_REGION: Región donde está el Job
    - GCP_PROJECT_ID: ID del proyecto de GCP
    """
    # Get configuration from Airflow Variables
    job_name = Variable.get('GCP_CLOUD_RUN_JOB_NAME', default_var=None)
    region = Variable.get('GCP_CLOUD_RUN_REGION', default_var=None)
    project_id = Variable.get('GCP_PROJECT_ID', default_var=None)
    gcp_conn_id = Variable.get('GCP_CONNECTION_ID', default_var='google_cloud_default')
    
    if not job_name:
        raise ValueError(
            "GCP_CLOUD_RUN_JOB_NAME Airflow Variable must be set. "
            "Please set it to your Cloud Run Job name (e.g., 'insert-ducklake')"
        )
    
    if not region:
        raise ValueError(
            "GCP_CLOUD_RUN_REGION Airflow Variable must be set. "
            "Please set it to your job region (e.g., 'europe-southwest1')"
        )
    
    if not project_id:
        raise ValueError(
            "GCP_PROJECT_ID Airflow Variable must be set. "
            "Please set it to your GCP project ID"
        )
    
    env_vars_list = [
        {'name': 'TABLE_NAME', 'value': table_name},
        {'name': 'URL', 'value': url},
        {'name': 'ZONE_TYPE', 'value': zone_type}
    ]
    
    print(f"[CLOUD_RUN_JOB] Executing job: {job_name}")
    print(f"[CLOUD_RUN_JOB] Region: {region}, Project: {project_id}")
    print(f"[CLOUD_RUN_JOB] Environment variables: {env_vars_list}")
    
    try:
        # Usar CloudRunHook para obtener las credenciales y el cliente
        hook = CloudRunHook(gcp_conn_id=gcp_conn_id)
        
        # Obtener las credenciales desde el hook
        credentials = hook.get_credentials()
        
        # Crear el cliente de Jobs
        client = run_v2.JobsClient(credentials=credentials)
        
        # Construir el nombre completo del job
        job_path = f"projects/{project_id}/locations/{region}/jobs/{job_name}"
        
        # Crear el request para ejecutar el job con overrides
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
        
        # Ejecutar el job
        print(f"[CLOUD_RUN_JOB] Executing job at: {job_path}")
        operation = client.run_job(request=request)
        
        # El job se ejecuta de forma asíncrona
        execution_name = operation.name if hasattr(operation, 'name') else None
        print(f"[CLOUD_RUN_JOB] ✅ Job execution started")
        print(f"[CLOUD_RUN_JOB] Execution: {execution_name}")
        
        # Nota: El job se ejecutará y terminará automáticamente
        # No esperamos aquí porque es un job que se ejecuta de forma asíncrona
        
        return {
            'status': 'success',
            'message': f'Data merged successfully into bronze_{table_name}',
            'table_name': f'bronze_{table_name}',
            'url': url,
            'execution_name': execution_name
        }
        
    except Exception as e:
        error_msg = f"Failed to execute Cloud Run Job: {str(e)}"
        print(f"[CLOUD_RUN_JOB] ❌ {error_msg}")
        import traceback
        print(f"[CLOUD_RUN_JOB] Traceback: {traceback.format_exc()}")
        raise RuntimeError(error_msg) from e
