# Cloud Run Job: execute-sql

Este Cloud Run Job ejecuta consultas SQL personalizadas en DuckDB (DuckLake).

## Estructura

```
gcp/executor_cloud/
├── main.py           # Código principal del job
├── Dockerfile        # Imagen Docker para el job
├── cloudbuild.yaml   # Configuración de Cloud Build
└── README.md         # Esta documentación
```

## Despliegue

### ⚠️ IMPORTANTE: Crear repositorio de Artifact Registry y dar permisos

Antes de ejecutar el build, necesitas:

#### 1. Crear el repositorio de Artifact Registry

1. Ve a **Artifact Registry** > **Repositories**: https://console.cloud.google.com/artifacts
2. Haz clic en **CREATE REPOSITORY**
3. Configuración:
   - **Name**: `cloud-run-source-deploy`
   - **Format**: Docker
   - **Mode**: Standard
   - **Region**: `europe-southwest1` (Madrid)
4. Haz clic en **CREATE**

#### 2. Dar permisos a Cloud Build Service Account

La Service Account de Cloud Build necesita permisos para subir imágenes. Hazlo desde la interfaz:

1. Ve al repositorio que acabas de crear: `cloud-run-source-deploy`
2. Haz clic en la pestaña **PERMISSIONS**
3. Haz clic en **ADD PRINCIPAL**
4. En **New principals**, busca: `PROJECT_NUMBER@cloudbuild.gserviceaccount.com`
   - Para encontrar tu PROJECT_NUMBER: Ve a **IAM & Admin** > **Settings** y copia el "Project number"
   - Ejemplo: `123456789012@cloudbuild.gserviceaccount.com`
5. En **Role**, selecciona: **Artifact Registry Writer**
6. Haz clic en **SAVE**

**Alternativa rápida**: Si no encuentras el PROJECT_NUMBER, puedes dar permisos a nivel de proyecto:
- Ve a **IAM & Admin** > **IAM**
- Busca la Service Account de Cloud Build (formato: `PROJECT_NUMBER@cloudbuild.gserviceaccount.com`)
- Agrega el rol: **Artifact Registry Writer** a nivel de proyecto

**Alternativa**: Si prefieres usar otro nombre de repositorio, actualiza `cloudbuild.yaml` y reemplaza `cloud-run-source-deploy` con tu nombre.

---

Tienes dos opciones para construir la imagen: usar la interfaz web (más fácil, sin CLI) o usar gcloud CLI.

### Opción A: Desde la Interfaz Web (Recomendado - No requiere CLI)

#### 1. Subir el código a un repositorio

Primero, necesitas tener el código en un repositorio accesible:
- GitHub
- GitLab
- O cualquier repositorio Git

#### 2. Construir la imagen con Cloud Build

1. Ve a **Cloud Build** > **Triggers**: https://console.cloud.google.com/cloud-build/triggers
2. Haz clic en **CREATE TRIGGER**
3. Configuración:
   - **Name**: `build-execute-sql`
   - **Event**: Push to a branch
   - **Source**: Conecta tu repositorio (GitHub/GitLab)
   - **Branch**: `main` o `master`
   - **Configuration**: Cloud Build configuration file (yaml)
   - **Location**: `gcp/executor_cloud/cloudbuild.yaml`
4. Haz clic en **CREATE**

5. Haz push a tu repositorio para activar el build

#### 3. Crear el Cloud Run Job desde la interfaz

1. Ve a **Cloud Run** > **Jobs**: https://console.cloud.google.com/run/jobs
2. Haz clic en **CREATE JOB**
3. Configuración:
   - **Job name**: `execute-sql`
   - **Region**: `europe-southwest1` (Madrid)
   - **Container image URL**: `europe-southwest1-docker.pkg.dev/muceim-bigdata/cloud-run-source-deploy/execute-sql:latest`
   - **CPU**: 2 (o más según necesites)
   - **Memory**: 4Gi (o más según necesites)
   - **Timeout**: 3600s (1 hora) o más
   - **Max retries**: 0 (o 1-2 si quieres reintentos)

4. **Variables de entorno** (se pasan desde Airflow, pero puedes definir defaults):
   - `POSTGRES_HOST`: (tu host de PostgreSQL)
   - `POSTGRES_PORT`: `5432`
   - `POSTGRES_DB`: (tu base de datos)
   - `POSTGRES_USER`: (tu usuario)
   - `POSTGRES_PASSWORD`: (tu contraseña) - **Usa Secret Manager**
   - `S3_ENDPOINT`: (tu endpoint RustFS, ej: `rustfs:9000`)
   - `RUSTFS_USER`: (tu usuario RustFS)
   - `RUSTFS_PASSWORD`: (tu contraseña RustFS) - **Usa Secret Manager**
   - `RUSTFS_BUCKET`: `mitma`
   - `RUSTFS_SSL`: `false` o `true`
   - `SQL_QUERY`: (la consulta SQL a ejecutar - se pasa desde Airflow)

5. **Secrets** (recomendado para contraseñas):
   - Ve a **Secrets** y agrega:
     - `POSTGRES_PASSWORD` → Secret Manager secret
     - `RUSTFS_PASSWORD` → Secret Manager secret

6. **VPC Connector** (si PostgreSQL/RustFS están en una VPC):
   - Si tus recursos están en una VPC privada, configura un VPC Connector
   - ⚠️ **IMPORTANTE - Acceso a Internet**: Si configuras un VPC Connector, también debes configurar el **VPC egress**:
     - **VPC egress**: Selecciona **"All traffic"** o **"Private ranges"** según tus necesidades
     - Si seleccionas **"All traffic"**, el tráfico a internet pasará por la VPC (necesitarás NAT Gateway)
     - Si solo necesitas acceso a recursos privados, selecciona **"Private ranges"** y mantén el acceso directo a internet público

7. Haz clic en **CREATE**

### 3. Permisos

Asegúrate de que la Service Account de Airflow tenga:
- `roles/run.invoker` - Para ejecutar el job

```bash
gcloud projects add-iam-policy-binding muceim-bigdata \
  --member="serviceAccount:muceim-cloud-runner@muceim-bigdata.iam.gserviceaccount.com" \
  --role="roles/run.invoker"
```

## Uso desde Airflow

El job se ejecuta pasando la variable de entorno `SQL_QUERY` con la consulta SQL a ejecutar.

Ejemplo de uso desde Python:

```python
from google.cloud import run_v2

client = run_v2.JobsClient()

job_name = "projects/muceim-bigdata/locations/europe-southwest1/jobs/execute-sql"

request = run_v2.RunJobRequest(
    name=job_name,
    overrides=run_v2.RunJobRequest.Overrides(
        container_overrides=[
            run_v2.RunJobRequest.Overrides.ContainerOverrides(
                env=[
                    {"name": "SQL_QUERY", "value": "SELECT COUNT(*) FROM bronze_mitma_od_municipios;"}
                ]
            )
        ]
    )
)

operation = client.run_job(request=request)
result = operation.result()
```

## Variables de Entorno

### Requeridas
- `SQL_QUERY`: La consulta SQL a ejecutar en DuckLake

### Opcionales (para conexión a DuckLake)
- `POSTGRES_HOST`: Host de PostgreSQL
- `POSTGRES_PORT`: Puerto de PostgreSQL (default: 5432)
- `POSTGRES_DB`: Nombre de la base de datos
- `POSTGRES_USER`: Usuario de PostgreSQL
- `POSTGRES_PASSWORD`: Contraseña de PostgreSQL
- `S3_ENDPOINT`: Endpoint de RustFS (default: rustfs:9000)
- `RUSTFS_USER`: Usuario de RustFS
- `RUSTFS_PASSWORD`: Contraseña de RustFS
- `RUSTFS_BUCKET`: Bucket de RustFS (default: mitma)
- `RUSTFS_SSL`: SSL habilitado (default: false)

## Testing local

Para probar localmente antes de desplegar:

```bash
# Instalar dependencias
pip install -r requirements.txt

# Configurar variables de entorno
export SQL_QUERY="SELECT COUNT(*) FROM bronze_mitma_od_municipios;"
export POSTGRES_HOST=...
export POSTGRES_PORT=5432
export POSTGRES_DB=...
export POSTGRES_USER=...
export POSTGRES_PASSWORD=...
export S3_ENDPOINT=rustfs:9000
export RUSTFS_USER=...
export RUSTFS_PASSWORD=...
export RUSTFS_BUCKET=mitma
export RUSTFS_SSL=false

# Ejecutar
python main.py
```

## Notas

- El job ejecuta la consulta SQL tal como se proporciona. Asegúrate de que la consulta sea válida y segura.
- Para consultas que devuelven resultados, se mostrará un preview de las primeras 5 filas en los logs.
- Para consultas que no devuelven resultados (INSERT/UPDATE/DELETE/MERGE), se confirmará la ejecución exitosa.
- El job usa configuración optimizada para 32 GiB RAM y 8 CPUs por defecto.

