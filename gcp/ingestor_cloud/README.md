# Cloud Run Job: insert-ducklake

Este Cloud Run Job descarga CSVs desde URLs y los inserta en DuckDB usando el ancho de banda de Google Cloud.

## Estructura

```
cloud_run_job/
├── main.py           # Código principal del job
├── Dockerfile        # Imagen Docker para el job
├── requirements.txt  # Dependencias Python
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
   - **Name**: `build-insert-ducklake`
   - **Event**: Push to a branch
   - **Source**: Conecta tu repositorio (GitHub/GitLab)
   - **Branch**: `main` o `master`
   - **Configuration**: Cloud Build configuration file (yaml)
   - **Location**: `cloud_run_job/cloudbuild.yaml` (crea este archivo, ver abajo)
4. Haz clic en **CREATE**

**Nota**: El archivo `cloud_run_job/cloudbuild.yaml` ya está creado y configurado para usar Artifact Registry.

5. Haz push a tu repositorio para activar el build

#### 3. Crear el Cloud Run Job desde la interfaz

1. Ve a **Cloud Run** > **Jobs**: https://console.cloud.google.com/run/jobs
2. Haz clic en **CREATE JOB**
3. Configuración:
   - **Job name**: `insert-ducklake`
   - **Region**: `europe-southwest1` (Madrid)
   - **Container image URL**: `europe-southwest1-docker.pkg.dev/muceim-bigdata/cloud-run-source-deploy/insert-ducklake:latest`
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
     - Si no configuras esto correctamente, el job **NO podrá acceder a internet** (por ejemplo, para descargar CSVs desde URLs públicas)
     - Ver más detalles en la sección "⚠️ Configuración de Red y Acceso a Internet" más abajo

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

El job se ejecuta automáticamente desde Airflow usando la función `exec_gcp_ducklake_ingestor()` en `dags/utils/gcp.py`.

Las variables de entorno `TABLE_NAME`, `URL`, y `ZONE_TYPE` se pasan automáticamente desde Airflow.

## ⚠️ Configuración de Acceso a Internet

**NOTA**: Si ves `curl exit code: 0` pero recibes `HTTP 500` u otros códigos de error HTTP, Cloud Run **SÍ tiene acceso a internet**. El problema es que el servidor remoto está fallando, no la configuración de Cloud Run.

Si tu Cloud Run job no puede acceder a internet (por ejemplo, `curl` falla con errores de conexión o DNS):

### Verificación Inicial

Primero, verifica si tienes VPC Connector configurado:

```bash
gcloud run jobs describe insert-ducklake \
  --region=europe-southwest1 \
  --format="get(template.template.vpcAccess.connector)"
```

- Si devuelve un valor (nombre de connector): Tienes VPC configurado → ve a "Solución A"
- Si está vacío: No tienes VPC → ve a "Solución B"

---

### Solución A: Si tienes VPC Connector configurado

Si tus recursos están en servicios externos accesibles por internet (no en una VPC privada de Google Cloud), **NO necesitas VPC Connector**. Remuévelo:

#### Desde la Consola de Google Cloud

1. Ve a [Cloud Run Jobs](https://console.cloud.google.com/run/jobs)
2. Haz clic en tu job (`insert-ducklake`)
3. Haz clic en **EDIT**
4. Expande **Networking, Security, Limits & Admin**
5. En **VPC connector**, cambia a **"No VPC connector"**
6. Haz clic en **SAVE**

#### Desde línea de comandos

```bash
gcloud run jobs update insert-ducklake \
  --region=europe-southwest1 \
  --clear-vpc-connector
```

---

### Solución B: Si NO tienes VPC Connector (y aún así no funciona) 🔍

**Si verificaste que `vpcAccess: null` y aún así no funciona**, sigue estos pasos de diagnóstico:

#### 1. Organization Policies

Revisa si hay políticas de organización que bloqueen el acceso a internet:

```bash
# Verificar constraints de red
gcloud resource-manager org-policies list \
  --project=TU_PROJECT_ID \
  --format="table(constraint)"
```

Busca políticas relacionadas con `compute.vmCanIpForward` o `compute.restrictVpcPeering`.

#### 2. Firewall Rules

Verifica que no haya reglas de firewall bloqueando el tráfico saliente (aunque Cloud Run no debería verse afectado por firewall rules de VPC).

#### 3. Service Account Permissions

Asegúrate de que la Service Account del job tenga los permisos necesarios (aunque esto no debería afectar el acceso a internet).

#### 4. Revisar Logs del Job ⚠️ IMPORTANTE

Revisa los logs del Cloud Run job para ver el error específico que está dando `curl`:

```bash
# Ver logs del job
gcloud logging read "resource.type=cloud_run_job AND resource.labels.job_name=insert-ducklake" \
  --limit=50 \
  --format="table(timestamp,textPayload)"

# O filtrar solo errores de curl
gcloud logging read "resource.type=cloud_run_job AND resource.labels.job_name=insert-ducklake AND textPayload=~\"curl\"" \
  --limit=20 \
  --format="table(timestamp,textPayload)"
```

O desde la consola: [Cloud Run Jobs > insert-ducklake > Logs](https://console.cloud.google.com/run/jobs)

**Busca estos errores comunes:**
- `curl: (6) Could not resolve host` → Problema de DNS
- `curl: (7) Failed to connect` → No puede conectar (firewall/proxy)
- `curl: (28) Timeout` → Timeout después de 30 segundos
- `curl: (35) SSL connect error` → Problema con SSL/TLS

#### 5. Probar con un job de prueba simple

Crea un job de prueba mínimo para verificar el acceso a internet:

```python
import subprocess
result = subprocess.run(["curl", "-I", "https://www.google.com"], capture_output=True, text=True)
print(result.stdout)
print(result.stderr)
```

---

### Si necesitas VPC Connector (solo si tus recursos están en una VPC privada)

Si necesitas acceder a recursos privados en tu VPC **Y** también necesitas internet:

1. Ve a la consola
2. En **VPC egress**, selecciona **"Private ranges"**
3. Esto permite: acceso a internet público directo + acceso a rangos privados vía VPC

O desde línea de comandos:
```bash
gcloud run jobs update insert-ducklake \
  --region=europe-southwest1 \
  --vpc-egress=private-ranges-only
```

## Testing local

Para probar localmente antes de desplegar:

```bash
# Instalar dependencias
pip install -r requirements.txt

# Configurar variables de entorno
export TABLE_NAME=mitma_od_municipios
export URL=https://ejemplo.com/data.csv
export ZONE_TYPE=municipios
export POSTGRES_HOST=...
# ... resto de variables

# Ejecutar
python main.py
```

