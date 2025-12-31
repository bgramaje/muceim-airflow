# Diagnóstico: Cloud Run Job sin Acceso a Internet

## Situación
- ✅ No tienes VPC connector configurado (`vpcAccess: null`)
- ❌ El job no puede acceder a internet (curl/ping fallan)

## Pasos de Diagnóstico

### 1. Ver los logs del job

```bash
# Ver los últimos logs del job
gcloud logging read "resource.type=cloud_run_job AND resource.labels.job_name=insert-ducklake" \
  --limit=50 \
  --format="table(timestamp,textPayload)" \
  --project=TU_PROJECT_ID

# O filtrar solo los errores de curl
gcloud logging read "resource.type=cloud_run_job AND resource.labels.job_name=insert-ducklake AND textPayload=~\"curl\"" \
  --limit=20 \
  --format="table(timestamp,textPayload)"
```

**O desde la consola:**
1. Ve a [Cloud Run Jobs](https://console.cloud.google.com/run/jobs)
2. Haz clic en `insert-ducklake`
3. Ve a la pestaña **LOGS**

Busca mensajes que contengan:
- `curl exit code`
- `curl stderr`
- `Error executing curl`
- `timed out`

### 2. Verificar el error específico

Los errores comunes son:

- **`curl: (6) Could not resolve host`** → Problema de DNS
- **`curl: (7) Failed to connect`** → No puede establecer conexión (firewall/proxy)
- **`curl: (28) Timeout`** → Timeout (30 segundos)
- **`curl: (35) SSL connect error`** → Problema con SSL/TLS
- **`Connection refused`** → El servicio rechaza la conexión

### 3. Verificar configuración del proyecto

```bash
# Verificar si hay organization policies que bloqueen acceso
gcloud resource-manager org-policies list \
  --project=TU_PROJECT_ID

# Verificar la configuración completa del job
gcloud run jobs describe insert-ducklake \
  --region=europe-southwest1 \
  --format=yaml
```

### 4. Probar con un job de prueba simple

Crea un job de prueba mínimo para verificar el acceso:

```python
import subprocess
import sys

# Probar diferentes comandos
tests = [
    ("ping", ["ping", "-c", "1", "8.8.8.8"]),  # Ping a Google DNS
    ("curl_google", ["curl", "-I", "https://www.google.com"]),
    ("nslookup", ["nslookup", "www.google.com"]),
]

for name, cmd in tests:
    print(f"\n=== Testing {name} ===")
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=10)
        print(f"Exit code: {result.returncode}")
        print(f"Stdout: {result.stdout[:500]}")
        if result.stderr:
            print(f"Stderr: {result.stderr[:500]}")
    except Exception as e:
        print(f"Error: {e}")
```

## Posibles Soluciones

### Si el error es DNS (`Could not resolve host`)

Puede ser un problema con la resolución DNS. Verifica:
- Si estás usando DNS personalizado
- Si hay políticas de organización bloqueando DNS

### Si el error es timeout

Aumenta el timeout en el código o verifica si hay firewalls bloqueando.

### Si el error es connection refused/failed to connect

Puede haber:
- Firewall rules bloqueando el tráfico
- Organization policies
- Proxy requerido

### Si nada funciona

Verifica:
1. ¿El job puede ejecutarse? (sí, según tu descripción)
2. ¿Puede acceder a servicios de Google Cloud? (probablemente sí)
3. ¿Solo falla con internet público?

Si solo falla con internet público pero funciona con servicios de Google Cloud, puede ser una política de organización bloqueando egress a internet público.

