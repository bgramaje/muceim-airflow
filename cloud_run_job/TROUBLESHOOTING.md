# Troubleshooting: Signal 9 en Cloud Run Job

## Error: "Container terminated on signal 9"

### ¿Qué significa?

Signal 9 (SIGKILL) significa que el contenedor fue terminado **forzadamente** por el sistema operativo. Esto puede ocurrir por:

1. **Timeout** - El job excedió el tiempo máximo permitido
2. **Out of Memory (OOM)** - El contenedor se quedó sin memoria
3. **Límites de recursos excedidos**

### Cómo diagnosticar

#### 1. Verificar si es un problema de timeout

En los logs, busca cuánto tiempo estuvo corriendo el job antes de terminar:

```bash
gcloud logging read "resource.type=cloud_run_job AND resource.labels.job_name=insert-ducklake" \
  --limit=50 \
  --format="table(timestamp,textPayload)" \
  --project=TU_PROJECT_ID
```

Si el job siempre termina después de X segundos (por ejemplo, 3600s si tienes timeout de 1 hora), entonces es un problema de **timeout**.

#### 2. Verificar si es un problema de memoria (OOM)

Busca en los logs mensajes relacionados con memoria:

```bash
gcloud logging read "resource.type=cloud_run_job AND resource.labels.job_name=insert-ducklake AND (textPayload=~\"memory\" OR textPayload=~\"OOM\" OR textPayload=~\"killed\")" \
  --limit=20 \
  --format="table(timestamp,textPayload)"
```

O revisa los logs en la consola: **Cloud Run > Jobs > insert-ducklake > Logs**

### Soluciones

#### Si es un problema de **TIMEOUT**:

**Aumentar el timeout del Cloud Run Job:**

**Desde la consola:**
1. Ve a **Cloud Run** > **Jobs** > `insert-ducklake`
2. Haz clic en **EDIT**
3. Busca **Timeout** (puede estar en "Container, Networking, Security, Limits & Admin")
4. Aumenta el valor (por ejemplo, de 3600s a 7200s o más)
5. Guarda

**Desde línea de comandos:**
```bash
gcloud run jobs update insert-ducklake \
  --region=europe-southwest1 \
  --timeout=7200s  # 2 horas (máximo: 3600s para servicios, pero Jobs puede tener más)
```

**Nota**: Para Cloud Run Jobs, el timeout máximo es **24 horas (86400s)**.

#### Si es un problema de **MEMORIA (OOM)**:

**Aumentar la memoria del Cloud Run Job:**

**Desde la consola:**
1. Ve a **Cloud Run** > **Jobs** > `insert-ducklake`
2. Haz clic en **EDIT**
3. Busca **Memory** o **Container memory**
4. Aumenta el valor (por ejemplo, de 4Gi a 8Gi)
5. Guarda

**Desde línea de comandos:**
```bash
gcloud run jobs update insert-ducklake \
  --region=europe-southwest1 \
  --memory=8Gi  # Aumentar memoria
```

**También aumentar CPU si es necesario:**
```bash
gcloud run jobs update insert-ducklake \
  --region=europe-southwest1 \
  --cpu=4 \
  --memory=8Gi
```

#### Ver configuración actual:

```bash
gcloud run jobs describe insert-ducklake \
  --region=europe-southwest1 \
  --format="yaml(template.template.timeout,template.template.containers[0].resources)"
```

### Recomendaciones

1. **Para archivos grandes**: Aumenta tanto timeout como memoria
2. **Para procesamiento intensivo**: Aumenta CPU y memoria
3. **Monitorea los logs**: Revisa regularmente para ajustar recursos según necesidad

### Configuración recomendada para archivos grandes

- **CPU**: 4 cores
- **Memory**: 8Gi (o más según el tamaño del archivo)
- **Timeout**: 7200s (2 horas) o más si procesas muchos datos

