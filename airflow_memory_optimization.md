# Optimización de Memoria - Airflow OOM (SIGKILL -9)

## 📊 Problema Identificado
- **Error**: `CRITICAL - Process terminated by signal. SIGKILL -9`
- **Causa**: Pool con 15 slots provocando 15 tareas paralelas, cada una cargando CSVs/JSONs completos en memoria
- **Síntoma**: DuckDB + Pandas + GeoPandas sin límites de memoria

## ✅ Soluciones Implementadas

### 1. ✓ Reducción de Paralelismo (CRÍTICO)
```yaml
# airflow_settings.yaml
od_pool: 15 slots → 3 slots
```
**Impacto**: Reduce de 15 tareas paralelas a solo 3 simultáneas
- Menos presión sobre memoria
- Las tareas toman más tiempo pero no fallan por OOM

### 2. ✓ Límites de Memoria en DuckDB (utils.py)
```sql
SET max_memory='2GB';              -- Máximo 2GB por conexión
SET threads=2;                      -- Reduce paralelismo interno (era ilimitado)
SET memory_limit='2GB';             -- Límite estricto
SET max_temp_directory_size='8GB';  -- Temp reducido de 40GB a 8GB
```

### 3. 📋 Pasos Adicionales Recomendados

#### A. Configurar límites en Docker/Kubernetes (si aplica)
```dockerfile
# En tu Dockerfile o docker-compose
environment:
  - AIRFLOW__CORE__MAX_TASK_INSTANCES_PER_DAGRUN=3
  - AIRFLOW__CORE__DAG_FILE_PROCESSOR_TIMEOUT=600
  - AIRFLOW__CORE__KILLED_TASK_CLEANUP_TIME=60
```

#### B. Aumentar timeout de tareas
En `dags/main.py` o en cada DAG:
```python
with DAG(..., execution_timeout=timedelta(hours=2)):
```

#### C. Monitorear memoria en ejecución
```bash
# Terminal 1: Ver estado de tareas
docker stats airflow-worker

# Terminal 2: Ver logs
tail -f logs/dag_id/task_id/attempt_*.log | grep -i "memory\|killed"
```

## 🔍 Alternativas si el Problema Persiste

### Opción A: Chunked Processing (Streaming)
Modificar `merge_from_csv()` para procesar datos en bloques de 10K filas:
```python
def merge_from_csv_chunked(table_name, url, chunk_size=10000):
    """Lee CSV en chunks en lugar de cargar todo a la vez."""
    for chunk in pd.read_csv(url, chunksize=chunk_size):
        con.execute(f"INSERT INTO {table_name} SELECT * FROM chunk")
```

### Opción B: Procesar URLs secuencialmente
Cambiar de `dynamic_task_map()` a loop secuencial:
```python
# En lugar de paralelo por URL
for url in urls:
    BRONZE_mitma_od_insert(url, zone_type)  # Una a una
```

### Opción C: Usar Spark en lugar de Pandas (si escalas)
Para volúmenes muy grandes, Spark maneja OOM mejor.

## 🧪 Cómo Validar los Cambios

1. **Reducir dataset para testing**:
   ```python
   # En dags/main.py cambiar temporalmente:
   start_date = "2025-12-01"
   end_date = "2025-12-02"  # Solo 1 día en lugar de rango completo
   ```

2. **Ejecutar DAG con pruebas**:
   ```bash
   astro dev restart
   # Ve a http://localhost:8080 y dispara manualmente
   ```

3. **Monitorear en tiempo real**:
   ```bash
   watch -n 1 'docker stats airflow-worker --no-stream | tail -1'
   ```

## 📈 Métricas Esperadas

| Métrica | Antes | Después |
|---------|-------|---------|
| Tareas Paralelas | 15 | 3 |
| Memoria por Tarea | ~800MB | ~300-500MB |
| Tiempo Total DAG | ~5 min | ~15-20 min |
| OOM Errors | Frecuente | Raramente |

## 🚀 Próximos Pasos

Si persiste el error después de estos cambios:

1. Revisar logs detallados:
   ```bash
   grep -i "memory\|killed\|oom" logs/**/*.log
   ```

2. Aumentar recursos del contenedor Docker:
   ```bash
   # En docker-compose o config
   memory: 4GB  # Aumentar si es posible
   ```

3. Considerar split de DAG:
   - Grupo 1: MITMA OD (distritos)
   - Grupo 2: MITMA OD (municipios)
   - Grupo 3: MITMA OD (gau)
   - (Ejecutan secuencialmente, no simultáneamente)

---

**Última actualización**: 2025-12-16
**Estado**: ✅ Cambios implementados - Pendiente validación en producción
