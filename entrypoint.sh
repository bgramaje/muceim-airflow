#!/bin/bash
# Script de entrada que carga .env antes de iniciar Airflow
# Este script se ejecuta antes de cualquier comando de Airflow

# Cargar variables de entorno desde .env si existe
# El .env debe estar en la raíz del proyecto (se monta como volumen)
# El .env ya contiene las variables con los nombres correctos (AIRFLOW_CONN_*, AIRFLOW_VAR_*)
if [ -f /usr/local/airflow/.env ]; then
    echo "📋 Cargando variables de entorno desde .env..."
    set -a
    source /usr/local/airflow/.env
    set +a
    echo "✅ Variables de entorno cargadas desde .env"
else
    echo "⚠️  Archivo .env no encontrado en /usr/local/airflow/.env"
    echo "   Usando variables de entorno del sistema o airflow_settings.yaml"
fi

# Ejecutar el comando original (pasado como argumentos)
# Esto permite que el entrypoint original de Astro Runtime se ejecute normalmente
exec "$@"

