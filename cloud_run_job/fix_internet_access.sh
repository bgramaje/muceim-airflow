#!/bin/bash
#
# Script para configurar el acceso a internet en Cloud Run Job
#
# Uso:
#   ./fix_internet_access.sh [OPTIONS]
#
# Opciones:
#   --job-name=NAME      Nombre del Cloud Run Job (default: insert-ducklake)
#   --region=REGION      Región del job (default: europe-southwest1)
#   --vpc-connector=NAME Nombre del VPC connector (opcional)
#   --egress=MODE        Modo de egress: private-ranges-only (default) o all-traffic
#   --dry-run            Solo mostrar qué se haría, sin ejecutar
#

set -e

# Valores por defecto
JOB_NAME="insert-ducklake"
REGION="europe-southwest1"
VPC_CONNECTOR=""
EGRESS_MODE="private-ranges-only"
DRY_RUN=false

# Parsear argumentos
REMOVE_VPC=false
for arg in "$@"; do
  case $arg in
    --job-name=*)
      JOB_NAME="${arg#*=}"
      shift
      ;;
    --region=*)
      REGION="${arg#*=}"
      shift
      ;;
    --vpc-connector=*)
      VPC_CONNECTOR="${arg#*=}"
      shift
      ;;
    --egress=*)
      EGRESS_MODE="${arg#*=}"
      shift
      ;;
    --remove-vpc)
      REMOVE_VPC=true
      shift
      ;;
    --dry-run)
      DRY_RUN=true
      shift
      ;;
    --help)
      echo "Uso: $0 [OPTIONS]"
      echo ""
      echo "Opciones:"
      echo "  --job-name=NAME      Nombre del Cloud Run Job (default: insert-ducklake)"
      echo "  --region=REGION      Región del job (default: europe-southwest1)"
      echo "  --remove-vpc         Remover VPC connector para acceso directo a internet (recomendado si no usas recursos privados)"
      echo "  --vpc-connector=NAME Nombre del VPC connector (opcional, solo si necesitas VPC)"
      echo "  --egress=MODE        Modo de egress: private-ranges-only (default) o all-traffic"
      echo "  --dry-run            Solo mostrar qué se haría, sin ejecutar"
      echo ""
      echo "Ejemplos:"
      echo "  # Remover VPC connector (acceso directo a internet)"
      echo "  $0 --remove-vpc"
      echo ""
      echo "  # Configurar VPC connector con acceso a internet"
      echo "  $0 --vpc-connector=my-connector --egress=private-ranges-only"
      exit 0
      ;;
    *)
      echo "❌ Opción desconocida: $arg"
      echo "Usa --help para ver la ayuda"
      exit 1
      ;;
  esac
done

echo "🔍 Verificando configuración actual del Cloud Run Job..."
echo "   Job: $JOB_NAME"
echo "   Region: $REGION"
echo ""

# Verificar configuración actual
CURRENT_CONFIG=$(gcloud run jobs describe "$JOB_NAME" \
  --region="$REGION" \
  --format="get(template.template.vpcAccess.egress,template.template.vpcAccess.connector)" 2>/dev/null || echo "")

if [ -z "$CURRENT_CONFIG" ]; then
  echo "⚠️  No se pudo obtener la configuración del job. Verifica que existe y tienes permisos."
  exit 1
fi

CURRENT_EGRESS=$(echo "$CURRENT_CONFIG" | head -n1)
CURRENT_CONNECTOR=$(echo "$CURRENT_CONFIG" | tail -n1)

echo "📊 Configuración actual:"
echo "   VPC Connector: ${CURRENT_CONNECTOR:-(no configurado)}"
echo "   VPC Egress: ${CURRENT_EGRESS:-EGRESS_SETTING_UNSPECIFIED (acceso directo a internet)}"
echo ""

# Determinar si necesita cambios
NEEDS_UPDATE=false

if [ "$REMOVE_VPC" = true ]; then
  if [ -n "$CURRENT_CONNECTOR" ]; then
    NEEDS_UPDATE=true
    echo "📝 Se removerá el VPC Connector para permitir acceso directo a internet"
    UPDATE_CMD="gcloud run jobs update $JOB_NAME --region=$REGION --clear-vpc-connector"
  else
    echo "✅ No hay VPC connector configurado. El job ya tiene acceso directo a internet."
    exit 0
  fi
else
  # Lógica para configurar/actualizar VPC connector
  if [ -n "$VPC_CONNECTOR" ]; then
    if [ "$CURRENT_CONNECTOR" != "$VPC_CONNECTOR" ]; then
      NEEDS_UPDATE=true
      echo "📝 Se actualizará el VPC Connector: $CURRENT_CONNECTOR -> $VPC_CONNECTOR"
    fi
  elif [ -z "$CURRENT_CONNECTOR" ] && [ "$EGRESS_MODE" != "EGRESS_SETTING_UNSPECIFIED" ] && [ -n "$EGRESS_MODE" ]; then
    echo "⚠️  Se requiere --vpc-connector cuando se usa --egress diferente de EGRESS_SETTING_UNSPECIFIED"
    exit 1
  fi

  if [ -n "$EGRESS_MODE" ] && [ "$CURRENT_EGRESS" != "$EGRESS_MODE" ]; then
    NEEDS_UPDATE=true
    echo "📝 Se actualizará el VPC Egress: $CURRENT_EGRESS -> $EGRESS_MODE"
  fi

  if [ "$NEEDS_UPDATE" = false ]; then
    echo "✅ La configuración ya está correcta. No se necesitan cambios."
    exit 0
  fi

  # Construir comando para configurar VPC
  UPDATE_CMD="gcloud run jobs update $JOB_NAME --region=$REGION"

  if [ -n "$VPC_CONNECTOR" ]; then
    UPDATE_CMD="$UPDATE_CMD --vpc-connector=$VPC_CONNECTOR"
  fi

  if [ "$EGRESS_MODE" = "private-ranges-only" ]; then
    UPDATE_CMD="$UPDATE_CMD --vpc-egress=private-ranges-only"
  elif [ "$EGRESS_MODE" = "all-traffic" ]; then
    UPDATE_CMD="$UPDATE_CMD --vpc-egress=all-traffic"
  fi
fi

echo ""
echo "🚀 Comando a ejecutar:"
echo "   $UPDATE_CMD"
echo ""

if [ "$DRY_RUN" = true ]; then
  echo "🔍 DRY RUN: No se ejecutará el comando"
  exit 0
fi

read -p "¿Continuar con la actualización? (y/N): " -n 1 -r
echo
if [[ ! $REPLY =~ ^[Yy]$ ]]; then
  echo "❌ Actualización cancelada"
  exit 0
fi

echo ""
echo "⏳ Ejecutando actualización..."
eval "$UPDATE_CMD"

echo ""
echo "✅ Actualización completada!"
echo ""
echo "💡 Para verificar la nueva configuración:"
echo "   gcloud run jobs describe $JOB_NAME --region=$REGION --format=\"get(template.template.vpcAccess)\""

