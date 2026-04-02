#!/usr/bin/env bash
set -euo pipefail

# Uso:
#   bash tools/wholesale/deploy_boxes_inputs.sh \
#     --local-names "/ruta/local/names.csv" \
#     --local-xlsx-dir "/ruta/local/new_box_files"
#
# Opcionales:
#   --host 212.227.90.202
#   --user root

HOST="212.227.90.202"
USER_NAME="root"
REMOTE_BASE="/opt/bestcash/apps/BestCashBoxes/data"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
LOCAL_NAMES="${PROJECT_ROOT}/tools/data/names.csv"
LOCAL_XLSX_DIR="${PROJECT_ROOT}/tools/data/new_box_files"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --host)
      HOST="$2"
      shift 2
      ;;
    --user)
      USER_NAME="$2"
      shift 2
      ;;
    --local-names)
      LOCAL_NAMES="$2"
      shift 2
      ;;
    --local-xlsx-dir)
      LOCAL_XLSX_DIR="$2"
      shift 2
      ;;
    *)
      echo "Parametro no reconocido: $1"
      exit 1
      ;;
  esac
done

if [[ ! -f "$LOCAL_NAMES" ]]; then
  echo "No existe names.csv en: $LOCAL_NAMES"
  exit 1
fi

if [[ ! -d "$LOCAL_XLSX_DIR" ]]; then
  echo "No existe la carpeta de excels: $LOCAL_XLSX_DIR"
  echo "Crea esta carpeta y pon ahi los .xlsx:"
  echo "  $LOCAL_XLSX_DIR"
  exit 1
fi

if ! ls "$LOCAL_XLSX_DIR"/*.xlsx >/dev/null 2>&1; then
  echo "No hay archivos .xlsx en: $LOCAL_XLSX_DIR"
  exit 1
fi

echo "Creando carpetas remotas..."
ssh "${USER_NAME}@${HOST}" "mkdir -p '${REMOTE_BASE}/new_box_files'"

echo "Subiendo names.csv..."
scp "$LOCAL_NAMES" "${USER_NAME}@${HOST}:${REMOTE_BASE}/names.csv"

echo "Subiendo archivos .xlsx..."
scp "$LOCAL_XLSX_DIR"/*.xlsx "${USER_NAME}@${HOST}:${REMOTE_BASE}/new_box_files/"

echo "Listado remoto final:"
ssh "${USER_NAME}@${HOST}" "ls -la '${REMOTE_BASE}' && ls -la '${REMOTE_BASE}/new_box_files' | head -n 40"

echo "OK: subida completada."
echo "Origen local usado:"
echo "  names.csv: $LOCAL_NAMES"
echo "  xlsx dir : $LOCAL_XLSX_DIR"
