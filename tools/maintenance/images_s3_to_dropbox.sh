#!/usr/bin/env bash
set -euo pipefail
IFS=$'\n\t'

usage() {
  cat <<'EOF'
Uso:
  images_s3_to_dropbox.sh --asins-file RUTA --dropbox-base RUTA [opciones]

Opciones:
  --asins-file RUTA       Fichero con un ASIN por linea. Obligatorio.
  --dropbox-base RUTA     Carpeta base de Dropbox. Obligatorio.
  --bucket NOMBRE         Bucket S3. Default: bestcashproductimages
  --date-folder NOMBRE    Subcarpeta opcional dentro de dropbox-base
  --strict                Devuelve exit 1 si hay fallos o ASINs sin imagenes
  --help                  Muestra esta ayuda

Ejemplo:
  tools/maintenance/images_s3_to_dropbox.sh \
    --asins-file /tmp/asins.txt \
    --dropbox-base /BESTCASH/WALLAPOP \
    --date-folder 2026-05-01
EOF
}

require_cmd() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "ERROR: comando requerido no disponible: $1" >&2
    exit 2
  fi
}

ASINS_FILE=""
DROPBOX_BASE=""
BUCKET="bestcashproductimages"
DATE_FOLDER=""
STRICT=0

while [ $# -gt 0 ]; do
  case "$1" in
    --asins-file)
      ASINS_FILE="${2:-}"
      shift 2
      ;;
    --dropbox-base)
      DROPBOX_BASE="${2:-}"
      shift 2
      ;;
    --bucket)
      BUCKET="${2:-}"
      shift 2
      ;;
    --date-folder)
      DATE_FOLDER="${2:-}"
      shift 2
      ;;
    --strict)
      STRICT=1
      shift
      ;;
    --help|-h)
      usage
      exit 0
      ;;
    *)
      echo "ERROR: opcion no reconocida: $1" >&2
      usage >&2
      exit 2
      ;;
  esac
done

if [ -z "$ASINS_FILE" ] || [ -z "$DROPBOX_BASE" ]; then
  echo "ERROR: --asins-file y --dropbox-base son obligatorios" >&2
  usage >&2
  exit 2
fi

if [ ! -f "$ASINS_FILE" ]; then
  echo "ERROR: no existe el fichero de ASINs: $ASINS_FILE" >&2
  exit 2
fi

require_cmd aws
require_cmd dbxcli

DROPBOX_TARGET="$DROPBOX_BASE"
if [ -n "$DATE_FOLDER" ]; then
  DROPBOX_TARGET="${DROPBOX_BASE%/}/${DATE_FOLDER}"
fi

ASINS=()
while IFS= read -r raw || [ -n "$raw" ]; do
  asin="$(printf '%s' "$raw" | tr -d '\r' | xargs)"
  [ -n "$asin" ] && ASINS+=("$asin")
done < "$ASINS_FILE"

if [ "${#ASINS[@]}" -eq 0 ]; then
  echo "ERROR: el fichero no contiene ASINs validos" >&2
  exit 2
fi

FAILED_ASINS=()
MISSING_ASINS=()
UPLOADED_FILES=0

echo ""
echo "S3 -> Dropbox"
echo "----------------------------------------"
echo "Bucket: $BUCKET"
echo "Dropbox destino: $DROPBOX_TARGET"
echo "ASINs a procesar: ${#ASINS[@]}"
echo ""

for asin in "${ASINS[@]}"; do
  echo "ASIN: $asin"

  keys="$(aws s3api list-objects-v2 \
    --bucket "$BUCKET" \
    --prefix "$asin/" \
    --query 'Contents[].Key' \
    --output text 2>/dev/null || true)"

  if [ -z "$keys" ] || [ "$keys" = "None" ]; then
    echo "  WARN: no hay imagenes en S3"
    MISSING_ASINS+=("$asin")
    echo ""
    continue
  fi

  asin_failed=0
  while IFS= read -r key; do
    [ -z "$key" ] && continue
    filename="$(basename "$key")"
    dropbox_path="${DROPBOX_TARGET%/}/${asin}/${filename}"
    tmp_file="$(mktemp "/tmp/${asin}.XXXXXX")"

    echo "  -> $filename"
    if aws s3 cp "s3://${BUCKET}/${key}" "$tmp_file" >/dev/null 2>&1 && dbxcli put "$tmp_file" "$dropbox_path" >/dev/null; then
      UPLOADED_FILES=$((UPLOADED_FILES + 1))
      echo "     OK"
    else
      asin_failed=1
      echo "     ERROR"
    fi
    rm -f "$tmp_file"
  done <<< "$(printf '%s\n' "$keys" | tr '\t' '\n')"

  if [ "$asin_failed" -eq 1 ]; then
    FAILED_ASINS+=("$asin")
  fi

  echo ""
done

FAILED_UNIQUE=()
if [ "${#FAILED_ASINS[@]}" -gt 0 ]; then
  while IFS= read -r asin || [ -n "$asin" ]; do
    [ -n "$asin" ] && FAILED_UNIQUE+=("$asin")
  done < <(printf '%s\n' "${FAILED_ASINS[@]}" | sort -u)
fi

MISSING_UNIQUE=()
if [ "${#MISSING_ASINS[@]}" -gt 0 ]; then
  while IFS= read -r asin || [ -n "$asin" ]; do
    [ -n "$asin" ] && MISSING_UNIQUE+=("$asin")
  done < <(printf '%s\n' "${MISSING_ASINS[@]}" | sort -u)
fi

echo "----------------------------------------"
echo "Resumen"
echo "Ficheros subidos: $UPLOADED_FILES"
echo "ASINs con errores de subida: ${#FAILED_UNIQUE[@]}"
echo "ASINs sin imagenes: ${#MISSING_UNIQUE[@]}"

if [ "${#FAILED_UNIQUE[@]}" -gt 0 ]; then
  echo ""
  echo "Errores de subida:"
  printf ' - %s\n' "${FAILED_UNIQUE[@]}"
fi

if [ "${#MISSING_UNIQUE[@]}" -gt 0 ]; then
  echo ""
  echo "ASINs sin imagenes:"
  printf ' - %s\n' "${MISSING_UNIQUE[@]}"
fi

if [ "$STRICT" -eq 1 ] && { [ "${#FAILED_UNIQUE[@]}" -gt 0 ] || [ "${#MISSING_UNIQUE[@]}" -gt 0 ]; }; then
  exit 1
fi
