export AWS_PROFILE=bestcash

#!/usr/bin/env bash
set -euo pipefail
IFS=$'\n\t'

BUCKET="bestcashproductimages"
DEST_BASE="$HOME/Dropbox/BESTCASH/30_04_2026"

ASINS=(
B0D17478GB
B0D8KG8MYG
B0DBLGTR7V
B0FD387HTF
B0FRM7JVK6
B0FS7ZPGCV
B0FYLJ9V2R
B07H76WQDW
B07Q84FF7H
)

FAILED_ASIN=()

echo ""
echo "🚀 Descarga directa desde S3 por ASIN"
echo "----------------------------------------"
echo ""

for asin in "${ASINS[@]}"; do
    echo "📌 $asin"

    src="s3://${BUCKET}/${asin}/"
    dest="${DEST_BASE}/${asin}/"

    mkdir -p "$dest"

    # Intento de sincronización
    if aws s3 sync "$src" "$dest" --only-show-errors; then
        echo "   ✓ Descargado correctamente"
    else
        echo "   ❌ Error descargando $asin"
        FAILED_ASIN+=("$asin")
    fi

    echo ""
done

echo ""
echo "----------------------------------------"
echo "✅ Proceso terminado"
echo ""

if [ ${#FAILED_ASIN[@]} -eq 0 ]; then
    echo "🎉 Todos los ASIN fueron descargados correctamente"
else
    echo "⚠️ ASIN NO descargados:"
    for asin in "${FAILED_ASIN[@]}"; do
        echo "   - $asin"
    done
fi

echo ""
