export AWS_PROFILE=bestcash

#!/usr/bin/env bash
set -euo pipefail
IFS=$'\n\t'

BUCKET="bestcashproductimages"
DEST_BASE="$HOME/Dropbox/RODRIGO/MP1162"

ASINS=(
B0FR55D8KJ
B0F8V9FWPY
B0D7ZVJHLD
B0FP5CKCTX
B0F1FSRH2H
B0F4QSD53R
B0FNK8R129
B0DZHKRJ3X
B0FLXTHZDX
B0CVXC3L3Y
B0D2GPKDPK
B00FGKH6CO
B0F93SRQWS
B0GD7KWPX1
B0D4CT5H3D
B0FMDYGGSP
B0DQTDWV5T
B0FQ33MGYJ
B0FP5F7Y4Y
B07BDP1VQW
B0DRS9BCPW
B0G2RDWXX8
B0FN7JNS18
B0DLK5G3X1
B0B9MWGSVQ
B0BF8ZH467
B0FN7J5G94
B0FH24XBRQ
B0BZVX58WF
B0FG8458Y8
B0GDZZSSCX
B0GLYVYNMX
B0F8MDZL8K
B0FVT1WZLQ
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
