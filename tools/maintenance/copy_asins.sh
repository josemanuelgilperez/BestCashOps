export AWS_PROFILE=bestcash

#!/usr/bin/env bash
set -euo pipefail
IFS=$'\n\t'

BUCKET="bestcashproductimages"
DEST_BASE="$HOME/Dropbox/MAURO/MP0873"

ASINS=(
B0FCDBCJLG
B0CFF8QTC1
B0BJV8Q191
B09ZKT3723
B09ZKVLXCS
B0FJ85RVP1
B0D2M75RTH
B0DK889BVF
B0D72S7HL9
B08688PSLV
B0D4F1RH3X
B0DQ8HQS1R
B0CDL1VPYP
B0D72DV6QG
B09PV7HC72
B098MQPF5Q
B08DJ32BNF
B0DWN1WNNH
B0DPKG6BS2
B0DQGVG1KL
B0DFH6FDP6
B0CNCCHTND
B07NT77GT8
B0CPDRLSVT)

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
