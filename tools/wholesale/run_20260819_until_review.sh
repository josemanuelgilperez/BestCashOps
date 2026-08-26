#!/usr/bin/env bash
set -euo pipefail

cd /root/BestCashOps

mkdir -p logs tools/data

MAIN_LOG="logs/publish_20260819_until_review.log"
exec > >(tee -a "$MAIN_LOG") 2>&1

echo "START $(date -Is)"
echo "Mode: until review, no FTP upload"

PY=".venv/bin/python"
META_CSV="tools/data/new_pallet_categories_20260819.csv"
CODES="MP1542,MP1540,MP1539,MP1538,MP1537,MP1533,MP1531,MP1530,MP1528,MP1521,MP1517,MP1515,MP1514,MP1513,MP1512,MP1511,MP1510,MP1509,MP1507,MP1506,MP1505,MP1504,MP1503,MP1500,MP1499,MP1498,MP1495,MP1494,MP1493,MP1491,MP1486,MP1482,MP1481,MP1480,MP1479,MP1478,MP1475,MP1474,MP1473,MP1470,MP1469,MP1468,MP1467,MP1466,MP1465,MP1464,MP1462,MP1461,MP1459,MP1458,MP1457,MP1456,MP1455,MP1453,MP1452,MP1451,MP1450,MP1449,MP1447,MP1445,MP1444,MP1442,MP1441,MP1440,MP1439,MP1438,MP1437,MP1436,MP1435,MP1434,MP1433,MP1431,MP1430,MP1429,MP1428,MP1426,MP1423,MP1419,MP1418,MP1417,MP1416,MP1413,MP1412,MP1410,MP1409,MP1407,MP1405,MP1401,MP1390,MP1384,MP1382,MP1381,MP1380,MP1342,MP1332"

run_step() {
  echo
  echo "== $1 =="
  shift
  "$@"
}

run_step "Ingest XLSX into boxes and box_items" "$PY" wholesale/pipeline/ingest.py
run_step "Apply visible names and categories" "$PY" tools/wholesale/apply_box_metadata.py --input "$META_CSV" --fail-on-missing
run_step "Bootstrap scraped products from XLSX costs/descriptions" "$PY" tools/wholesale/bootstrap_scraped_from_xlsx.py --boxes "$CODES"
run_step "Update PVP from available scraped data" "$PY" wholesale/pipeline/enrich.py --only-pvp-update
run_step "Initial quality report" "$PY" tools/wholesale/quality_report.py --new-pallets --output-prefix tools/data/quality_20260819_after_bootstrap
run_step "Apply already-existing S3 images" "$PY" tools/wholesale/apply_existing_s3_images.py --from-csv tools/data/quality_20260819_after_bootstrap_missing_by_asin.tsv
run_step "Quality report after S3 reuse" "$PY" tools/wholesale/quality_report.py --new-pallets --output-prefix tools/data/quality_20260819_after_s3

MISS="tools/data/images_missing_20260819.txt"
awk -F '\t' 'NR > 1 && $8 == "1" {print $5}' tools/data/quality_20260819_after_s3_missing_by_asin.tsv | sort -u > "$MISS"
MISSING_COUNT="$(wc -l < "$MISS" | tr -d ' ')"
echo "missing_images_after_s3=$MISSING_COUNT"

if [[ "$MISSING_COUNT" != "0" ]]; then
  rm -f tools/data/images_missing_20260819_part_*
  split -n l/4 -d -a 2 "$MISS" tools/data/images_missing_20260819_part_

  pids=()
  for part in tools/data/images_missing_20260819_part_*; do
    [[ -s "$part" ]] || continue
    worker_id="${part##*_part_}"
    echo "Launching image worker $worker_id with $(wc -l < "$part" | tr -d ' ') ASINs"
    (
      "$PY" tools/wholesale/rescrape_missing_price_image.py \
        --from-txt "$part" \
        --only-if-missing \
        --domains es,de,fr,it,com \
        --crawl-timeout 10 \
        --crawl-retries 1 \
        --sleep 0.1 \
        --max-images 4
    ) > "logs/rescrape_images_20260819_${worker_id}.log" 2>&1 &
    pids+=("$!")
  done

  for pid in "${pids[@]}"; do
    wait "$pid"
  done
fi

run_step "Final quality before finance/build" "$PY" tools/wholesale/quality_report.py --new-pallets --output-prefix tools/data/quality_20260819_ready
run_step "Finance for this batch" "$PY" wholesale/pipeline/finance.py --new-pallets
run_step "Build local HTML" "$PY" wholesale/web/build_html.py
run_step "Build local categories" "$PY" wholesale/web/categories.py
run_step "Review quality report" "$PY" tools/wholesale/quality_report.py --new-pallets --output-prefix tools/data/quality_20260819_review

touch tools/data/REVIEW_READY_20260819
echo "REVIEW_READY $(date -Is)"
echo "FTP_UPLOAD_SKIPPED"
echo "END $(date -Is)"
