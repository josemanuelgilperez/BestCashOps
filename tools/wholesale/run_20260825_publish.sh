#!/usr/bin/env bash
set -euo pipefail

cd /root/BestCashOps

mkdir -p logs tools/data

MAIN_LOG="logs/publish_20260825.log"
exec > >(tee -a "$MAIN_LOG") 2>&1

echo "START $(date -Is)"
echo "Mode: process 20260825 batch on server, then FTP"

PY=".venv/bin/python"
META_CSV="tools/data/new_pallet_categories_20260825.csv"
CODES="MP1604,MP1601,MP1596,MP1594,MP1592,MP1589,MP1588,MP1584,MP1581,MP1579,MP1574,MP1573,MP1572,MP1571,MP1568,MP1567,MP1566,MP1565,MP1564,MP1563,MP1562,MP1560,MP1558,MP1554,MP1553,MP1552,MP1548,MP1546,MP1545,MP1543,MP1541,MP1536,MP1535,MP1534,MP1532,MP1529,MP1522,MP1516,MP1502,MP1489,MP1488,MP1460,MP1454,MP1443,MP1422,MP1400"

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
run_step "Initial quality report" "$PY" tools/wholesale/quality_report.py --new-pallets --output-prefix tools/data/quality_20260825_after_bootstrap
run_step "Apply already-existing S3 images" "$PY" tools/wholesale/apply_existing_s3_images.py --from-csv tools/data/quality_20260825_after_bootstrap_missing_by_asin.tsv
run_step "Quality report after S3 reuse" "$PY" tools/wholesale/quality_report.py --new-pallets --output-prefix tools/data/quality_20260825_after_s3

MISS="tools/data/asins_20260825_final_pass.txt"
awk -F '\t' 'NR > 1 && ($7 == "1" || $8 == "1") {print $5}' tools/data/quality_20260825_after_s3_missing_by_asin.tsv | sort -u > "$MISS"
MISSING_COUNT="$(wc -l < "$MISS" | tr -d ' ')"
echo "remaining_asins_for_final_pass=$MISSING_COUNT"

if [[ "$MISSING_COUNT" != "0" ]]; then
  run_step "Final rescrape in parallel" bash -c '
    rm -f tools/data/asins_20260825_final_part_*
    split -n l/6 -d -a 2 tools/data/asins_20260825_final_pass.txt tools/data/asins_20260825_final_part_

    pids=()
    for part in tools/data/asins_20260825_final_part_*; do
      [[ -s "$part" ]] || continue
      worker_id="${part##*_part_}"
      echo "Launching worker $worker_id with $(wc -l < "$part" | tr -d " ") ASINs"
      (
        .venv/bin/python tools/wholesale/rescrape_missing_price_image.py \
          --from-txt "$part" \
          --only-if-missing \
          --domains es,de,fr,it,com,co.uk,nl,pl \
          --crawl-timeout 12 \
          --crawl-retries 1 \
          --sleep 0.1 \
          --max-images 8
      ) > "logs/rescrape_images_20260825_${worker_id}.log" 2>&1 &
      pids+=("$!")
    done

    for pid in "${pids[@]}"; do
      wait "$pid"
    done
  '
fi

run_step "Quality after final rescrape" "$PY" tools/wholesale/quality_report.py --new-pallets --output-prefix tools/data/quality_20260825_after_rescrape

run_step "Suggest prices for any remaining no-price ASINs" "$PY" tools/wholesale/suggest_prices_from_similar_products.py \
  --missing-tsv tools/data/quality_20260825_after_rescrape_missing_by_asin.tsv \
  --output tools/data/quality_20260825_price_suggestions.tsv \
  --top 5 \
  --min-score 0.24

run_step "Select fallback prices if needed" "$PY" - <<'PY'
import csv
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from pathlib import Path

missing_path = Path("tools/data/quality_20260825_after_rescrape_missing_by_asin.tsv")
suggestions_path = Path("tools/data/quality_20260825_price_suggestions.tsv")
selected_path = Path("tools/data/quality_20260825_price_selected.tsv")

def money(value):
    try:
        dec = Decimal(str(value or "").replace(",", "."))
    except (InvalidOperation, ValueError):
        return None
    return dec.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP) if dec > 0 else None

def fallback_price(row):
    title = (row.get("titulo") or "").lower()
    if "sábana" in title or "sabana" in title or "colchon" in title:
        return Decimal("9.99")
    if "manillar" in title or "cochecito" in title:
        return Decimal("12.99")
    if "tubo interior" in title or "inner tube" in title:
        return Decimal("9.99")
    pvp = money(row.get("pvp_ud"))
    if pvp:
        return max((pvp * Decimal("10")).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP), Decimal("4.99"))
    return Decimal("4.99")

suggested = {}
if suggestions_path.exists():
    for row in csv.DictReader(suggestions_path.open(encoding="utf-8"), delimiter="\t"):
        asin = (row.get("asin") or "").strip().upper()
        price = money(row.get("precio_sugerido"))
        if asin and price and asin not in suggested:
            suggested[asin] = price

selected = {}
if missing_path.exists():
    for row in csv.DictReader(missing_path.open(encoding="utf-8"), delimiter="\t"):
        if row.get("falta_precio") != "1":
            continue
        asin = (row.get("asin") or "").strip().upper()
        if asin and asin not in selected:
            selected[asin] = suggested.get(asin) or fallback_price(row)

with selected_path.open("w", newline="", encoding="utf-8") as file_obj:
    writer = csv.DictWriter(file_obj, fieldnames=["asin", "precio_seleccionado"], delimiter="\t")
    writer.writeheader()
    for asin, price in sorted(selected.items()):
        writer.writerow({"asin": asin, "precio_seleccionado": f"{price:.2f}"})

print(f"selected_prices={len(selected)}")
print(f"target={selected_path}")
for asin, price in sorted(selected.items()):
    source = "similar" if asin in suggested else "fallback"
    print(f"{asin}\t{price:.2f}\t{source}")
PY

PRICE_SELECTED="tools/data/quality_20260825_price_selected.tsv"
if [[ "$(($(wc -l < "$PRICE_SELECTED") - 1))" -gt 0 ]]; then
  run_step "Apply selected approximate prices" "$PY" tools/wholesale/apply_similar_prices.py --input "$PRICE_SELECTED" --cost-rate 0.07
else
  echo "No remaining prices to apply"
fi

run_step "Finance for this batch" "$PY" wholesale/pipeline/finance.py --new-pallets
run_step "Build local HTML" "$PY" wholesale/web/build_html.py
run_step "Build local categories" "$PY" wholesale/web/categories.py
run_step "Mark this batch as new in listing pages" "$PY" - <<'PY'
from pathlib import Path

codes = "MP1604,MP1601,MP1596,MP1594,MP1592,MP1589,MP1588,MP1584,MP1581,MP1579,MP1574,MP1573,MP1572,MP1571,MP1568,MP1567,MP1566,MP1565,MP1564,MP1563,MP1562,MP1560,MP1558,MP1554,MP1553,MP1552,MP1548,MP1546,MP1545,MP1543,MP1541,MP1536,MP1535,MP1534,MP1532,MP1529,MP1522,MP1516,MP1502,MP1489,MP1488,MP1460,MP1454,MP1443,MP1422,MP1400".split(",")
Path("wholesale/data/new_published_pallets.txt").write_text("\n".join(codes) + "\n", encoding="utf-8")
print(f"new_published_pallets={len(codes)}")
PY
run_step "Apply new lot badges and filters" "$PY" tools/wholesale/mark_new_lots.py \
  --site wholesale/web/output \
  --new-codes-file wholesale/data/new_published_pallets.txt
run_step "Final quality report" "$PY" tools/wholesale/quality_report.py --new-pallets --output-prefix tools/data/quality_20260825_final
run_step "FTP upload" "$PY" wholesale/scripts/upload_ftp.py

touch tools/data/PUBLISHED_FTP_20260825
echo "PUBLISHED_FTP $(date -Is)"
echo "END $(date -Is)"
