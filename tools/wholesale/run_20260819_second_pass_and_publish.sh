#!/usr/bin/env bash
set -euo pipefail

cd /root/BestCashOps

mkdir -p logs tools/data

MAIN_LOG="logs/publish_20260819_second_pass_and_ftp.log"
exec > >(tee -a "$MAIN_LOG") 2>&1

echo "START $(date -Is)"
echo "Mode: final second pass, then FTP"

PY=".venv/bin/python"
SOURCE_REPORT="tools/data/quality_20260819_review_missing_by_asin.tsv"
ASIN_LIST="tools/data/asins_20260819_second_pass.txt"
AFTER_RESCRAPE="tools/data/quality_20260819_second_after_rescrape"
PRICE_SUGGESTIONS="tools/data/quality_20260819_second_price_suggestions.tsv"
PRICE_SELECTED="tools/data/quality_20260819_second_price_selected.tsv"
FINAL_REPORT="tools/data/quality_20260819_final"

run_step() {
  echo
  echo "== $1 =="
  shift
  "$@"
}

run_step "Build ASIN list from review report" "$PY" - <<'PY'
import csv
from pathlib import Path

source = Path("tools/data/quality_20260819_review_missing_by_asin.tsv")
target = Path("tools/data/asins_20260819_second_pass.txt")
rows = list(csv.DictReader(source.open(encoding="utf-8"), delimiter="\t"))
asins = sorted({(row.get("asin") or "").strip().upper() for row in rows if row.get("asin")})
target.write_text("\n".join(asins) + "\n", encoding="utf-8")
print(f"asins={len(asins)}")
print(f"target={target}")
PY

run_step "Final rescrape for remaining ASINs in parallel" bash -c '
  rm -f tools/data/asins_20260819_second_pass_part_*
  split -n l/4 -d -a 2 tools/data/asins_20260819_second_pass.txt tools/data/asins_20260819_second_pass_part_

  pids=()
  for part in tools/data/asins_20260819_second_pass_part_*; do
    [[ -s "$part" ]] || continue
    worker_id="${part##*_part_}"
    echo "Launching final worker $worker_id with $(wc -l < "$part" | tr -d " ") ASINs"
    (
      .venv/bin/python tools/wholesale/rescrape_missing_price_image.py \
        --from-txt "$part" \
        --only-if-missing \
        --domains es,de,fr,it,com,co.uk,nl,pl \
        --crawl-timeout 12 \
        --crawl-retries 1 \
        --sleep 0.1 \
        --max-images 8
    ) > "logs/rescrape_images_20260819_final_${worker_id}.log" 2>&1 &
    pids+=("$!")
  done

  for pid in "${pids[@]}"; do
    wait "$pid"
  done
'

run_step "Quality after final rescrape" "$PY" tools/wholesale/quality_report.py \
  --new-pallets \
  --output-prefix "$AFTER_RESCRAPE"

run_step "Suggest prices for any remaining no-price ASINs" "$PY" tools/wholesale/suggest_prices_from_similar_products.py \
  --missing-tsv "${AFTER_RESCRAPE}_missing_by_asin.tsv" \
  --output "$PRICE_SUGGESTIONS" \
  --top 5 \
  --min-score 0.24

run_step "Select fallback prices if needed" "$PY" - <<'PY'
import csv
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from pathlib import Path

missing_path = Path("tools/data/quality_20260819_second_after_rescrape_missing_by_asin.tsv")
suggestions_path = Path("tools/data/quality_20260819_second_price_suggestions.tsv")
selected_path = Path("tools/data/quality_20260819_second_price_selected.tsv")

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

if [[ "$(($(wc -l < "$PRICE_SELECTED") - 1))" -gt 0 ]]; then
  run_step "Apply selected approximate prices" "$PY" tools/wholesale/apply_similar_prices.py \
    --input "$PRICE_SELECTED" \
    --cost-rate 0.07
else
  echo "No remaining prices to apply"
fi

run_step "Final finance" "$PY" wholesale/pipeline/finance.py --new-pallets
run_step "Final HTML build" "$PY" wholesale/web/build_html.py
run_step "Final category build" "$PY" wholesale/web/categories.py
run_step "Final quality report" "$PY" tools/wholesale/quality_report.py \
  --new-pallets \
  --output-prefix "$FINAL_REPORT"

run_step "FTP upload" "$PY" wholesale/scripts/upload_ftp.py

touch tools/data/PUBLISHED_FTP_20260819
echo "PUBLISHED_FTP $(date -Is)"
echo "END $(date -Is)"
