#!/usr/bin/env python3
import argparse
import os
import re
import sys
import unicodedata
from datetime import datetime
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from pathlib import Path

import pandas as pd


REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)


NO_IMAGE_URL = "https://bestcashproductimages.s3.amazonaws.com/image_not_found.jpg"


def parse_args():
    parser = argparse.ArgumentParser(
        description="Crea fichas base en amazon_scraped_products desde Excel procesados."
    )
    parser.add_argument(
        "--processed-dir",
        default="wholesale/data/processed",
        help="Carpeta con Pallet_MPxxxx_Items.xlsx ya ingeridos.",
    )
    parser.add_argument(
        "--boxes",
        action="append",
        help="Codigos MP separados por coma. Si se omite, usa todos los Excel del directorio.",
    )
    return parser.parse_args()


def money(value):
    if value is None:
        return None
    try:
        dec = Decimal(str(value).replace(",", "."))
    except (InvalidOperation, ValueError):
        return None
    if dec <= 0:
        return None
    return dec.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)


def unit_price(total_cost, units):
    total = money(total_cost)
    try:
        qty = Decimal(str(units).replace(",", "."))
    except (InvalidOperation, ValueError):
        return None
    if total is None or qty <= 0:
        return None
    return (total / qty).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)


def handle_for(title, asin):
    title = unicodedata.normalize("NFKD", title or asin)
    title = title.encode("ascii", "ignore").decode("ascii")
    title = re.sub(r"[^a-zA-Z0-9\s]", "", title).lower()
    stem = "-".join(title.split())[:120].strip("-")
    return f"{stem}-{asin}" if stem else asin.lower()


def wanted_codes(raw_boxes):
    codes = set()
    for raw in raw_boxes or []:
        for part in raw.split(","):
            code = part.strip().upper()
            if re.fullmatch(r"MP\d+", code):
                codes.add(code)
    return codes


def code_from_filename(path):
    match = re.search(r"(M[PL]\d+)", path.name, flags=re.IGNORECASE)
    return match.group(1).upper() if match else None


def row_records(processed_dir, codes):
    seen = {}
    for path in sorted(Path(processed_dir).glob("*.xlsx")):
        code = code_from_filename(path)
        if not code or (codes and code not in codes):
            continue
        df = pd.read_excel(path)
        for _, row in df.iterrows():
            asin = str(row.get("Asin") or "").strip().upper()
            if not asin or asin == "NAN":
                continue
            title = str(row.get("ItemDesc") or "").strip()
            if not title or title.lower() == "nan":
                title = asin
            price = unit_price(row.get("TotalCost"), row.get("Units"))
            if asin not in seen or (price and not seen[asin]["price"]):
                seen[asin] = {
                    "asin": asin,
                    "title": title,
                    "price": price,
                }
    return list(seen.values())


def main():
    args = parse_args()

    from db import get_connection

    codes = wanted_codes(args.boxes)
    records = row_records(args.processed_dir, codes)
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    conn = get_connection()
    cur = conn.cursor()
    inserted = 0
    skipped_existing = 0
    missing_price = 0
    try:
        for record in records:
            cur.execute(
                "SELECT 1 FROM amazon_scraped_products WHERE asin=%s LIMIT 1",
                (record["asin"],),
            )
            if cur.fetchone():
                skipped_existing += 1
                continue

            if record["price"] is None:
                missing_price += 1

            title = record["title"]
            cur.execute(
                """
                INSERT INTO amazon_scraped_products (
                    asin, scraping_domain, categoria, titulo_amazon, marca,
                    precio, precio_coste, precio_amazon, rate, dimensiones,
                    peso, peso_amazon, imagen_principal, imagenes_adicionales,
                    caracteristicas, titulo_breve, descripcion,
                    descripcion_tecnica, hashtags, handle, vendor,
                    seo_title, seo_description, fecha_scraping
                ) VALUES (
                    %s, 'xlsx_base', NULL, %s, NULL,
                    NULL, NULL, %s, NULL, NULL,
                    NULL, NULL, %s, NULL,
                    NULL, %s, %s,
                    %s, NULL, %s, 'BestCash',
                    %s, %s, %s
                )
                """,
                (
                    record["asin"],
                    title,
                    record["price"],
                    NO_IMAGE_URL,
                    title[:50],
                    title,
                    title,
                    handle_for(title, record["asin"]),
                    title[:100],
                    title[:200],
                    now,
                ),
            )
            inserted += 1
        conn.commit()
    finally:
        cur.close()
        conn.close()

    print(f"records_from_xlsx={len(records)}")
    print(f"inserted={inserted}")
    print(f"skipped_existing={skipped_existing}")
    print(f"inserted_without_price={missing_price}")


if __name__ == "__main__":
    main()
