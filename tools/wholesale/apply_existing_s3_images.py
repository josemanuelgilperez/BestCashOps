#!/usr/bin/env python3
import argparse
import csv
import os
import re
import sys


REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from db import get_connection
from wholesale.pipeline.enrich import get_existing_images_from_s3


NO_IMAGE_MARKER = "image_not_found"


def parse_args():
    parser = argparse.ArgumentParser(
        description="Actualiza imagen_principal desde imagenes ya existentes en S3."
    )
    parser.add_argument("--from-csv", required=True, help="CSV/TSV con columna asin.")
    parser.add_argument("--limit", type=int)
    return parser.parse_args()


def normalize_asin(value):
    value = (value or "").strip().upper()
    return value if re.fullmatch(r"[A-Z0-9]{10}", value) else None


def load_asins(path, limit=None):
    with open(path, newline="", encoding="utf-8") as file_obj:
        sample = file_obj.read(4096)
        file_obj.seek(0)
        dialect = csv.Sniffer().sniff(sample, delimiters=",;\t")
        reader = csv.DictReader(file_obj, dialect=dialect)
        asin_field = next(
            (name for name in (reader.fieldnames or []) if name.strip().lower() == "asin"),
            None,
        )
        if not asin_field:
            raise SystemExit("No encuentro columna asin.")
        seen = set()
        asins = []
        for row in reader:
            asin = normalize_asin(row.get(asin_field))
            if asin and asin not in seen:
                seen.add(asin)
                asins.append(asin)
                if limit and len(asins) >= limit:
                    break
        return asins


def missing_image(row):
    image = (row.get("imagen_principal") or "").strip()
    return not image or NO_IMAGE_MARKER in image


def main():
    args = parse_args()
    asins = load_asins(args.from_csv, args.limit)
    conn = get_connection()
    cur = conn.cursor(dictionary=True)
    updated = 0
    skipped = 0
    no_s3 = 0
    try:
        for idx, asin in enumerate(asins, start=1):
            cur.execute(
                "SELECT imagen_principal FROM amazon_scraped_products WHERE asin=%s LIMIT 1",
                (asin,),
            )
            row = cur.fetchone()
            if not missing_image(row):
                skipped += 1
                continue
            urls = get_existing_images_from_s3(asin)
            if not urls:
                no_s3 += 1
                continue
            cur.execute(
                """
                UPDATE amazon_scraped_products
                SET imagen_principal=%s, imagenes_adicionales=%s
                WHERE asin=%s
                """,
                (urls[0], ", ".join(urls[1:]) if len(urls) > 1 else None, asin),
            )
            updated += cur.rowcount
            if idx % 100 == 0:
                conn.commit()
                print(f"procesados={idx} updated={updated} no_s3={no_s3}", flush=True)
        conn.commit()
    finally:
        cur.close()
        conn.close()
    print(f"asins={len(asins)}")
    print(f"updated={updated}")
    print(f"skipped={skipped}")
    print(f"no_s3={no_s3}")


if __name__ == "__main__":
    main()
