#!/usr/bin/env python3
import argparse
import csv
import os
import sys
from datetime import datetime
from decimal import Decimal, ROUND_HALF_UP
from pathlib import Path


REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from db import get_connection


def parse_args():
    parser = argparse.ArgumentParser(
        description="Aplica precios estimados por similares y calcula precio_coste."
    )
    parser.add_argument(
        "--input",
        default="tools/data/nuevos_66_precios_similares_seleccionados.tsv",
        help="TSV con asin y precio_seleccionado.",
    )
    parser.add_argument(
        "--cost-rate",
        default="0.07",
        help="Porcentaje de coste sobre precio elegido. 0.07 = 7%.",
    )
    return parser.parse_args()


def money(value):
    return Decimal(str(value).replace(",", ".")).quantize(
        Decimal("0.01"),
        rounding=ROUND_HALF_UP,
    )


def main():
    args = parse_args()
    path = Path(args.input)
    rows = list(csv.DictReader(path.open(encoding="utf-8"), delimiter="\t"))
    cost_rate = Decimal(args.cost_rate)
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    conn = get_connection()
    cur = conn.cursor()
    updated = 0
    try:
        for row in rows:
            asin = (row.get("asin") or "").strip().upper()
            selected_price = money(row["precio_seleccionado"])
            selected_cost = (selected_price * cost_rate).quantize(
                Decimal("0.01"),
                rounding=ROUND_HALF_UP,
            )
            cur.execute(
                """
                UPDATE amazon_scraped_products
                SET precio = %s,
                    precio_coste = %s,
                    scraping_domain = 'similar_price',
                    fecha_scraping = %s
                WHERE asin = %s
                """,
                (selected_price, selected_cost, now, asin),
            )
            updated += cur.rowcount
            print(f"{asin}\tprecio={selected_price}\tprecio_coste={selected_cost}")
        conn.commit()
    finally:
        cur.close()
        conn.close()

    print(f"filas_input={len(rows)}")
    print(f"filas_actualizadas={updated}")


if __name__ == "__main__":
    main()
