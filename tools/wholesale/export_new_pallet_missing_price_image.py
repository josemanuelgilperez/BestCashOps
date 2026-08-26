#!/usr/bin/env python3
import csv
import os
import re
import sys
from pathlib import Path


REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from db import get_connection


def get_new_pallet_codes():
    codes = set()
    for path in Path("wholesale/data/processed").glob("*.xlsx"):
        match = re.search(r"(MP\d+)", path.name, flags=re.IGNORECASE)
        if match:
            codes.add(match.group(1).upper())
    return sorted(codes)


def main():
    output_path = Path("tools/data/nuevos_66_asins_falta_precio_imagen_post_rescrape.tsv")
    codes = get_new_pallet_codes()
    if not codes:
        raise SystemExit("No hay codigos MP en wholesale/data/processed.")

    placeholders = ",".join(["%s"] * len(codes))
    missing_image_expr = """
        (
          asp.asin IS NULL
          OR asp.imagen_principal IS NULL
          OR TRIM(asp.imagen_principal) = ''
          OR asp.imagen_principal LIKE '%image_not_found%'
        )
    """
    has_price_expr = """
        (
          COALESCE(asp.precio, 0) > 0
          OR COALESCE(asp.precio_amazon, 0) > 0
          OR COALESCE(asp.precio_coste, 0) > 0
        )
    """
    missing_price_expr = f"(asp.asin IS NULL OR NOT {has_price_expr})"

    conn = get_connection()
    cur = conn.cursor(dictionary=True)
    try:
        cur.execute(
            f"""
            SELECT
              bi.box_code AS pallet,
              b.name AS pallet_nombre,
              b.category AS categoria,
              b.status AS estado,
              bi.asin,
              SUM(bi.quantity) AS unidades,
              CASE WHEN {missing_price_expr} THEN 1 ELSE 0 END AS falta_precio,
              CASE WHEN {missing_image_expr} THEN 1 ELSE 0 END AS falta_imagen,
              CASE WHEN {missing_price_expr} AND {missing_image_expr} THEN 1 ELSE 0 END AS falta_ambos,
              bi.pvp_ud,
              bi.precio_lote_ud,
              asp.precio,
              asp.precio_amazon,
              asp.precio_coste,
              asp.titulo_amazon AS titulo,
              asp.imagen_principal,
              asp.scraping_domain,
              asp.fecha_scraping
            FROM box_items bi
            JOIN boxes b ON b.code = bi.box_code
            LEFT JOIN amazon_scraped_products asp ON asp.asin = bi.asin
            WHERE bi.box_code IN ({placeholders})
              AND bi.asin IS NOT NULL
              AND bi.asin <> ''
              AND ({missing_price_expr} OR {missing_image_expr})
            GROUP BY
              bi.box_code, b.name, b.category, b.status, bi.asin,
              falta_precio, falta_imagen, falta_ambos,
              bi.pvp_ud, bi.precio_lote_ud,
              asp.precio, asp.precio_amazon, asp.precio_coste,
              asp.titulo_amazon, asp.imagen_principal,
              asp.scraping_domain, asp.fecha_scraping
            ORDER BY bi.box_code, falta_ambos DESC, falta_precio DESC, falta_imagen DESC, bi.asin
            """,
            codes,
        )
        rows = cur.fetchall()
    finally:
        cur.close()
        conn.close()

    output_path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = [
        "pallet",
        "pallet_nombre",
        "categoria",
        "estado",
        "asin",
        "unidades",
        "falta_precio",
        "falta_imagen",
        "falta_ambos",
        "pvp_ud",
        "precio_lote_ud",
        "precio",
        "precio_amazon",
        "precio_coste",
        "titulo",
        "imagen_principal",
        "scraping_domain",
        "fecha_scraping",
    ]
    with output_path.open("w", newline="", encoding="utf-8") as file_obj:
        writer = csv.DictWriter(file_obj, fieldnames=fieldnames, delimiter="\t")
        writer.writeheader()
        writer.writerows(rows)

    print(f"pallets_nuevos={len(codes)}")
    print(f"filas={len(rows)}")
    print(output_path)


if __name__ == "__main__":
    main()
