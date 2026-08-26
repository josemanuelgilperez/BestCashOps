#!/usr/bin/env python3
import os
import re
import sys
from pathlib import Path


REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from db import get_connection


def main():
    codes = set()
    for path in Path("wholesale/data/processed").glob("*.xlsx"):
        match = re.search(r"(MP\d+)", path.name, flags=re.IGNORECASE)
        if match:
            codes.add(match.group(1).upper())
    codes = sorted(codes)
    if not codes:
        raise SystemExit("No hay Excel procesados en wholesale/data/processed.")

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
              COUNT(DISTINCT bi.asin) AS total_asins,
              COUNT(DISTINCT CASE WHEN {missing_price_expr} THEN bi.asin END) AS sin_precio,
              COUNT(DISTINCT CASE WHEN {missing_image_expr} THEN bi.asin END) AS sin_imagen,
              COUNT(DISTINCT CASE WHEN {missing_price_expr} AND {missing_image_expr} THEN bi.asin END) AS sin_ambas,
              COUNT(DISTINCT CASE WHEN {missing_price_expr} OR {missing_image_expr} THEN bi.asin END) AS con_alguna_falta,
              COUNT(*) AS lineas
            FROM box_items bi
            JOIN boxes b ON b.code = bi.box_code
            LEFT JOIN amazon_scraped_products asp ON asp.asin = bi.asin
            WHERE bi.box_code IN ({placeholders})
              AND bi.asin IS NOT NULL
              AND bi.asin <> ''
            """,
            codes,
        )
        summary = cur.fetchone()
        print(f"pallets_nuevos={len(codes)}")
        print(
            "resumen "
            + " ".join(f"{key}={value}" for key, value in summary.items())
        )

        cur.execute(
            f"""
            SELECT
              bi.box_code AS pallet,
              b.name AS nombre,
              COUNT(DISTINCT bi.asin) AS total_asins,
              COUNT(DISTINCT CASE WHEN {missing_price_expr} THEN bi.asin END) AS sin_precio,
              COUNT(DISTINCT CASE WHEN {missing_image_expr} THEN bi.asin END) AS sin_imagen,
              COUNT(DISTINCT CASE WHEN {missing_price_expr} AND {missing_image_expr} THEN bi.asin END) AS sin_ambas,
              COUNT(DISTINCT CASE WHEN {missing_price_expr} OR {missing_image_expr} THEN bi.asin END) AS con_alguna_falta
            FROM box_items bi
            JOIN boxes b ON b.code = bi.box_code
            LEFT JOIN amazon_scraped_products asp ON asp.asin = bi.asin
            WHERE bi.box_code IN ({placeholders})
              AND bi.asin IS NOT NULL
              AND bi.asin <> ''
            GROUP BY bi.box_code, b.name
            HAVING con_alguna_falta > 0
            ORDER BY con_alguna_falta DESC, bi.box_code
            """,
            codes,
        )
        rows = cur.fetchall()
        print(f"pallets_con_faltas={len(rows)}")
        print("pallet\tnombre\ttotal_asins\tsin_precio\tsin_imagen\tsin_ambas\tcon_alguna_falta")
        for row in rows:
            print(
                "\t".join(
                    str(row[key] or "")
                    for key in (
                        "pallet",
                        "nombre",
                        "total_asins",
                        "sin_precio",
                        "sin_imagen",
                        "sin_ambas",
                        "con_alguna_falta",
                    )
                )
            )
    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    main()
