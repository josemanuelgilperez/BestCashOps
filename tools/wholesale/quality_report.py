#!/usr/bin/env python3
import argparse
import csv
import os
import re
import sys
from pathlib import Path


REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from db import get_connection
from wholesale.pipeline.finance import resolve_scope_codes


def parse_args():
    parser = argparse.ArgumentParser(description="Reporte de calidad de pallets mayoristas.")
    parser.add_argument("--boxes", action="append", help="Códigos MP separados por coma. Puede repetirse.")
    parser.add_argument("--from-asins", help="TXT de ASINs para resolver pallets afectados.")
    parser.add_argument("--new-pallets", action="store_true", help="Usa wholesale/data/processed/*.xlsx.")
    parser.add_argument("--all-available", action="store_true", help="Todos los pallets Disponible/Reservado.")
    parser.add_argument(
        "--output-prefix",
        default="tools/data/quality",
        help="Prefijo de salida. Genera *_missing_by_asin.tsv y *_summary_by_pallet.tsv.",
    )
    return parser.parse_args()


def _price_expr():
    return """
        (
          COALESCE(asp.precio, 0) > 0
          OR COALESCE(asp.precio_amazon, 0) > 0
          OR COALESCE(asp.precio_coste, 0) > 0
        )
    """


def _missing_image_expr():
    return """
        (
          asp.asin IS NULL
          OR asp.imagen_principal IS NULL
          OR TRIM(asp.imagen_principal) = ''
          OR asp.imagen_principal LIKE '%image_not_found%'
        )
    """


def _where_scope(codes, all_available):
    clauses = [
        "b.status IN ('Disponible','Reservado')",
        "bi.asin IS NOT NULL",
        "bi.asin <> ''",
    ]
    params = []
    if codes:
        placeholders = ",".join(["%s"] * len(codes))
        clauses.append(f"bi.box_code IN ({placeholders})")
        params.extend(codes)
    elif not all_available:
        raise SystemExit("Indica --new-pallets, --boxes, --from-asins o --all-available.")
    return " AND ".join(clauses), params


def run_report(codes, output_prefix, all_available=False):
    has_price = _price_expr()
    missing_price = f"(asp.asin IS NULL OR NOT {has_price})"
    missing_image = _missing_image_expr()
    where_sql, params = _where_scope(codes, all_available)

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
              CASE WHEN {missing_price} THEN 1 ELSE 0 END AS falta_precio,
              CASE WHEN {missing_image} THEN 1 ELSE 0 END AS falta_imagen,
              CASE WHEN {missing_price} AND {missing_image} THEN 1 ELSE 0 END AS falta_ambos,
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
            WHERE {where_sql}
              AND ({missing_price} OR {missing_image})
            GROUP BY
              bi.box_code, b.name, b.category, b.status, bi.asin,
              falta_precio, falta_imagen, falta_ambos,
              bi.pvp_ud, bi.precio_lote_ud,
              asp.precio, asp.precio_amazon, asp.precio_coste,
              asp.titulo_amazon, asp.imagen_principal,
              asp.scraping_domain, asp.fecha_scraping
            ORDER BY bi.box_code, falta_ambos DESC, falta_precio DESC, falta_imagen DESC, bi.asin
            """,
            params,
        )
        detail_rows = cur.fetchall()

        cur.execute(
            f"""
            SELECT
              bi.box_code AS pallet,
              b.name AS nombre,
              COUNT(DISTINCT bi.asin) AS total_asins,
              COUNT(DISTINCT CASE WHEN {missing_price} THEN bi.asin END) AS sin_precio,
              COUNT(DISTINCT CASE WHEN {missing_image} THEN bi.asin END) AS sin_imagen,
              COUNT(DISTINCT CASE WHEN {missing_price} AND {missing_image} THEN bi.asin END) AS sin_ambas,
              COUNT(DISTINCT CASE WHEN {missing_price} OR {missing_image} THEN bi.asin END) AS con_alguna_falta,
              COUNT(DISTINCT CASE WHEN asp.scraping_domain = 'similar_price' THEN bi.asin END) AS con_precio_estimado
            FROM box_items bi
            JOIN boxes b ON b.code = bi.box_code
            LEFT JOIN amazon_scraped_products asp ON asp.asin = bi.asin
            WHERE {where_sql}
            GROUP BY bi.box_code, b.name
            HAVING con_alguna_falta > 0 OR con_precio_estimado > 0
            ORDER BY con_alguna_falta DESC, con_precio_estimado DESC, bi.box_code
            """,
            params,
        )
        summary_rows = cur.fetchall()
    finally:
        cur.close()
        conn.close()

    prefix = Path(output_prefix)
    prefix.parent.mkdir(parents=True, exist_ok=True)
    detail_path = prefix.with_name(prefix.name + "_missing_by_asin.tsv")
    summary_path = prefix.with_name(prefix.name + "_summary_by_pallet.tsv")

    detail_fields = [
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
    summary_fields = [
        "pallet",
        "nombre",
        "total_asins",
        "sin_precio",
        "sin_imagen",
        "sin_ambas",
        "con_alguna_falta",
        "con_precio_estimado",
    ]

    with detail_path.open("w", newline="", encoding="utf-8") as file_obj:
        writer = csv.DictWriter(file_obj, fieldnames=detail_fields, delimiter="\t")
        writer.writeheader()
        writer.writerows(detail_rows)

    with summary_path.open("w", newline="", encoding="utf-8") as file_obj:
        writer = csv.DictWriter(file_obj, fieldnames=summary_fields, delimiter="\t")
        writer.writeheader()
        writer.writerows(summary_rows)

    unique_asins = {row["asin"] for row in detail_rows}
    sin_precio = {row["asin"] for row in detail_rows if row["falta_precio"] == 1}
    sin_imagen = {row["asin"] for row in detail_rows if row["falta_imagen"] == 1}
    sin_ambas = {row["asin"] for row in detail_rows if row["falta_ambos"] == 1}

    print(f"scope_pallets={len(codes) if codes else 'all_available'}")
    print(f"asins_con_alguna_falta={len(unique_asins)}")
    print(f"sin_precio={len(sin_precio)} sin_imagen={len(sin_imagen)} sin_ambas={len(sin_ambas)}")
    print(f"pallets_reportados={len(summary_rows)}")
    print(f"detalle={detail_path}")
    print(f"resumen={summary_path}")


def main():
    args = parse_args()
    codes = resolve_scope_codes(
        boxes=args.boxes,
        from_asins=args.from_asins,
        new_pallets=args.new_pallets,
    )
    run_report(codes, args.output_prefix, all_available=args.all_available)


if __name__ == "__main__":
    main()
