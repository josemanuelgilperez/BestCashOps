#!/usr/bin/env python3
import argparse
import csv
import difflib
import os
import re
import sys
from decimal import Decimal
from pathlib import Path


REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from db import get_connection


def parse_args():
    parser = argparse.ArgumentParser(
        description="Sugiere precio para ASIN sin precio usando productos similares ya existentes en BD."
    )
    parser.add_argument(
        "--missing-tsv",
        default="tools/data/nuevos_66_asins_falta_precio_imagen_post_rescrape.tsv",
        help="TSV de faltantes exportado desde los 66 pallets nuevos.",
    )
    parser.add_argument(
        "--output",
        default="tools/data/nuevos_66_sugerencias_precio_por_similares.tsv",
        help="TSV de salida con candidatos de precio.",
    )
    parser.add_argument("--top", type=int, default=5, help="Numero de candidatos por ASIN.")
    parser.add_argument(
        "--min-score",
        type=float,
        default=0.28,
        help="Puntuacion minima de similitud para incluir candidato.",
    )
    return parser.parse_args()


def normalize_text(value):
    value = (value or "").lower()
    value = re.sub(r"[^a-z0-9áéíóúüñ]+", " ", value)
    value = re.sub(r"\s+", " ", value).strip()
    return value


def tokens(value):
    stop = {
        "de",
        "del",
        "la",
        "el",
        "los",
        "las",
        "para",
        "con",
        "sin",
        "por",
        "en",
        "un",
        "una",
        "y",
        "o",
        "set",
        "pcs",
        "pieza",
        "piezas",
    }
    return {tok for tok in normalize_text(value).split() if len(tok) > 2 and tok not in stop}


def as_float(value):
    if value is None:
        return None
    if isinstance(value, Decimal):
        return float(value)
    try:
        return float(str(value).replace(",", "."))
    except ValueError:
        return None


def load_missing(path):
    rows = []
    seen = set()
    with open(path, encoding="utf-8") as file_obj:
        reader = csv.DictReader(file_obj, delimiter="\t")
        for row in reader:
            if row.get("falta_precio") != "1":
                continue
            asin = (row.get("asin") or "").strip().upper()
            if not asin or asin in seen:
                continue
            seen.add(asin)
            rows.append(row)
    return rows


def load_candidate_products(conn, source_row, limit=500):
    cur = conn.cursor(dictionary=True)
    try:
        source_tokens = sorted(tokens(source_row.get("titulo")), key=len, reverse=True)
        source_tokens = [tok for tok in source_tokens if len(tok) >= 4][:8]
        if not source_tokens:
            source_tokens = sorted(tokens(source_row.get("titulo")))[:4]

        params = []
        like_clauses = []
        for token in source_tokens:
            like_clauses.append("(titulo_amazon LIKE %s OR titulo_breve LIKE %s)")
            params.extend([f"%{token}%", f"%{token}%"])

        where_like = " OR ".join(like_clauses) or "1=1"
        cur.execute(
            f"""
            SELECT
              asin,
              titulo_amazon,
              titulo_breve,
              categoria,
              marca,
              precio,
              precio_amazon,
              precio_coste,
              imagen_principal,
              scraping_domain
            FROM amazon_scraped_products
            WHERE asin IS NOT NULL
              AND asin <> ''
              AND (
                COALESCE(precio, 0) > 0
                OR COALESCE(precio_amazon, 0) > 0
                OR COALESCE(precio_coste, 0) > 0
              )
              AND ({where_like})
            LIMIT {int(limit)}
            """,
            params,
        )
        products = []
        for row in cur.fetchall():
            title = row.get("titulo_amazon") or row.get("titulo_breve") or ""
            norm = normalize_text(title)
            tok = tokens(title)
            price = as_float(row.get("precio")) or as_float(row.get("precio_amazon")) or as_float(row.get("precio_coste"))
            if not norm or not tok or not price:
                continue
            row["_title"] = title
            row["_norm"] = norm
            row["_tokens"] = tok
            row["_price"] = price
            products.append(row)
        return products
    finally:
        cur.close()


def score_match(source, candidate):
    source_title = source.get("titulo") or source.get("titulo_amazon") or ""
    source_tokens = tokens(source_title)
    if not source_tokens:
        return 0

    candidate_tokens = candidate["_tokens"]
    overlap = len(source_tokens & candidate_tokens) / max(len(source_tokens), 1)
    title_ratio = difflib.SequenceMatcher(
        None,
        normalize_text(source_title),
        candidate["_norm"],
    ).ratio()

    score = (0.65 * overlap) + (0.35 * title_ratio)

    source_category = normalize_text(source.get("categoria"))
    candidate_category = normalize_text(candidate.get("categoria"))
    if source_category and candidate_category and source_category == candidate_category:
        score += 0.08

    return min(score, 1.0)


def pick_recommended_price(candidates):
    if not candidates:
        return None
    top_prices = sorted(c["_price"] for c in candidates[:3])
    return top_prices[len(top_prices) // 2]


def main():
    args = parse_args()
    missing = load_missing(args.missing_tsv)

    conn = get_connection()
    output_rows = []
    try:
        for row in missing:
            scored = []
            asin = row.get("asin")
            for candidate in load_candidate_products(conn, row):
                if candidate["asin"] == asin:
                    continue
                score = score_match(row, candidate)
                if score >= args.min_score:
                    scored.append((score, candidate))
            scored.sort(key=lambda item: item[0], reverse=True)
            candidates = [candidate for _, candidate in scored[: args.top]]
            recommended = pick_recommended_price(candidates)

            if not candidates:
                output_rows.append(
                    {
                        "asin": asin,
                        "pallet": row.get("pallet"),
                        "titulo": row.get("titulo"),
                        "precio_sugerido": "",
                        "candidate_rank": "",
                        "candidate_score": "",
                        "candidate_asin": "",
                        "candidate_price": "",
                        "candidate_title": "",
                        "candidate_category": "",
                        "candidate_image": "",
                    }
                )
                continue

            for rank, (score, candidate) in enumerate(scored[: args.top], start=1):
                output_rows.append(
                    {
                        "asin": asin,
                        "pallet": row.get("pallet"),
                        "titulo": row.get("titulo"),
                        "precio_sugerido": f"{recommended:.2f}" if recommended is not None else "",
                        "candidate_rank": rank,
                        "candidate_score": f"{score:.3f}",
                        "candidate_asin": candidate.get("asin"),
                        "candidate_price": f"{candidate['_price']:.2f}",
                        "candidate_title": candidate.get("_title"),
                        "candidate_category": candidate.get("categoria") or "",
                        "candidate_image": candidate.get("imagen_principal") or "",
                    }
                )
    finally:
        conn.close()

    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = [
        "asin",
        "pallet",
        "titulo",
        "precio_sugerido",
        "candidate_rank",
        "candidate_score",
        "candidate_asin",
        "candidate_price",
        "candidate_title",
        "candidate_category",
        "candidate_image",
    ]
    with output.open("w", newline="", encoding="utf-8") as file_obj:
        writer = csv.DictWriter(file_obj, fieldnames=fieldnames, delimiter="\t")
        writer.writeheader()
        writer.writerows(output_rows)

    with_candidates = len({r["asin"] for r in output_rows if r.get("candidate_asin")})
    print(f"sin_precio={len(missing)}")
    print(f"con_candidatos={with_candidates}")
    print(f"salida={output}")


if __name__ == "__main__":
    main()
