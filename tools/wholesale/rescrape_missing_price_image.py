#!/usr/bin/env python3
import argparse
import csv
import json
import os
import re
import sys
import time
from urllib.parse import quote_plus
from urllib.request import urlopen


REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from db import get_connection
from wholesale.pipeline.enrich import (
    CRAWLBASE_TOKEN,
    NO_IMAGE_URL,
    actualizar_imagenes_producto,
    actualizar_pvp_ud_desde_fuentes,
    build_scraped_data_from_product,
    download_and_upload_images,
    extract_product_image_urls,
    insertar_scraped_data,
    normalize_decimal,
    prepare_scraped_data_for_mysql,
)


PRICE_FIELDS = ("precio", "precio_amazon", "precio_coste")
DOMAINS = ["es", "de", "fr", "it", "com", "com.be", "co.uk", "ca", "nl", "pl", "se"]


def normalize_asin(value):
    value = (value or "").strip().upper()
    return value if re.fullmatch(r"[A-Z0-9]{10}", value) else None


def parse_args():
    parser = argparse.ArgumentParser(
        description=(
            "Re-scrapea ASINs concretos y actualiza amazon_scraped_products "
            "sin depender de amazon_delivery."
        )
    )
    parser.add_argument("--asin", action="append", help="ASIN a procesar. Puede repetirse.")
    parser.add_argument("--from-txt", help="TXT con un ASIN por linea.")
    parser.add_argument("--from-csv", help="CSV/TSV con columna asin o ASIN.")
    parser.add_argument("--limit", type=int, help="Limita la cantidad de ASINs.")
    parser.add_argument("--sleep", type=float, default=0.5, help="Pausa entre ASINs.")
    parser.add_argument("--crawl-timeout", type=int, default=10, help="Timeout por dominio.")
    parser.add_argument("--crawl-retries", type=int, default=1, help="Reintentos por dominio.")
    parser.add_argument(
        "--domains",
        default=",".join(DOMAINS),
        help="Dominios Amazon separados por coma, por ejemplo es,de,fr,it.",
    )
    parser.add_argument("--skip-images", action="store_true", help="No subir imagenes a S3.")
    parser.add_argument(
        "--max-images",
        type=int,
        default=4,
        help="Maximo de imagenes nuevas a subir por ASIN. 0 = sin limite.",
    )
    parser.add_argument(
        "--only-if-missing",
        action="store_true",
        help="Procesa solo ASINs que siguen sin precio o sin imagen en la BD.",
    )
    parser.add_argument(
        "--update-pvp",
        action="store_true",
        help="Al final rellena box_items.pvp_ud desde amazon_scraped_products.",
    )
    return parser.parse_args()


def product_price(product):
    candidates = [
        product.get("rawPrice"),
        product.get("price"),
        product.get("originalPrice"),
        (product.get("price") or {}).get("amount") if isinstance(product.get("price"), dict) else None,
        (product.get("price") or {}).get("value") if isinstance(product.get("price"), dict) else None,
    ]
    for candidate in candidates:
        price = normalize_decimal(candidate)
        if price is not None:
            return price
    return None


def product_has_image(product):
    return bool(extract_product_image_urls(product))


def intentar_scraping_rapido(
    asin,
    domains,
    need_price=False,
    need_image=False,
    timeout=10,
    retries=1,
):
    best_product = None
    best_domain = None

    for dominio in domains:
        print(f"🌍 Intentando dominio .{dominio} para ASIN {asin}", flush=True)
        url_amazon = f"https://www.amazon.{dominio}/dp/{asin}"
        encoded_url = quote_plus(url_amazon)
        crawlbase_url = (
            f"https://api.crawlbase.com/"
            f"?token={CRAWLBASE_TOKEN}"
            f"&scraper=amazon-product-details"
            f"&url={encoded_url}"
        )

        for attempt in range(1, retries + 1):
            try:
                response = urlopen(crawlbase_url, timeout=timeout).read().decode("utf-8")
                data = json.loads(response)
                status = data.get("status")
                if status and status != 200:
                    break

                body = data.get("body")
                if isinstance(body, str):
                    body = json.loads(body)
                if not isinstance(body, dict):
                    break

                name = body.get("name")
                if isinstance(name, str) and name.strip():
                    if best_product is None:
                        best_product = body
                        best_domain = dominio

                    has_needed_price = (not need_price) or product_price(body) is not None
                    has_needed_image = (not need_image) or product_has_image(body)
                    if has_needed_price and has_needed_image:
                        return body, dominio
                break
            except Exception as exc:
                if attempt == retries:
                    print(f"   ⚠️ Fallo .{dominio}: {exc}", flush=True)
                else:
                    print(f"   ⚠️ Reintento {attempt}/{retries} .{dominio}: {exc}", flush=True)

    return best_product, best_domain


def read_asins_from_txt(path):
    asins = []
    with open(path, encoding="utf-8") as file_obj:
        for line in file_obj:
            asin = normalize_asin(line)
            if asin:
                asins.append(asin)
    return asins


def read_asins_from_table(path):
    with open(path, newline="", encoding="utf-8") as file_obj:
        sample = file_obj.read(4096)
        file_obj.seek(0)
        dialect = csv.Sniffer().sniff(sample, delimiters=",;\t")
        reader = csv.DictReader(file_obj, dialect=dialect)
        if reader.fieldnames:
            asin_field = next(
                (name for name in reader.fieldnames if name.strip().lower() == "asin"),
                None,
            )
            if asin_field:
                return [
                    asin
                    for row in reader
                    for asin in [normalize_asin(row.get(asin_field))]
                    if asin
                ]

        file_obj.seek(0)
        reader = csv.reader(file_obj, dialect=dialect)
        return [
            asin
            for row in reader
            if row
            for asin in [normalize_asin(row[0])]
            if asin
        ]


def unique_preserve_order(values):
    seen = set()
    result = []
    for value in values:
        if value not in seen:
            seen.add(value)
            result.append(value)
    return result


def load_asins(args):
    asins = []
    for asin in args.asin or []:
        normalized = normalize_asin(asin)
        if normalized:
            asins.append(normalized)
    if args.from_txt:
        asins.extend(read_asins_from_txt(args.from_txt))
    if args.from_csv:
        asins.extend(read_asins_from_table(args.from_csv))

    asins = unique_preserve_order(asins)
    if args.limit:
        asins = asins[: args.limit]
    return asins


def is_missing_price(row):
    if not row:
        return True
    return all(normalize_decimal(row.get(field)) is None for field in PRICE_FIELDS)


def is_missing_image(row):
    if not row:
        return True
    image = (row.get("imagen_principal") or "").strip()
    return not image or image == NO_IMAGE_URL or "image_not_found" in image


def get_existing_product(cursor, asin):
    cursor.execute(
        """
        SELECT *
        FROM amazon_scraped_products
        WHERE asin = %s
        LIMIT 1
        """,
        (asin,),
    )
    return cursor.fetchone()


def get_delivery_record(cursor, asin):
    cursor.execute(
        """
        SELECT ItemDesc, UnitCost, UnitRecovery, RecoveryRate, ItemPkgWeight, GLDesc
        FROM amazon_delivery
        WHERE Asin = %s
        ORDER BY ShipmentClosed DESC
        LIMIT 1
        """,
        (asin,),
    )
    return cursor.fetchone() or {}


def keep_existing_when_new_is_empty(new_data, existing):
    if not existing:
        return new_data

    preserve_keys = [
        "categoria",
        "titulo_amazon",
        "marca",
        "precio",
        "precio_coste",
        "precio_amazon",
        "rate",
        "dimensiones",
        "peso",
        "peso_amazon",
        "imagen_principal",
        "imagenes_adicionales",
        "caracteristicas",
        "titulo_breve",
        "descripcion",
        "descripcion_tecnica",
        "hashtags",
        "handle",
        "vendor",
        "seo_title",
        "seo_description",
    ]
    for key in preserve_keys:
        value = new_data.get(key)
        existing_value = existing.get(key)
        if value in (None, "", NO_IMAGE_URL) and existing_value not in (None, "", NO_IMAGE_URL):
            new_data[key] = existing_value
    return new_data


def patch_price_from_product(data, product):
    candidates = [
        product.get("rawPrice"),
        (product.get("price") or {}).get("amount") if isinstance(product.get("price"), dict) else None,
        (product.get("price") or {}).get("value") if isinstance(product.get("price"), dict) else None,
    ]
    for candidate in candidates:
        price = normalize_decimal(candidate)
        if price is not None:
            data["precio"] = price
            return


def process_asin(conn, asin, args):
    cursor = conn.cursor(dictionary=True)
    try:
        existing = get_existing_product(cursor, asin)
        missing_price_before = is_missing_price(existing)
        missing_image_before = is_missing_image(existing)

        if args.only_if_missing and not (missing_price_before or missing_image_before):
            print(f"SKIP {asin}: ya tiene precio e imagen", flush=True)
            return "skipped"

        print(
            f"ASIN {asin}: falta_precio={int(missing_price_before)} "
            f"falta_imagen={int(missing_image_before)}",
            flush=True,
        )

        product, dominio = intentar_scraping_rapido(
            asin,
            domains=[d.strip() for d in args.domains.split(",") if d.strip()],
            need_price=missing_price_before,
            need_image=missing_image_before and not args.skip_images,
            timeout=args.crawl_timeout,
            retries=args.crawl_retries,
        )
        if not product:
            print(f"MISS {asin}: Crawlbase no devolvio producto", flush=True)
            return "miss"

        record = get_delivery_record(cursor, asin)
        data = build_scraped_data_from_product(asin, record, product, dominio)
        data["precio"] = product_price(product)
        patch_price_from_product(data, product)
        data = keep_existing_when_new_is_empty(data, existing)
        data = prepare_scraped_data_for_mysql(data)

        insertar_scraped_data(data, cursor)
        conn.commit()

        image_count = 0
        if not args.skip_images and missing_image_before:
            images = extract_product_image_urls(product)
            if args.max_images and args.max_images > 0:
                images = images[: args.max_images]
            urls_s3 = download_and_upload_images(asin, images)
            if urls_s3:
                actualizar_imagenes_producto(asin, urls_s3, cursor)
                conn.commit()
                image_count = len(urls_s3)

        updated = get_existing_product(cursor, asin)
        print(
            f"OK {asin}: precio={int(not is_missing_price(updated))} "
            f"imagen={int(not is_missing_image(updated))} imagenes_subidas={image_count}",
            flush=True,
        )
        return "ok"
    except Exception as exc:
        conn.rollback()
        print(f"ERROR {asin}: {exc}", flush=True)
        return "error"
    finally:
        cursor.close()


def main():
    try:
        sys.stdout.reconfigure(line_buffering=True)
    except Exception:
        pass

    args = parse_args()
    asins = load_asins(args)
    if not asins:
        raise SystemExit("No hay ASINs validos para procesar.")

    print(f"ASINs a re-scrapear: {len(asins)}", flush=True)

    conn = get_connection()
    counts = {"ok": 0, "miss": 0, "error": 0, "skipped": 0}
    try:
        for idx, asin in enumerate(asins, start=1):
            print(f"\n[{idx}/{len(asins)}]", flush=True)
            status = process_asin(conn, asin, args)
            counts[status] = counts.get(status, 0) + 1
            if args.sleep:
                time.sleep(args.sleep)

        if args.update_pvp:
            print("\nActualizando box_items.pvp_ud desde fuentes...", flush=True)
            affected = actualizar_pvp_ud_desde_fuentes(conn)
            conn.commit()
            print(f"pvp_ud filas afectadas: {affected}", flush=True)
    finally:
        conn.close()

    print(
        "\nResumen: "
        + ", ".join(f"{key}={counts.get(key, 0)}" for key in sorted(counts)),
        flush=True,
    )


if __name__ == "__main__":
    main()
