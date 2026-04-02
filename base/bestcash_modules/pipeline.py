import logging
import csv
from datetime import datetime

from .utils import (
    sanitize_text,
    parse_price,
    clean_price,
    get_product_price,
    generate_shopify_handle,
    traducir_categoria,
    extraer_dimensiones_y_peso,
    construir_texto_dimensiones_peso,
    normalize_payload_dict,
)
from .attributes import extraer_atributos
from .scraping import intentar_scraping_con_dominios
from .images import download_and_upload_images
from .ai import (
    generar_contenido_completo,
    traducir_a_espanol,
    limpiar_dimensiones_y_extraer_peso_gramos,
)
from .db import (
    get_connection,
    get_asins,
    get_pending_asins,
    filter_missing_asins,
    get_delivery,
    upsert_scraped_product,
)


def _read_asins_from_csv(csv_path):
    asins = []
    with open(csv_path, newline="", encoding="utf-8") as file_obj:
        reader = csv.reader(file_obj)
        for row in reader:
            if not row:
                continue
            asin = (row[0] or "").strip()
            if asin:
                asins.append(asin)
    return asins


def _read_asins_from_txt(txt_path):
    asins = []
    with open(txt_path, encoding="utf-8") as file_obj:
        for line in file_obj:
            asin = (line or "").strip()
            if asin:
                asins.append(asin)
    return asins


def run_pipeline(asin=None, limit=None, from_csv=None, from_txt=None, skip_existing=True):
    conn = get_connection()
    procesados = 0
    insertados = 0
    sin_delivery = 0
    sin_titulo = 0
    omitidos_existentes = 0

    try:
        if from_csv:
            asins = _read_asins_from_csv(from_csv)
            if skip_existing:
                before = len(asins)
                asins = filter_missing_asins(conn, asins)
                omitidos_existentes += before - len(asins)
        elif from_txt:
            asins = _read_asins_from_txt(from_txt)
            if skip_existing:
                before = len(asins)
                asins = filter_missing_asins(conn, asins)
                omitidos_existentes += before - len(asins)
        elif asin:
            asins = [asin]
            if skip_existing:
                filtered = filter_missing_asins(conn, asins)
                if not filtered:
                    logging.info("ASIN %s ya existe en amazon_scraped_products. Se omite.", asin)
                    asins = []
                    omitidos_existentes += 1
                else:
                    asins = filtered
        else:
            asins = get_pending_asins(conn) if skip_existing else get_asins(conn)

        if limit and limit > 0:
            asins = asins[:limit]

        logging.info("ASINs a procesar: %s", len(asins))

        for current_asin in asins:
            procesados += 1
            logging.info("[%s/%s] Procesando ASIN %s", procesados, len(asins), current_asin)

            product, scraping_domain = intentar_scraping_con_dominios(current_asin)
            delivery = get_delivery(conn, current_asin)

            if not delivery:
                logging.warning("ASIN %s sin datos en amazon_delivery. Se omite.", current_asin)
                sin_delivery += 1
                continue

            itemdesc = sanitize_text(delivery.get("ItemDesc"))
            atributos = extraer_atributos(itemdesc)

            precio = clean_price(get_product_price(product))
            precio_amazon = clean_price(parse_price(delivery.get("UnitCost")))
            precio_coste = clean_price(parse_price(delivery.get("UnitRecovery")))
            rate = parse_price(delivery.get("RecoveryRate"))
            peso_amazon = parse_price(delivery.get("ItemPkgWeight"))

            titulo_base = sanitize_text(product.get("name")) if product else itemdesc
            if not titulo_base:
                logging.warning("ASIN %s sin titulo base. Se omite.", current_asin)
                sin_titulo += 1
                continue

            imagenes_urls = []
            caracteristicas_raw = []
            caracteristicas_text = None
            descripcion_tecnica = None
            marca = None
            categoria = traducir_categoria((product or {}).get("gl") or delivery.get("GLDesc"))
            dimensiones = None
            peso = None
            if product:
                imagenes = product.get("highResolutionImages") or product.get("images") or []
                imagenes_urls = download_and_upload_images(current_asin, imagenes)
                caracteristicas_raw = product.get("features", []) or []
                marca = sanitize_text(product.get("brand"))
                dimensiones, peso = extraer_dimensiones_y_peso(product)
                if dimensiones is None or peso is None:
                    texto_dim_peso = construir_texto_dimensiones_peso(product)
                    dim_ia, peso_ia = limpiar_dimensiones_y_extraer_peso_gramos(texto_dim_peso)
                    if not dimensiones and dim_ia:
                        dimensiones = dim_ia
                    if peso is None and peso_ia is not None:
                        peso = peso_ia
                descripcion_tecnica = "\n".join(caracteristicas_raw) if caracteristicas_raw else None
                logging.info(
                    "ASIN %s scraping OK (%s) | imagenes=%s | precio=%s",
                    current_asin,
                    scraping_domain,
                    len(imagenes_urls),
                    precio,
                )
            else:
                logging.info(
                    "ASIN %s sin scraping, usando fallback amazon_delivery",
                    current_asin,
                )

            features = "\n".join(caracteristicas_raw) if caracteristicas_raw else ""
            desc_amz = product.get("description", "") if product else ""

            contenido_ia = generar_contenido_completo(
                titulo_base,
                desc_amz or itemdesc or "",
                caracteristicas_raw,
            )

            titulo = contenido_ia.get("titulo_amazon") or titulo_base
            if atributos.get("talla"):
                titulo += f" T{atributos['talla']}"
            if atributos.get("color"):
                titulo += f" {atributos['color']}"
            titulo = titulo[:120]

            titulo_breve = contenido_ia.get("titulo_breve") or titulo[:120]
            caracteristicas_text = contenido_ia.get("caracteristicas") or features or None
            hashtags = contenido_ia.get("hashtags") or None
            vendor = contenido_ia.get("vendor") or "BestCash"
            seo_title = contenido_ia.get("seo_title") or titulo[:120]
            seo_description = contenido_ia.get("seo_description") or ""

            descripcion = contenido_ia.get("descripcion")
            if not descripcion:
                if features:
                    descripcion = "\n".join(
                        [f"✔️ {feat}" for feat in caracteristicas_raw[:5]]
                    )
                else:
                    descripcion = titulo
            descripcion += f"\nRef. BestCash {current_asin}"

            descripcion_tecnica = (
                contenido_ia.get("descripcion_tecnica")
                or descripcion_tecnica
                or caracteristicas_text
                or titulo
            )

            # Refuerzo: garantiza espanol incluso si hay fallbacks sin IA.
            titulo = traducir_a_espanol(titulo)
            titulo_breve = traducir_a_espanol(titulo_breve)
            descripcion = traducir_a_espanol(descripcion)
            descripcion_tecnica = traducir_a_espanol(descripcion_tecnica)
            if caracteristicas_text:
                caracteristicas_text = traducir_a_espanol(caracteristicas_text)
            if hashtags:
                hashtags = traducir_a_espanol(hashtags)
            if dimensiones:
                dimensiones = traducir_a_espanol(dimensiones)
            vendor = traducir_a_espanol(vendor)
            seo_title = traducir_a_espanol(seo_title)
            seo_description = traducir_a_espanol(seo_description or descripcion[:320])

            titulo = (titulo or titulo_base)[:120]
            titulo_breve = (titulo_breve or titulo)[:120]
            seo_title = (seo_title or titulo)[:120]
            seo_description = (seo_description or descripcion[:320])[:320]

            data = {
                "asin": current_asin,
                "scraping_domain": scraping_domain or "delivery",
                "categoria": categoria,
                "titulo_amazon": titulo,
                "marca": marca,
                "precio": precio,
                "precio_amazon": precio_amazon,
                "precio_coste": precio_coste,
                "rate": rate,
                "dimensiones": dimensiones,
                "peso": peso,
                "peso_amazon": peso_amazon,
                "imagen_principal": imagenes_urls[0] if imagenes_urls else None,
                "imagenes_adicionales": (
                    ", ".join(imagenes_urls[1:]) if len(imagenes_urls) > 1 else None
                ),
                "caracteristicas": caracteristicas_text,
                "titulo_breve": titulo_breve,
                "descripcion": descripcion,
                "descripcion_tecnica": descripcion_tecnica,
                "hashtags": hashtags,
                "handle": generate_shopify_handle(titulo, current_asin),
                "vendor": vendor,
                "seo_title": seo_title,
                "seo_description": seo_description,
                "fecha_scraping": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            }

            upsert_scraped_product(conn, normalize_payload_dict(data))
            insertados += 1
            logging.info(
                "ASIN %s guardado | categoria=%s | precio=%s | coste=%s | amazon=%s",
                current_asin,
                categoria,
                precio,
                precio_coste,
                precio_amazon,
            )
    finally:
        conn.close()

    logging.info(
        "FIN | procesados=%s insertados=%s omitidos_existentes=%s sin_delivery=%s sin_titulo=%s",
        procesados,
        insertados,
        omitidos_existentes,
        sin_delivery,
        sin_titulo,
    )
