import os
import re
import json
import unicodedata
import argparse
import requests
import mysql.connector
from urllib.request import urlopen
from urllib.parse import quote_plus
from datetime import datetime
from decimal import Decimal, InvalidOperation
from dotenv import load_dotenv, find_dotenv
import sys

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
REPO_ROOT = os.path.dirname(BASE_DIR)
for _p in (REPO_ROOT, BASE_DIR):
    if _p not in sys.path:
        sys.path.insert(0, _p)

from db import get_connection
from unidecode import unidecode
from openai import OpenAI
import boto3
from botocore.exceptions import ProfileNotFound
import time
from io import BytesIO

# ------------------------------
# CONFIGURACIÓN
# ------------------------------
load_dotenv(find_dotenv())

db_config = None  # la conexión real se obtiene desde db.get_connection()

CRAWLBASE_TOKEN = os.getenv("CRAWLBASE_TOKEN", "9a_E5QjtbAz2sAbVt2U3vQ")
IMAGE_BUCKET = 'bestcashproductimages'
NO_IMAGE_URL = f"https://{IMAGE_BUCKET}.s3.amazonaws.com/image_not_found.jpg"

_s3_client = None


def _get_s3_client():
    """Cliente S3 perezoso: claves en env o cadena por defecto; tolera AWS_PROFILE sin perfil en disco."""
    global _s3_client
    if _s3_client is not None:
        return _s3_client

    key = os.environ.get("AWS_ACCESS_KEY_ID")
    secret = os.environ.get("AWS_SECRET_ACCESS_KEY")
    if key and secret:
        _s3_client = boto3.client(
            "s3",
            aws_access_key_id=key,
            aws_secret_access_key=secret,
            aws_session_token=os.environ.get("AWS_SESSION_TOKEN") or None,
        )
        return _s3_client

    try:
        _s3_client = boto3.client("s3")
    except ProfileNotFound:
        old_profile = os.environ.pop("AWS_PROFILE", None)
        try:
            _s3_client = boto3.client("s3")
        finally:
            if old_profile is not None:
                os.environ["AWS_PROFILE"] = old_profile

    return _s3_client


client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

# ------------------------------
# FUNCIONES AUXILIARES
# ------------------------------
def get_completion(prompt):
    try:
        completion = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": "Eres un asistente experto en redacción y clasificación de productos. Responde siempre en español, sin comillas innecesarias."},
                {"role": "user", "content": prompt}
            ]
        )
        return completion.choices[0].message.content.strip().replace('"', '').replace("'", "")
    except Exception as e:
        print(f"[ERROR] OpenAI: {e}")
        return ""

def seo_friendly_filename(text):
    text = unicodedata.normalize('NFKD', text).encode('ascii', 'ignore').decode('ascii')
    text = text.lower()
    text = re.sub(r'[^a-z0-9]+', '-', text)
    return text.strip('-')[:80]

def generate_shopify_handle(title, asin):
    product_name = unidecode(title or f"FALTA-TITULO-{asin}")
    cleaned_name = re.sub(r'[^a-zA-Z0-9\s]', '', product_name).lower()
    return f"{'-'.join(cleaned_name.split())}-{asin}"

def get_existing_images_from_s3(asin):
    try:
        resp = _get_s3_client().list_objects_v2(Bucket=IMAGE_BUCKET, Prefix=f"{asin}/")
        if "Contents" in resp:
            urls = [f"https://{IMAGE_BUCKET}.s3.amazonaws.com/{obj['Key']}" for obj in resp["Contents"]]
            print(f"   ✅ Imágenes ya existen en S3 para {asin}: {len(urls)} encontradas")
            return urls
        return []
    except Exception as e:
        print(f"   ❌ Error comprobando imágenes en S3: {e}")
        return []

def download_and_upload_images(asin, imagenes):
    existing = get_existing_images_from_s3(asin)
    if existing:
        return existing

    urls_s3 = []

    for idx, img_url in enumerate(imagenes):
        try:
            nombre_img = f"{asin}_{idx+1}.jpg"
            s3_key = f"{asin}/{nombre_img}"

            response = requests.get(img_url, timeout=15)

            if response.status_code == 200:
                image_bytes = BytesIO(response.content)

                _get_s3_client().upload_fileobj(
                    image_bytes,
                    IMAGE_BUCKET,
                    s3_key,
                    ExtraArgs={'ContentType': 'image/jpeg'}
                )

                s3_url = f"https://{IMAGE_BUCKET}.s3.amazonaws.com/{s3_key}"
                print(f"   🖼️ Imagen subida a S3: {s3_url}")
                urls_s3.append(s3_url)

        except Exception as e:
            print(f"   ❌ Error subiendo imagen {img_url}: {e}")

    return urls_s3


def extract_product_image_urls(product):
    if not product:
        return []

    urls = []
    image_keys = {
        "additionalImages",
        "altImages",
        "colorImages",
        "gallery",
        "galleryImages",
        "hiRes",
        "highResolutionImages",
        "image",
        "imageLarge",
        "imageUrl",
        "imageUrls",
        "images",
        "landingImage",
        "large",
        "largeImage",
        "mainImage",
        "mainImageUrl",
        "manufacturerProductImages",
        "medium",
        "primaryImage",
        "thumb",
        "thumbnail",
        "variantImages",
        "variants",
        "zoom",
    }

    def add_url(value):
        if isinstance(value, str):
            value = value.strip()
            if not value.startswith("http"):
                return
            lower = value.lower()
            looks_like_image = any(
                marker in lower
                for marker in (
                    ".jpg",
                    ".jpeg",
                    ".png",
                    ".webp",
                    ".gif",
                    "images-na.ssl-images-amazon",
                    "m.media-amazon.com",
                    "ssl-images-amazon",
                    "media-amazon",
                )
            )
            if looks_like_image and value not in urls:
                urls.append(value)

    def add_many(value, depth=0, scan_branch=False):
        if depth > 5:
            return
        if isinstance(value, str):
            add_url(value)
        elif isinstance(value, dict):
            for key, nested in value.items():
                key_text = str(key)
                should_scan = scan_branch or key_text in image_keys or any(
                    marker in key_text.lower()
                    for marker in ("image", "img", "thumb", "large", "hires", "gallery", "variant")
                )
                if should_scan:
                    add_many(nested, depth + 1, scan_branch=True)
        elif isinstance(value, list):
            for item in value:
                add_many(item, depth + 1, scan_branch=scan_branch)

    for key in (
        "highResolutionImages",
        "images",
        "mainImage",
        "image",
        "imageUrl",
        "mainImageUrl",
        "largeImage",
        "thumbnail",
        "manufacturerProductImages",
        "imageUrls",
        "gallery",
        "galleryImages",
        "variantImages",
        "colorImages",
        "additionalImages",
        "altImages",
        "primaryImage",
        "landingImage",
    ):
        add_many(product.get(key), scan_branch=True)

    return urls


def get_product_price(product):
    if not product:
        return None
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


def product_has_needed_data(product, need_price=False, need_image=False):
    if need_price and get_product_price(product) is None:
        return False
    if need_image and not extract_product_image_urls(product):
        return False
    return True


def traducir_categoria(gl_key):
    gl_mapeo = {
        "gl_apparel": "Ropa", "gl_baby_product": "Productos para bebé", "gl_beauty": "Belleza",
        "gl_electronics": "Electrónica", "gl_furniture": "Muebles", "gl_home": "Hogar",
        "gl_home_improvement": "Mejoras del hogar", "gl_kitchen": "Cocina", "gl_lawn_and_garden": "Jardín y exteriores",
        "gl_luggage": "Equipaje", "gl_musical_instruments": "Instrumentos musicales", "gl_pet_products": "Productos para mascotas",
        "gl_shoes": "Calzado", "gl_sports": "Deportes y aire libre", "gl_tools": "Herramientas", "gl_wine": "Vinos",
        "gl_wireless": "Dispositivos inalámbricos / móviles"
    }
    return gl_mapeo.get(gl_key, None)

def generar_contenido_ia(titulo_original, descripcion_raw, caracteristicas_raw):

    prompt = f"""
Devuelve exclusivamente un JSON válido.

Producto:
Título original: {titulo_original}
Descripción original: {descripcion_raw}
Características:
{chr(10).join(caracteristicas_raw or [])}

Genera en español:

{{
  "titulo_amazon": "...",
  "titulo_breve": "...",
  "descripcion": "...",
  "caracteristicas": "...",
  "hashtags": "..."
}}
"""

    def llamada_openai():
        completion = client.chat.completions.create(
            model="gpt-4o-mini",
            response_format={"type": "json_object"},
            messages=[
                {"role": "system", "content": "Eres experto en redacción ecommerce. Devuelve solo JSON válido."},
                {"role": "user", "content": prompt}
            ]
        )
        return json.loads(completion.choices[0].message.content)

    try:
        # 🔹 Retry aplicado aquí
        return retry(llamada_openai, retries=3, delay=2)

    except Exception as e:
        print(f"[ERROR OpenAI tras reintentos]: {e}")
        return {}

def retry(func, retries=3, delay=2):
    """
    Ejecuta una función con reintentos exponenciales simples.
    """
    for attempt in range(retries):
        try:
            return func()
        except Exception as e:
            if attempt == retries - 1:
                raise
            print(f"⚠️ Reintento {attempt+1}/{retries} tras error: {e}")
            time.sleep(delay * (attempt + 1))

def normalize_for_mysql(value):
    if isinstance(value, list):
        return " | ".join(str(v) for v in value)
    if isinstance(value, dict):
        return json.dumps(value, ensure_ascii=False)
    return value


def normalize_decimal(value):
    if value is None:
        return None
    if isinstance(value, Decimal):
        return value
    if isinstance(value, (int, float)):
        return Decimal(str(value))

    text = str(value).strip()
    if not text or text.lower() in ("none", "nan", "null"):
        return None

    match = re.search(r"-?\d+(?:[.,]\d+)?", text)
    if not match:
        return None

    try:
        return Decimal(match.group(0).replace(",", "."))
    except InvalidOperation:
        return None


def truncate_text(value, max_len):
    if value is None:
        return None
    value = str(value).strip()
    if len(value) <= max_len:
        return value
    return value[:max_len].rstrip()


def truncate_handle(value, asin, max_len=150):
    value = truncate_text(value, max_len)
    if not value or not asin or value.endswith(asin):
        return value

    suffix = f"-{asin}"
    if len(suffix) >= max_len:
        return value[:max_len]
    return f"{value[:max_len - len(suffix)].rstrip('-')}{suffix}"


def prepare_scraped_data_for_mysql(data):
    data = {k: normalize_for_mysql(v) for k, v in data.items()}

    limits = {
        "categoria": 100,
        "scraping_domain": 20,
        "marca": 100,
        "dimensiones": 100,
        "titulo_breve": 50,
        "hashtags": 255,
        "vendor": 100,
        "seo_title": 100,
        "seo_description": 200,
    }
    for key, limit in limits.items():
        data[key] = truncate_text(data.get(key), limit)

    data["handle"] = truncate_handle(data.get("handle"), data.get("asin"), 150)

    for key in ("precio", "precio_coste", "precio_amazon", "rate", "peso", "peso_amazon"):
        data[key] = normalize_decimal(data.get(key))

    return data

# ------------------------------
# SCRAPING
# ------------------------------
def intentar_scraping(asin, need_price=False, need_image=False, dominios=None):
    dominios = dominios or ["es", "de", "fr", "it", "com", "com.be", "co.uk", "ca", "nl", "pl", "se"]
    best_product = None
    best_domain = None

    for dominio in dominios:
        try:
            print(f"🌍 Intentando dominio .{dominio} para ASIN {asin}", flush=True)

            def llamada_crawlbase():
                url_amazon = f"https://www.amazon.{dominio}/dp/{asin}"
                encoded_url = quote_plus(url_amazon)
                crawlbase_url = (
                    f"https://api.crawlbase.com/"
                    f"?token={CRAWLBASE_TOKEN}"
                    f"&scraper=amazon-product-details"
                    f"&url={encoded_url}"
                )

                response = urlopen(crawlbase_url, timeout=20).read().decode('utf-8')
                return json.loads(response)

            # 🔹 Retry aplicado aquí
            data = retry(llamada_crawlbase, retries=3, delay=2)

            status = data.get("status")
            if status and status != 200:
                print(f"⚠️ Crawlbase status {status} para {asin} en .{dominio}")
                continue

            body = data.get("body")

            if isinstance(body, str):
                try:
                    body = json.loads(body)
                except json.JSONDecodeError:
                    print(f"❌ body no es JSON válido para {asin} en .{dominio}")
                    continue

            if not isinstance(body, dict):
                continue

            product = body
            name = product.get("name")

            if isinstance(name, str) and name.strip():
                if best_product is None:
                    best_product = product
                    best_domain = dominio
                if product_has_needed_data(product, need_price=need_price, need_image=need_image):
                    return product, dominio

        except Exception as e:
            print(f"❌ Fallo scraping {asin} en dominio .{dominio}: {e}")
            continue

    return best_product, best_domain

# ------------------------------
# FUENTE DE DATOS (box_items + boxes + amazon_delivery)
# ------------------------------
def get_asins_para_procesar(limit=None):
    conn = None
    cursor = None
    try:
        conn = get_connection()
        cursor = conn.cursor(dictionary=True)
        query = """
            SELECT DISTINCT bi.asin, d.ItemDesc, d.UnitCost, d.UnitRecovery, d.RecoveryRate,
                            d.ItemPkgWeight, d.GLDesc
            FROM box_items bi
            JOIN boxes b ON bi.box_code = b.code
            LEFT JOIN amazon_delivery d ON bi.asin = d.Asin
            WHERE b.status IN ('Disponible','Reservado')
              AND bi.asin IS NOT NULL AND bi.asin <> ''
              AND NOT EXISTS (SELECT 1 FROM amazon_scraped_products asp WHERE asp.asin = bi.asin)
            ORDER BY bi.asin
        """
        if limit:
            query += f"\nLIMIT {int(limit)}"
        cursor.execute(query)
        return cursor.fetchall()
    except Exception as e:
        print(f"[ERROR] get_asins_para_procesar: {e}")
        return []
    finally:
        if cursor:
            cursor.close()
        if conn and conn.is_connected():
            conn.close()

# ------------------------------
# INSERCIÓN EN DB
# ------------------------------
def insertar_scraped_data(data, cursor):

    insert_query = """
    INSERT INTO amazon_scraped_products (
        asin,
        scraping_domain,
        categoria,
        titulo_amazon,
        marca,
        precio,
        precio_coste,
        precio_amazon,
        rate,
        dimensiones,
        peso,
        peso_amazon,
        imagen_principal,
        imagenes_adicionales,
        caracteristicas,
        titulo_breve,
        descripcion,
        descripcion_tecnica,
        hashtags,
        handle,
        vendor,
        seo_title,
        seo_description,
        fecha_scraping
    ) VALUES (
        %(asin)s,
        %(scraping_domain)s,
        %(categoria)s,
        %(titulo_amazon)s,
        %(marca)s,
        %(precio)s,
        %(precio_coste)s,
        %(precio_amazon)s,
        %(rate)s,
        %(dimensiones)s,
        %(peso)s,
        %(peso_amazon)s,
        %(imagen_principal)s,
        %(imagenes_adicionales)s,
        %(caracteristicas)s,
        %(titulo_breve)s,
        %(descripcion)s,
        %(descripcion_tecnica)s,
        %(hashtags)s,
        %(handle)s,
        %(vendor)s,
        %(seo_title)s,
        %(seo_description)s,
        %(fecha_scraping)s
    )
    ON DUPLICATE KEY UPDATE
        scraping_domain      = VALUES(scraping_domain),
        categoria            = VALUES(categoria),
        titulo_amazon        = VALUES(titulo_amazon),
        marca                = VALUES(marca),
        precio               = VALUES(precio),
        precio_coste         = VALUES(precio_coste),
        precio_amazon        = VALUES(precio_amazon),
        rate                 = VALUES(rate),
        dimensiones          = VALUES(dimensiones),
        peso                 = VALUES(peso),
        peso_amazon          = VALUES(peso_amazon),
        imagen_principal     = VALUES(imagen_principal),
        imagenes_adicionales = VALUES(imagenes_adicionales),
        caracteristicas      = VALUES(caracteristicas),
        titulo_breve         = VALUES(titulo_breve),
        descripcion          = VALUES(descripcion),
        descripcion_tecnica  = VALUES(descripcion_tecnica),
        hashtags             = VALUES(hashtags),
        handle               = VALUES(handle),
        vendor               = VALUES(vendor),
        seo_title            = VALUES(seo_title),
        seo_description      = VALUES(seo_description),
        fecha_scraping       = VALUES(fecha_scraping)
    """

    cursor.execute(insert_query, data)


def actualizar_imagenes_producto(asin, urls_s3, cursor):
    if not urls_s3:
        return
    imagen_principal = urls_s3[0]
    imagenes_adicionales = ", ".join(urls_s3[1:]) if len(urls_s3) > 1 else None
    cursor.execute(
        """
        UPDATE amazon_scraped_products
        SET imagen_principal = %s,
            imagenes_adicionales = %s
        WHERE asin = %s
        """,
        (imagen_principal, imagenes_adicionales, asin),
    )


def actualizar_pvp_ud_desde_fuentes(conn):
    """
    Rellena box_items.pvp_ud para pallets disponibles/reservados.
    Prioridad:
      1) amazon_scraped_products.precio
      2) amazon_scraped_products.precio_amazon
      3) amazon_delivery.UnitRecovery (máximo por ASIN)
    Solo actualiza si pvp_ud está NULL o 0.
    """
    cur = conn.cursor()
    cur.execute("""
        UPDATE box_items bi
        JOIN boxes b ON b.code = bi.box_code
        LEFT JOIN amazon_scraped_products asp ON asp.asin = bi.asin
        LEFT JOIN (
            SELECT Asin, MAX(UnitRecovery) AS UnitRecovery
            FROM amazon_delivery
            GROUP BY Asin
        ) ad ON ad.Asin = bi.asin
        SET bi.pvp_ud = CASE
            WHEN TRIM(COALESCE(asp.precio, '')) REGEXP '^-?[0-9]+([,.][0-9]+)?$'
              THEN CAST(REPLACE(TRIM(asp.precio), ',', '.') AS DECIMAL(10,2))
            WHEN TRIM(COALESCE(asp.precio_amazon, '')) REGEXP '^-?[0-9]+([,.][0-9]+)?$'
              THEN CAST(REPLACE(TRIM(asp.precio_amazon), ',', '.') AS DECIMAL(10,2))
            WHEN ad.UnitRecovery IS NOT NULL AND ad.UnitRecovery > 0
              THEN ad.UnitRecovery
            ELSE bi.pvp_ud
        END
        WHERE b.status IN ('Disponible','Reservado')
          AND bi.asin IS NOT NULL AND bi.asin <> ''
          AND (bi.pvp_ud IS NULL OR bi.pvp_ud = 0)
    """)
    n = cur.rowcount
    cur.close()
    return n


def build_scraped_data_from_product(asin, record, product, dominio):
    scraping_domain = dominio or "es"

    titulo_original = product.get("name", "").strip()
    desc_raw = product.get("description", "") or ""
    caracteristicas_raw = product.get("features", []) or []

    contenido_ia = generar_contenido_ia(
        titulo_original,
        desc_raw,
        caracteristicas_raw
    )

    titulo_amazon = contenido_ia.get("titulo_amazon", titulo_original)
    titulo_breve = contenido_ia.get("titulo_breve", titulo_amazon)
    descripcion = contenido_ia.get("descripcion", desc_raw)
    caracteristicas = contenido_ia.get("caracteristicas", "")
    hashtags = contenido_ia.get("hashtags", "")

    raw_price = get_product_price(product)

    gl_value = product.get("gl") or record.get("GLDesc")
    categoria = traducir_categoria(gl_value)

    return {
        "asin": asin,
        "scraping_domain": scraping_domain,
        "categoria": categoria,
        "titulo_amazon": titulo_amazon,
        "marca": (product.get("brand") or "").strip(),
        "precio": raw_price,
        "precio_coste": record.get("UnitRecovery"),
        "precio_amazon": record.get("UnitCost"),
        "rate": record.get("RecoveryRate"),
        "dimensiones": None,
        "peso": None,
        "peso_amazon": record.get("ItemPkgWeight"),
        "imagen_principal": NO_IMAGE_URL,
        "imagenes_adicionales": None,
        "caracteristicas": caracteristicas,
        "titulo_breve": titulo_breve,
        "descripcion": descripcion,
        "descripcion_tecnica": caracteristicas,
        "hashtags": hashtags,
        "handle": generate_shopify_handle(titulo_amazon, asin),
        "vendor": "BestCash",
        "seo_title": titulo_amazon,
        "seo_description": descripcion,
        "fecha_scraping": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
    }


def build_scraped_data_from_delivery(asin, record):
    item_desc = record.get("ItemDesc") or asin

    contenido_ia = generar_contenido_ia(
        item_desc,
        item_desc,
        []
    )

    titulo_amazon = contenido_ia.get("titulo_amazon", item_desc)
    titulo_breve = contenido_ia.get("titulo_breve", titulo_amazon)
    descripcion = contenido_ia.get("descripcion", item_desc)
    hashtags = contenido_ia.get("hashtags", "")

    return {
        "asin": asin,
        "scraping_domain": "es",
        "categoria": traducir_categoria(record.get("GLDesc")),
        "titulo_amazon": titulo_amazon,
        "marca": None,
        "precio": None,
        "precio_coste": record.get("UnitRecovery"),
        "precio_amazon": record.get("UnitCost"),
        "rate": record.get("RecoveryRate"),
        "dimensiones": None,
        "peso": None,
        "peso_amazon": record.get("ItemPkgWeight"),
        "imagen_principal": NO_IMAGE_URL,
        "imagenes_adicionales": None,
        "caracteristicas": None,
        "titulo_breve": titulo_breve,
        "descripcion": descripcion,
        "descripcion_tecnica": item_desc,
        "hashtags": hashtags,
        "handle": generate_shopify_handle(titulo_amazon, asin),
        "vendor": "BestCash",
        "seo_title": titulo_amazon,
        "seo_description": descripcion,
        "fecha_scraping": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
    }


def run_enrich(
    limit=None,
    skip_images=False,
    sleep_seconds=0.5,
    only_pvp_update=False,
    update_pvp=True,
):
    try:
        sys.stdout.reconfigure(line_buffering=True)
    except Exception:
        pass

    conn = get_connection()
    if only_pvp_update:
        try:
            print(
                "\n📊 Actualizando box_items.pvp_ud (sin scraping/IA)…",
                flush=True,
            )
            t_pvp = time.perf_counter()
            nrows = actualizar_pvp_ud_desde_fuentes(conn)
            conn.commit()
            print(
                f"✅ pvp_ud aplicado en box_items | filas afectadas (rowcount MySQL): {nrows} "
                f"| {time.perf_counter() - t_pvp:.1f}s",
                flush=True,
            )
        except Exception as e:
            conn.rollback()
            print(f"❌ Error actualizando pvp_ud: {e}", flush=True)
            raise
        finally:
            conn.close()
        return

    print(
        "📡 Buscando ASIN de cajas Disponible/Reservado que aún no están en amazon_scraped_products...",
        flush=True,
    )
    t_query = time.perf_counter()
    registros = get_asins_para_procesar(limit=limit)
    print(
        f"🔍 ASINs a enriquecer (scraping/IA): {len(registros)} "
        f"({time.perf_counter() - t_query:.1f}s)",
        flush=True,
    )
    if not registros:
        print(
            "ℹ️  Nada que insertar en amazon_scraped_products: todos esos ASIN ya tienen ficha.",
            flush=True,
        )

    cursor = conn.cursor()
    ok = 0
    errors = 0

    try:
        for i, record in enumerate(registros, start=1):
            asin = record["asin"]
            print(f"\n🔄 [{i}/{len(registros)}] Procesando {asin}", flush=True)

            try:
                product, dominio = intentar_scraping(
                    asin,
                    need_price=True,
                    need_image=not skip_images,
                )

                # ======================================================
                # CASO 1: SCRAPING DISPONIBLE
                # ======================================================
                if product:
                    print(f"✅ Scraping encontrado en .{dominio}", flush=True)
                    data = build_scraped_data_from_product(asin, record, product, dominio)

                    print("➡️ Datos obtenidos por SCRAPING", flush=True)

                # ======================================================
                # CASO 2: FALLBACK AMAZON_DELIVERY
                # ======================================================
                else:
                    print("⚠️ Scraping no disponible, usando fallback amazon_delivery", flush=True)
                    data = build_scraped_data_from_delivery(asin, record)

                    print("➡️ Datos obtenidos por DELIVERY", flush=True)

                # 🔹 Normalización robusta antes de insertar
                data = prepare_scraped_data_for_mysql(data)

                print(f"   💾 Insertando en amazon_scraped_products…", flush=True)
                insertar_scraped_data(data, cursor)
                conn.commit()
                print(f"   ✅ Guardado {asin}", flush=True)

                if product and not skip_images:
                    imagenes = extract_product_image_urls(product)
                    urls_s3 = download_and_upload_images(asin, imagenes)
                    if urls_s3:
                        actualizar_imagenes_producto(asin, urls_s3, cursor)
                        conn.commit()
                        print(f"   ✅ Imágenes enlazadas en BD para {asin}: {len(urls_s3)}", flush=True)

                ok += 1
                time.sleep(sleep_seconds)

            except Exception as e:
                conn.rollback()
                errors += 1
                print(f"❌ Error procesando {asin}: {e}", flush=True)

        if update_pvp:
            # Rellenar pvp_ud usando scraping y/o UnitRecovery como fallback
            try:
                print(
                    "\n📊 Actualizando box_items.pvp_ud (UPDATE masivo con JOINs; puede tardar varios minutos)…",
                    flush=True,
                )
                t_pvp = time.perf_counter()
                nrows = actualizar_pvp_ud_desde_fuentes(conn)
                conn.commit()
                print(
                    f"✅ pvp_ud aplicado en box_items | filas afectadas (rowcount MySQL): {nrows} "
                    f"| {time.perf_counter() - t_pvp:.1f}s",
                    flush=True,
                )
            except Exception as e:
                conn.rollback()
                print(f"❌ Error actualizando pvp_ud: {e}", flush=True)
        else:
            print("\nℹ️  Saltando actualización masiva de pvp_ud (--skip-pvp-update).", flush=True)

    finally:
        cursor.close()
        conn.close()

    print(
        f"\n📌 Resumen enrich: guardados={ok}, errores={errors}, solicitados={len(registros)}",
        flush=True,
    )


# ------------------------------
# MAIN
# ------------------------------
if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Enriquece ASIN pendientes y actualiza pvp_ud para lotes mayoristas."
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Procesa como máximo N ASIN pendientes en esta ejecución.",
    )
    parser.add_argument(
        "--skip-images",
        action="store_true",
        help="Guarda fichas sin descargar/subir imágenes a S3.",
    )
    parser.add_argument(
        "--sleep",
        type=float,
        default=0.5,
        help="Segundos de pausa entre ASIN procesados.",
    )
    parser.add_argument(
        "--only-pvp-update",
        action="store_true",
        help="No scrapea; solo rellena box_items.pvp_ud desde fuentes disponibles.",
    )
    parser.add_argument(
        "--skip-pvp-update",
        action="store_true",
        help="No ejecuta el UPDATE masivo de box_items.pvp_ud al final.",
    )
    args = parser.parse_args()
    run_enrich(
        limit=args.limit,
        skip_images=args.skip_images,
        sleep_seconds=args.sleep,
        only_pvp_update=args.only_pvp_update,
        update_pvp=not args.skip_pvp_update,
    )
