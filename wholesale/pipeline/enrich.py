import os
import re
import json
import unicodedata
import requests
import mysql.connector
from urllib.request import urlopen
from urllib.parse import quote_plus
from datetime import datetime
from dotenv import load_dotenv, find_dotenv
import sys

# asegurar que el directorio raíz del proyecto está en sys.path
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if BASE_DIR not in sys.path:
    sys.path.append(BASE_DIR)

from db import get_connection
from unidecode import unidecode
from openai import OpenAI
import boto3
from botocore.exceptions import ClientError
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

# Configuración S3
session = boto3.Session(profile_name='bestcash')
s3 = session.client('s3')

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
        resp = s3.list_objects_v2(Bucket=IMAGE_BUCKET, Prefix=f"{asin}/")
        if "Contents" in resp:
            urls = [f"https://{IMAGE_BUCKET}.s3.amazonaws.com/{obj['Key']}" for obj in resp["Contents"]]
            print(f"   ✅ Imágenes ya existen en S3 para {asin}: {len(urls)} encontradas")
            return urls
        return []
    except Exception as e:
        print(f"   ❌ Error comprobando imágenes en S3: {e}")
        return []

def download_and_upload_images(asin, imagenes, titulo_amazon):
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

                s3.upload_fileobj(
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

# ------------------------------
# SCRAPING
# ------------------------------
def intentar_scraping(asin):
    dominios = ["es", "de", "fr", "it", "com", "com.be", "co.uk", "ca", "nl", "pl", "se"]

    for dominio in dominios:
        try:
            print(f"🌍 Intentando dominio .{dominio} para ASIN {asin}")

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
                return product, dominio

        except Exception as e:
            print(f"❌ Fallo scraping {asin} en dominio .{dominio}: {e}")
            continue

    return None, None

# ------------------------------
# FUENTE DE DATOS (box_items + boxes + amazon_delivery)
# ------------------------------
def get_asins_para_procesar():
    try:
        conn = get_connection()
        cursor = conn.cursor(dictionary=True)
        cursor.execute("""
            SELECT DISTINCT bi.asin, d.ItemDesc, d.UnitCost, d.UnitRecovery, d.RecoveryRate,
                            d.ItemPkgWeight, d.GLDesc
            FROM box_items bi
            JOIN boxes b ON bi.box_code = b.code
            LEFT JOIN amazon_delivery d ON bi.asin = d.Asin
            WHERE b.status IN ('Disponible','Reservado')
              AND bi.asin IS NOT NULL AND bi.asin <> ''
              AND NOT EXISTS (SELECT 1 FROM amazon_scraped_products asp WHERE asp.asin = bi.asin)
        """)
        return cursor.fetchall()
    except Exception as e:
        print(f"[ERROR] get_asins_para_procesar: {e}")
        return []
    finally:
        if conn.is_connected():
            cursor.close()
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
            WHEN asp.precio IS NOT NULL AND asp.precio <> '' AND asp.precio <> 'None'
              THEN CAST(REPLACE(asp.precio, ',', '.') AS DECIMAL(10,2))
            WHEN asp.precio_amazon IS NOT NULL AND asp.precio_amazon <> '' AND asp.precio_amazon <> 'None'
              THEN CAST(REPLACE(asp.precio_amazon, ',', '.') AS DECIMAL(10,2))
            WHEN ad.UnitRecovery IS NOT NULL AND ad.UnitRecovery > 0
              THEN ad.UnitRecovery
            ELSE bi.pvp_ud
        END
        WHERE b.status IN ('Disponible','Reservado')
          AND bi.asin IS NOT NULL AND bi.asin <> ''
          AND (bi.pvp_ud IS NULL OR bi.pvp_ud = 0)
    """)
    cur.close()


# ------------------------------
# MAIN
# ------------------------------
if __name__ == "__main__":
    registros = get_asins_para_procesar()
    print(f"🔍 ASINs a procesar: {len(registros)}")

    conn = get_connection()
    cursor = conn.cursor()

    try:
        for i, record in enumerate(registros, start=1):
            asin = record["asin"]
            print(f"\n🔄 [{i}/{len(registros)}] Procesando {asin}")

            try:
                product, dominio = intentar_scraping(asin)

                # ======================================================
                # CASO 1: SCRAPING DISPONIBLE
                # ======================================================
                if product:
                    print(f"✅ Scraping encontrado en .{dominio}")
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

                    imagenes = product.get("highResolutionImages") or product.get("images") or []
                    urls_s3 = download_and_upload_images(asin, imagenes, titulo_amazon)

                    raw_price = product.get("rawPrice")

                    gl_value = product.get("gl") or record.get("GLDesc")
                    categoria = traducir_categoria(gl_value)

                    data = {
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
                        "imagen_principal": urls_s3[0] if urls_s3 else NO_IMAGE_URL,
                        "imagenes_adicionales": ", ".join(urls_s3[1:]) if len(urls_s3) > 1 else None,
                        "caracteristicas": caracteristicas,
                        "titulo_breve": titulo_breve,
                        "descripcion": descripcion,
                        "descripcion_tecnica": caracteristicas,
                        "hashtags": hashtags,
                        "handle": generate_shopify_handle(titulo_amazon, asin),
                        "vendor": "BestCash",
                        "seo_title": titulo_amazon,
                        "seo_description": descripcion,
                        "fecha_scraping": datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    }

                    print("➡️ Datos obtenidos por SCRAPING")

                # ======================================================
                # CASO 2: FALLBACK AMAZON_DELIVERY
                # ======================================================
                else:
                    print("⚠️ Scraping no disponible, usando fallback amazon_delivery")

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

                    data = {
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
                        "fecha_scraping": datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    }

                    print("➡️ Datos obtenidos por DELIVERY")

                # 🔹 Normalización robusta antes de insertar
                data = {k: normalize_for_mysql(v) for k, v in data.items()}

                insertar_scraped_data(data, cursor)
                conn.commit()

                time.sleep(0.5)

            except Exception as e:
                conn.rollback()
                print(f"❌ Error procesando {asin}: {e}")

        # Rellenar pvp_ud usando scraping y/o UnitRecovery como fallback
        try:
            actualizar_pvp_ud_desde_fuentes(conn)
            conn.commit()
            print("✅ pvp_ud actualizado en box_items (incluyendo fallback UnitRecovery)")
        except Exception as e:
            conn.rollback()
            print(f"❌ Error actualizando pvp_ud: {e}")

    finally:
        cursor.close()
        conn.close()