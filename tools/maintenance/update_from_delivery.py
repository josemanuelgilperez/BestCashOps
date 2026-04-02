import os
import csv
import html
import re
import json
import unicodedata
import mysql.connector
from openai import OpenAI
from dotenv import load_dotenv, find_dotenv
from datetime import datetime
from unidecode import unidecode

# -----------------------------------
# CARGA DE VARIABLES DE ENTORNO
# -----------------------------------
load_dotenv(find_dotenv())

db_config = {
    'user': os.getenv('DB_USER', 'admin'),
    'password': os.getenv('DB_PASSWORD', '23092023BCdb'),
    'host': os.getenv('DB_HOST', 'bestcash.cvsxzrox0hah.eu-west-1.rds.amazonaws.com'),
    'database': os.getenv('DB_NAME', 'bestcash_rds')
}

client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

# -----------------------------------
# FUNCIONES AUXILIARES
# -----------------------------------
def sanitize_text(text):
    if not text:
        return None
    text = html.unescape(text)
    text = text.replace('"', '').replace("'", "")
    return text.strip()

def traducir_categoria(gl_key):
    gl_mapeo = {
        "gl_apparel": "Ropa", "gl_baby_product": "Productos para bebé", "gl_beauty": "Belleza",
        "gl_electronics": "Electrónica", "gl_furniture": "Muebles", "gl_home": "Hogar",
        "gl_home_improvement": "Mejoras del hogar", "gl_kitchen": "Cocina", "gl_lawn_and_garden": "Jardín y exteriores",
        "gl_luggage": "Equipaje", "gl_musical_instruments": "Instrumentos musicales", "gl_pet_products": "Productos para mascotas",
        "gl_shoes": "Calzado", "gl_sports": "Deportes y aire libre", "gl_tools": "Herramientas", "gl_wine": "Vinos",
        "gl_wireless": "Dispositivos móviles / inalámbricos"
    }
    return gl_mapeo.get(gl_key, "N/A")

def generate_shopify_handle(title, asin):
    product_name = unidecode(title or f"producto-{asin}")
    cleaned_name = re.sub(r'[^a-zA-Z0-9\s]', '', product_name).lower()
    return f"{'-'.join(cleaned_name.split())}-{asin}"

def get_completion(prompt):
    try:
        completion = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": "Eres un redactor experto en productos de ecommerce. Responde SIEMPRE en español, sin comillas innecesarias."},
                {"role": "user", "content": prompt}
            ]
        )
        return sanitize_text(completion.choices[0].message.content.strip())
    except Exception as e:
        print(f"[ERROR OpenAI]: {e}")
        return ""

# -----------------------------------
# BASE DE DATOS
# -----------------------------------
def get_data_from_delivery(asin):
    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor(dictionary=True)
        cursor.execute("""
            SELECT Asin, ItemDesc, UnitCost, UnitRecovery, RecoveryRate, ItemPkgWeight, GLDesc
            FROM amazon_delivery
            WHERE Asin = %s
            LIMIT 1
        """, (asin,))
        return cursor.fetchone()
    except Exception as e:
        print(f"[ERROR delivery {asin}]: {e}")
        return None
    finally:
        if conn.is_connected():
            cursor.close()
            conn.close()

def update_scraped_data(data):
    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor()
        query = """
        INSERT INTO amazon_scraped_products (
            asin, categoria, titulo_amazon, precio_coste, precio_amazon,
            rate, peso_amazon, titulo_breve, descripcion, descripcion_tecnica,
            hashtags, handle, vendor, seo_title, seo_description, fecha_scraping
        ) VALUES (
            %(asin)s, %(categoria)s, %(titulo_amazon)s, %(precio_coste)s, %(precio_amazon)s,
            %(rate)s, %(peso_amazon)s, %(titulo_breve)s, %(descripcion)s, %(descripcion_tecnica)s,
            %(hashtags)s, %(handle)s, %(vendor)s, %(seo_title)s, %(seo_description)s, %(fecha_scraping)s
        )
        ON DUPLICATE KEY UPDATE
            categoria=VALUES(categoria),
            titulo_amazon=VALUES(titulo_amazon),
            precio_coste=VALUES(precio_coste),
            precio_amazon=VALUES(precio_amazon),
            rate=VALUES(rate),
            peso_amazon=VALUES(peso_amazon),
            titulo_breve=VALUES(titulo_breve),
            descripcion=VALUES(descripcion),
            descripcion_tecnica=VALUES(descripcion_tecnica),
            hashtags=VALUES(hashtags),
            handle=VALUES(handle),
            vendor=VALUES(vendor),
            seo_title=VALUES(seo_title),
            seo_description=VALUES(seo_description),
            fecha_scraping=VALUES(fecha_scraping)
        """
        cursor.execute(query, data)
        conn.commit()
        print(f"   💾 Guardado/actualizado {data['asin']} en amazon_scraped_products")
    except Exception as e:
        print(f"[ERROR DB {data['asin']}]: {e}")
    finally:
        if conn.is_connected():
            cursor.close()
            conn.close()

def marcar_como_delivery(asin):
    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor()
        cursor.execute("""
            UPDATE amazon_delivery
            SET scrape_status = 'delivery'
            WHERE Asin = %s
        """, (asin,))
        conn.commit()
        print(f"   🟢 ASIN {asin} marcado como 'delivery' en amazon_delivery")
    except Exception as e:
        print(f"[ERROR marcar {asin}]: {e}")
    finally:
        if conn.is_connected():
            cursor.close()
            conn.close()

# -----------------------------------
# MAIN
# -----------------------------------
if __name__ == "__main__":
    tools_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    asins_csv = os.path.join(tools_dir, "data", "asins.csv")
    with open(asins_csv, newline="") as f:
        reader = csv.reader(f)
        asin_list = [row[0].strip() for row in reader if row]

    print(f"🔍 ASINs cargados desde CSV: {len(asin_list)}\n")

    procesados, sin_datos, errores = 0, 0, 0

    for asin in asin_list:
        print(f"\n🔄 Procesando {asin}...")
        try:
            delivery = get_data_from_delivery(asin)
            if not delivery:
                print("   ⚠️ No se encontró información en amazon_delivery")
                sin_datos += 1
                continue

            item_desc = sanitize_text(delivery.get('ItemDesc')) or asin
            titulo_breve = get_completion(f"Redacta un título breve en español para este producto: {item_desc}")
            descripcion = get_completion(f"Redacta una descripción atractiva en español para: {item_desc}")
            hashtags = get_completion(f"Genera 5 hashtags en español para este producto: {item_desc}")

            data = {
                'asin': asin,
                'categoria': traducir_categoria(delivery.get('GLDesc')),
                'titulo_amazon': item_desc,
                'precio_coste': delivery.get('UnitRecovery'),
                'precio_amazon': delivery.get('UnitCost'),
                'rate': delivery.get('RecoveryRate'),
                'peso_amazon': delivery.get('ItemPkgWeight'),
                'titulo_breve': titulo_breve,
                'descripcion': descripcion,
                'descripcion_tecnica': item_desc,
                'hashtags': hashtags,
                'handle': generate_shopify_handle(titulo_breve, asin),
                'vendor': 'BestCash',
                'seo_title': titulo_breve,
                'seo_description': descripcion,
                'fecha_scraping': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            }

            update_scraped_data(data)
            marcar_como_delivery(asin)
            procesados += 1

        except Exception as e:
            print(f"❌ Error general {asin}: {e}")
            errores += 1

    print("\n🧾 Resumen final:")
    print(f"   Procesados correctamente: {procesados}")
    print(f"   Sin datos en delivery: {sin_datos}")
    print(f"   Errores: {errores}")
