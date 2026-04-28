import csv
import json
import os
import re
from urllib.parse import quote_plus
from urllib.request import urlopen

import mysql.connector
from mysql.connector import Error
from openai import OpenAI
import sys

# Configuración directa de la base de datos
destination_db_config = {
    'user': 'admin',
    'password': '23092023BCdb',
    'host': 'bestcash.cvsxzrox0hah.eu-west-1.rds.amazonaws.com',
    'database': 'bestcash_rds'
}

client = OpenAI(api_key=os.environ.get("OPENAI_API_KEY"))

def get_title_summary(titulo_full):
    prompt = f'De "{titulo_full}" genera un título máximo 30 caracteres. Escribe solo el título'
    try:
        completion = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": "Eres un asistente informativo."},
                {"role": "user", "content": prompt}
            ]
        )
        return completion.choices[0].message.content.strip()
    except Exception as e:
        print(f"Error con OpenAI resumen: {e}")
        return titulo_full

def translate_to_spanish(text):
    prompt = f'Traduce al español el siguiente texto conservando el significado:\n"{text}"'
    try:
        completion = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": "Eres un traductor experto al español."},
                {"role": "user", "content": prompt}
            ]
        )
        return completion.choices[0].message.content.strip()
    except Exception as e:
        print(f"Error con OpenAI traducción: {e}")
        return text

def scrape_amazon_data_multi(asin):
    domains = ['es', 'de', 'fr', 'it', 'com', 'nl', 'com.be','pl', 'ae']
    for domain in domains:
        try:
            print(f"🌍 Intentando dominio .{domain} para ASIN {asin}")
            url = f'https://www.amazon.{domain}/dp/{asin}'
            encoded_url = quote_plus(url)
            api_url = f'https://api.crawlbase.com/?token=9a_E5QjtbAz2sAbVt2U3vQ&scraper=amazon-product-details&url={encoded_url}'
            response = urlopen(api_url).read().decode('utf-8')
            body = json.loads(response).get('body', {})
            product_information = body.get('productInformation', [])

            if not product_information or 'name' not in body:
                continue

            title = body.get('name', 'N/A')
            raw_price = body.get('rawPrice', 'N/A')

            dim_keys = [
                'Dimensiones del producto', 'Dimensiones del paquete',
                'Dimensiones Artículo', 'Dimensiones del artículo (profundidad x ancho x alto)',
                'Dimensiones del producto: largo x ancho x alto'
            ]
            weight_keys = ['Peso del producto', 'Recomendación de peso máximo', 'Peso del artículo']

            dimensions = 'N/A'
            weight = 'N/A'

            for item in product_information:
                name = re.sub(r'\s|\u200f', '', item['name']).lower()
                value = item['value']
                if any(re.sub(r'\s|\u200f', '', k).lower() == name for k in dim_keys):
                    if ';' in value:
                        dimensions, weight = map(str.strip, value.split(';'))
                        break
                    dimensions = value
                elif name in [re.sub(r'\s|\u200f', '', k).lower() for k in weight_keys]:
                    weight = value

            if weight != 'N/A':
                w_clean = re.sub(r'[^\d.,]', '', weight).replace(',', '.')
                try:
                    weight_g = float(w_clean) * 1000 if 'kg' in weight.lower() else float(w_clean)
                except:
                    weight_g = 'N/A'
            else:
                weight_g = 'N/A'

            return {
                'asin': asin,
                'titulo_full': title,
                'raw_price': raw_price,
                'dimensions': dimensions,
                'weight': weight_g
            }

        except Exception as e:
            print(f"❌ Error scraping {asin} en .{domain}: {e}")
            continue
    return None

def insert_product(cursor, connection, asin, title, price):
    cursor.execute("""
        INSERT INTO products_info (asin, title, price_amz, category_id, category_tpv, image_s3, tax)
        VALUES (%s, %s, %s, 0, 0, 0, 21)
    """, (asin, title, price))
    connection.commit()
    return cursor.lastrowid

def insert_reference(cursor, connection, asin, product_id):
    cursor.execute("INSERT INTO references_info (fcsku, product_id) VALUES (%s, %s)", (asin, product_id))
    connection.commit()
    return cursor.lastrowid

def insert_item(cursor, connection, code, reference_id, shop_id, price, ok_online):
    cursor.execute("""
        INSERT INTO items_info (code, reception_id, reference_id, shop_id, bestcash_price, is_many, ok_online)
        VALUES (%s, NULL, %s, %s, %s, 0, %s)
    """, (code, reference_id, shop_id, price, ok_online))
    connection.commit()

def main():
   csv_entrada = sys.argv[1]
    scraping_csv = 'amazon_info.csv'
    asin_procesados = set()

    connection = mysql.connector.connect(**destination_db_config)
    cursor = connection.cursor(dictionary=True, buffered=True)
    print("✅ Conectado a la base de datos")

    with open(csv_entrada, 'r', encoding='utf-8') as f:
        rows = list(csv.DictReader(f))

    with open(scraping_csv, 'a', newline='', encoding='utf-8') as out_csv:
        writer = csv.writer(out_csv)
        if out_csv.tell() == 0:
            writer.writerow(['ASIN', 'Título Completo', 'Raw Price', 'Dimensions', 'Weight (grams)'])

        for idx, row in enumerate(rows, 1):
            try:
                asin = row['asin'].strip()
                code = str(row['item']).zfill(8)
                price = float(row['precio'].replace(',', '.'))
                shop_id = int(row['tienda'].strip())
                ok_online = int(row.get('ok_online', '0').strip())

                print(f"\n📦 {idx}/{len(rows)} - ASIN: {asin} | Code: {code}")

                cursor.execute("SELECT id FROM products_info WHERE asin = %s", (asin,))
                product = cursor.fetchone()
                while cursor.nextset(): pass

                if product:
                    product_id = product['id']
                    print(f"🔁 Producto existente (ID: {product_id})")
                else:
                    datos = scrape_amazon_data_multi(asin)
                    if not datos:
                        print(f"⚠️ Scraping fallido para {asin}. Insertando como 'DESCONOCIDO'.")
                        titulo_es = "DESCONOCIDO"
                        titulo_res = "DESCONOCIDO"
                        raw_price = 'N/A'
                        dimensions = 'N/A'
                        weight = 'N/A'
                    else:
                        titulo_es = translate_to_spanish(datos['titulo_full'])
                        titulo_res = get_title_summary(titulo_es)
                        raw_price = datos['raw_price']
                        dimensions = datos['dimensions']
                        weight = datos['weight']

                    product_id = insert_product(cursor, connection, asin, titulo_res, price)
                    print(f"🆕 Producto insertado con ID {product_id}")

                    with open(scraping_csv, 'a', newline='', encoding='utf-8') as out_csv:
                        writer = csv.writer(out_csv)
                        writer.writerow([asin, titulo_es, raw_price, dimensions, weight])


                cursor.execute("SELECT id FROM references_info WHERE product_id = %s", (product_id,))
                ref = cursor.fetchone()
                while cursor.nextset(): pass

                if ref:
                    ref_id = ref['id']
                else:
                    ref_id = insert_reference(cursor, connection, asin, product_id)
                    print(f"🔗 Referencia creada ID {ref_id}")

                cursor.execute("SELECT id FROM items_info WHERE code = %s", (code,))
                exists = cursor.fetchone()
                while cursor.nextset(): pass

                if exists:
                    print(f"⚠️ Code {code} ya existe. Saltando.")
                    continue

                insert_item(cursor, connection, code, ref_id, shop_id, price, ok_online)
                print(f"✅ Item insertado: {code}")
                asin_procesados.add(asin)

            except Exception as e:
                print(f"❌ Error en fila {idx} (ASIN: {row.get('asin', 'N/A')}): {e}")

    connection.close()
    print("🔒 Conexión cerrada")

if __name__ == "__main__":
    main()
