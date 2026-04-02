import os
import re
import mysql.connector
from unidecode import unidecode
from dotenv import load_dotenv, find_dotenv

# Cargar variables de entorno
load_dotenv(find_dotenv())

# Configuración de la base de datos
db_config = {
    'user': os.getenv('DB_USER', 'admin'),
    'password': os.getenv('DB_PASSWORD', '23092023BCdb'),
    'host': os.getenv('DB_HOST', 'bestcash.cvsxzrox0hah.eu-west-1.rds.amazonaws.com'),
    'database': os.getenv('DB_NAME', 'bestcash_rds')
}

def generate_shopify_handle(title, asin):
    if not title:
        return f"FALTA-TITULO-{asin}"
    product_name = unidecode(title)
    cleaned_name = re.sub(r'[^a-zA-Z0-9\s]', '', product_name).lower()
    handle = '-'.join(cleaned_name.split())
    return f"{handle}-{asin}"

try:
    conn = mysql.connector.connect(**db_config)
    cursor = conn.cursor(dictionary=True)

    # Leer los datos actuales
    cursor.execute("SELECT asin, titulo_breve, handle FROM amazon_scraped_products")
    rows = cursor.fetchall()

    actualizados = 0
    total = len(rows)
    print(f"🔍 Procesando {total} registros...\n")

    for i, row in enumerate(rows, start=1):
        asin = row['asin']
        titulo_breve = row['titulo_breve']
        handle_actual = row['handle']

        if not titulo_breve:
            print(f"⏭ {i}/{total} - ASIN: {asin} sin título_breve. Se omite.")
            continue

        nuevo_handle = generate_shopify_handle(titulo_breve, asin)

        if nuevo_handle != handle_actual:
            print(f"🔁 {i}/{total} - ASIN: {asin} | {handle_actual} → {nuevo_handle}")
            cursor.execute(
                "UPDATE amazon_scraped_products SET handle = %s WHERE asin = %s",
                (nuevo_handle, asin)
            )
            conn.commit()
            actualizados += 1
        else:
            print(f"✅ {i}/{total} - ASIN: {asin} ya correcto.")

    print(f"\n🎯 Proceso completado: {actualizados} registros actualizados.")

except mysql.connector.Error as err:
    print(f"[ERROR] Problema con la base de datos: {err}")
finally:
    if conn.is_connected():
        cursor.close()
        conn.close()
