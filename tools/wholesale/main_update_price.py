import os
import mysql.connector
import requests
import json
import time
import csv
import urllib.parse


# -----------------------------------
# CONFIGURACIÓN BASE DE DATOS
# -----------------------------------
DB_CONFIG = {
    'user': os.getenv('DB_USER', 'admin'),
    'password': os.getenv('DB_PASSWORD', '23092023BCdb'),
    'host': os.getenv('DB_HOST', 'bestcash.cvsxzrox0hah.eu-west-1.rds.amazonaws.com'),
    'database': os.getenv('DB_NAME', 'bestcash_rds')
}

# -----------------------------------
# CONFIGURACIÓN CRAWLBASE
# -----------------------------------
CRAWLBASE_TOKEN = os.getenv("CRAWLBASE_TOKEN", "YOUR_CRAWLBASE_TOKEN")

# -----------------------------------
# CARGAR PALLETS DESDE CSV (RESPETA ORDEN)
# -----------------------------------
def cargar_pallets_desde_csv(csv_file):
    pallets = []
    with open(csv_file, 'r', encoding='utf-8') as f:
        reader = csv.reader(f)
        for row in reader:
            if not row:
                continue
            code = row[0].strip().upper()
            if code and code != "code":  # evita encabezado
                if code not in pallets:  # evita duplicados manteniendo el orden
                    pallets.append(code)
    return pallets

# -----------------------------------
# OBTENER NOMBRES DE PALLETS
# -----------------------------------
def obtener_nombres_pallets(codes):
    if not codes:
        return {}

    conn = mysql.connector.connect(**DB_CONFIG)
    cur = conn.cursor()

    placeholders = ",".join(["%s"] * len(codes))
    query = f"SELECT code, name FROM boxes WHERE code IN ({placeholders})"

    cur.execute(query, codes)
    rows = cur.fetchall()

    cur.close()
    conn.close()

    nombres = {row[0]: row[1] for row in rows}
    return nombres

# -----------------------------------
# SCRAPING DE PRECIO
# -----------------------------------
def scrape_price(asin):
    """
    Scraping multidominio con Crawlbase (amazon-product-details).
    Prueba varios dominios hasta obtener un precio válido.
    Devuelve float con el precio o None si falla todo.
    """

    #dominios = ['es', 'de', 'fr', 'it', 'co.uk', 'com', 'nl', 'pl', 'com.be', 'ca', 'se']
    dominios = ['es', 'de', 'fr', 'it']
    for dominio in dominios:
        try:
            print(f"🌍 Intentando dominio .{dominio} para ASIN {asin}")

            url_amazon = f"https://www.amazon.{dominio}/dp/{asin}"
            encoded_url = urllib.parse.quote_plus(url_amazon)

            crawl_url = (
                f"https://api.crawlbase.com/?token={CRAWLBASE_TOKEN}"
                f"&scraper=amazon-product-details"
                f"&url={encoded_url}"
            )

            r = requests.get(crawl_url, timeout=15)

            # Si el status es distinto a 200, pasa al siguiente dominio
            if r.status_code != 200:
                print(f"   ❌ STATUS {r.status_code} en dominio .{dominio} → probando siguiente...")
                continue

            data = r.json()
            body = data.get("body", {})

            # -----------------------------------------
            # 1. Intentar rawPrice (clave principal)
            # -----------------------------------------
            raw = body.get("rawPrice")
            if raw:
                try:
                    price = round(float(str(raw).replace(",", ".")), 2)
                    print(f"   ✅ Precio encontrado en .{dominio}: {price}")
                    return price
                except:
                    pass

            # -----------------------------------------
            # 2. Intentar price.amount
            # -----------------------------------------
            if isinstance(body.get("price"), dict):
                amount = body["price"].get("amount")
                if amount:
                    try:
                        price = round(float(str(amount).replace(",", ".")), 2)
                        print(f"   ✅ Precio encontrado (amount) en .{dominio}: {price}")
                        return price
                    except:
                        pass

            # -----------------------------------------
            # 3. Intentar price.value
            # -----------------------------------------
            if isinstance(body.get("price"), dict):
                value = body["price"].get("value")
                if value:
                    try:
                        price = round(float(str(value).replace(",", ".")), 2)
                        print(f"   ✅ Precio encontrado (value) en .{dominio}: {price}")
                        return price
                    except:
                        pass

            print(f"   ⚠️ Sin precio válido en dominio .{dominio}, probando siguiente...")

        except Exception as e:
            print(f"   ❌ Error scraping .{dominio} para {asin}: {e}")
            continue

    print(f"   ❌ Ningún dominio devolvió precio para {asin}")
    return None


# -----------------------------------
# OBTENER ASINS DE UN PALLET
# -----------------------------------
def obtener_asins_de_pallets(pallet_codes):
    conn = mysql.connector.connect(**DB_CONFIG)
    cur = conn.cursor()

    placeholders = ",".join(["%s"] * len(pallet_codes))
    query = f"SELECT DISTINCT asin FROM box_items WHERE box_code IN ({placeholders})"

    cur.execute(query, pallet_codes)
    asins = [row[0] for row in cur.fetchall()]

    cur.close()
    conn.close()
    return asins

# -----------------------------------
# OBTENER UnitCost (CORREGIDO ORDER BY)
# -----------------------------------
def obtener_unitcost(asin):
    conn = mysql.connector.connect(**DB_CONFIG)
    cur = conn.cursor()

    cur.execute("""
        SELECT UnitCost
        FROM amazon_delivery
        WHERE asin = %s
        ORDER BY ShipmentClosed DESC
        LIMIT 1
    """, (asin,))

    row = cur.fetchone()

    cur.close()
    conn.close()

    if not row:
        return None

    try:
        return round(float(row[0]), 2)
    except:
        return None

# -----------------------------------
# ACTUALIZAR DB
# -----------------------------------
def actualizar_precios_db(asin, precio, precio_amazon):
    conn = mysql.connector.connect(**DB_CONFIG)
    cur = conn.cursor()

    sql = """
        UPDATE amazon_scraped_products
        SET precio = %s, precio_amazon = %s
        WHERE asin = %s
    """
    cur.execute(sql, (precio, precio_amazon, asin))
    conn.commit()

    cur.close()
    conn.close()

# -----------------------------------
# MAIN
# -----------------------------------
def main():
    CSV_FILE = "input_boxes.csv"
    print("TOKEN REAL:", CRAWLBASE_TOKEN)


    print(f"📄 Leyendo pallets desde CSV: {CSV_FILE}")
    pallets = cargar_pallets_desde_csv(CSV_FILE)

    if not pallets:
        print("❌ No hay pallets en el CSV. Abortando.")
        return

    # Nombres desde DB
    nombres_pallets = obtener_nombres_pallets(pallets)

    print("\n📦 Pallets que se van a procesar (orden CSV):")
    for code in pallets:
        nombre = nombres_pallets.get(code, "(sin nombre)")
        print(f"   - {code}: {nombre}")
    print(f"➡️ Total pallets: {len(pallets)}\n")

    # PROCESO PRINCIPAL
    for idx, pallet_code in enumerate(pallets, start=1):
        nombre = nombres_pallets.get(pallet_code, "(sin nombre)")

        print("===================================")
        print(f"🚀 Comenzando pallet {idx}/{len(pallets)} → {pallet_code} ({nombre})")

        asins = obtener_asins_de_pallets([pallet_code])
        print(f"   ASIN detectados: {len(asins)}")

        for asin in asins:
            print("-----------------------------------")
            print(f"🔄 ASIN: {asin}")

            unitcost = obtener_unitcost(asin)

            if unitcost is None:
                print("⚠️ No hay UnitCost → se omite")
                continue

            scraped_price = scrape_price(asin)

            if scraped_price is not None:
                precio = scraped_price
                precio_amazon = unitcost
                print(f"📌 Scraping OK → precio={precio}, precio_amazon={precio_amazon}")
            else:
                precio = unitcost
                precio_amazon = unitcost
                print(f"📌 Scraping NO → usando UnitCost={unitcost}")

            actualizar_precios_db(asin, precio, precio_amazon)
            print("💾 Actualizado en BD.")

            time.sleep(1)

        print(f"✅ Finalizado pallet {pallet_code} ({nombre})\n")

        if idx < len(pallets):
            siguiente = pallets[idx]
            sig_nombre = nombres_pallets.get(siguiente, "(sin nombre)")
            print(f"➡️ Próximo pallet → {siguiente} ({sig_nombre})\n")

    print("🎉 PROCESO COMPLETO")

# -----------------------------------
if __name__ == "__main__":
    main()
