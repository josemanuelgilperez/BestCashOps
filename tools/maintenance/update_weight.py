import os
import re
import csv
import json
import mysql.connector
from urllib.request import urlopen
from urllib.parse import quote_plus
from dotenv import load_dotenv, find_dotenv
print(">>> EJECUTANDO update_weight.py DESDE:", __file__)

# -------------------------------------------------
# CONFIGURACIÓN
# -------------------------------------------------
load_dotenv(find_dotenv())

DB_CONFIG = {
    'user': os.getenv('DB_USER', 'admin'),
    'password': os.getenv('DB_PASSWORD', '23092023BCdb'),
    'host': os.getenv('DB_HOST', 'bestcash.cvsxzrox0hah.eu-west-1.rds.amazonaws.com'),
    'database': os.getenv('DB_NAME', 'bestcash_rds')
}

CRAWLBASE_TOKEN = os.getenv("CRAWLBASE_TOKEN", "9a_E5QjtbAz2sAbVt2U3vQ")

TOOLS_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CSV_ASINS = os.path.join(TOOLS_DIR, "data", "asins.csv")
DOMINIOS_AMAZON = ['es', 'de', 'fr', 'it', 'com', 'nl', 'co.uk']

PESO_KEYS = [
    # ES
    'Peso del producto', 'Peso del artículo', 'Peso neto',
    # EN
    'Item Weight', 'Product Weight',
    # FR
    "Poids de l'article",
    # DE
    'Artikelgewicht',
    # IT
    "Peso dell'articolo"
]


# -------------------------------------------------
# UTILIDADES
# -------------------------------------------------
def normalizar_peso_gramos(texto):
    if not texto:
        return None

    t = texto.lower().replace(',', '.')
    match = re.search(
        r'(?<!max)(?<!maximum)\s*([\d.]+)\s*(kg|kilogram|kilogramos|g|gram|oz|lb|lbs)',
        t
    )

    if not match:
        return None

    valor = float(match.group(1))
    unidad = match.group(2)

    if unidad in ('kg', 'kilogram', 'kilogramos'):
        return valor * 1000
    if unidad in ('g', 'gram'):
        return valor
    if unidad == 'oz':
        return valor * 28.3495
    if unidad in ('lb', 'lbs'):
        return valor * 453.592

    return None

def extraer_peso(product):
    # -------- Nivel 1: productInformation / technicalDetails
    for bloque in ('productInformation', 'technicalDetails'):
        for item in product.get(bloque, []) or []:
            name = re.sub(r'\s|\u200f', '', item.get('name', '')).lower()
            value = item.get('value', '')
            for clave in PESO_KEYS:
                if re.sub(r'\s|\u200f', '', clave).lower() == name:
                    peso = normalizar_peso_gramos(value)
                    if peso:
                        return peso

    # -------- Nivel 2: dimensiones combinadas
    for item in product.get('productInformation', []) or []:
        value = item.get('value', '')
        if ';' in value:
            partes = value.split(';')
            for p in partes:
                peso = normalizar_peso_gramos(p)
                if peso:
                    return peso

    # -------- Nivel 3: texto libre (último recurso)
    textos = []
    textos += product.get('features', []) or []
    textos.append(product.get('description', ''))

    for txt in textos:
        peso = normalizar_peso_gramos(txt)
        if peso:
            return peso

    return None


# -------------------------------------------------
# SCRAPING
# -------------------------------------------------
def scrapear_peso_por_asin(asin):
    for dominio in DOMINIOS_AMAZON:
        try:
            print(f"🌍 {asin} → amazon.{dominio}")

            url = f"https://www.amazon.{dominio}/dp/{asin}"
            encoded = quote_plus(url)
            crawl_url = (
                f"https://api.crawlbase.com/"
                f"?token={CRAWLBASE_TOKEN}"
                f"&scraper=amazon-product-details"
                f"&url={encoded}"
            )

            response = urlopen(crawl_url, timeout=20).read().decode("utf-8")
            data = json.loads(response)
            product = data.get("body", {})

            if not product:
                continue

            peso = extraer_peso(product)
            if peso:
                print(f"   ✅ Peso encontrado: {peso} g")
                return peso

        except Exception as e:
            print(f"   ❌ Error {dominio}: {e}")

    return None


# -------------------------------------------------
# CSV
# -------------------------------------------------
def leer_asins_desde_csv(path):
    asins = []
    with open(path, newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        if 'asin' not in reader.fieldnames:
            raise ValueError("El CSV debe tener una columna llamada 'asin'")
        for row in reader:
            asin = row['asin'].strip()
            if asin:
                asins.append(asin)
    return list(dict.fromkeys(asins))  # elimina duplicados manteniendo orden


# -------------------------------------------------
# DB
# -------------------------------------------------
def actualizar_peso(asin, peso):
    conn = mysql.connector.connect(**DB_CONFIG)
    cursor = conn.cursor()

    cursor.execute("""
        UPDATE amazon_scraped_products
        SET peso = %s,
            fecha_scraping = NOW()
        WHERE asin = %s
    """, (peso, asin))

    conn.commit()
    cursor.close()
    conn.close()


# -------------------------------------------------
# COMPRUEBA QUE EL ASIN EXISTE
# -------------------------------------------------

def asin_existe_en_bd(asin):
    conn = mysql.connector.connect(**DB_CONFIG)
    cursor = conn.cursor()

    cursor.execute(
        "SELECT 1 FROM amazon_scraped_products WHERE asin = %s LIMIT 1",
        (asin,)
    )
    existe = cursor.fetchone() is not None

    cursor.close()
    conn.close()
    return existe

def cargar_asins_bd():
    conn = mysql.connector.connect(**DB_CONFIG)
    cursor = conn.cursor()
    cursor.execute("SELECT asin FROM amazon_scraped_products")
    asins = {r[0] for r in cursor.fetchall()}
    cursor.close()
    conn.close()
    return asins

# -------------------------------------------------
# MAIN
# -------------------------------------------------
if __name__ == "__main__":

    asins = leer_asins_desde_csv(CSV_ASINS)
    print(f"🔍 ASINs cargados desde CSV: {len(asins)}")

    ok, fail, omitidos = 0, 0, 0
    asins_bd = cargar_asins_bd()
    for asin in asins:
        if asin in asins_bd:
            print(f"\n🔄 Procesando {asin}")
            db_config = {
                'user': os.getenv('DB_USER', 'admin'),
                'password': os.getenv('DB_PASSWORD', '23092023BCdb'),
                'host': os.getenv('DB_HOST', 'bestcash.cvsxzrox0hah.eu-west-1.rds.amazonaws.com'),
                'database': os.getenv('DB_NAME', 'bestcash_rds')
            }

            peso = scrapear_peso_por_asin(asin)

            if peso:
                actualizar_peso(asin, peso)
                ok += 1
            else:
                print("   ⚠️ Peso no encontrado")
                fail += 1

    print("\n🧾 RESUMEN FINAL")
    print(f"   ✔ Pesos actualizados: {ok}")
    print(f"   ❌ Sin peso: {fail}")
    print(f"   ⏭️ ASINs omitidos (no existen en BD): {omitidos}")
