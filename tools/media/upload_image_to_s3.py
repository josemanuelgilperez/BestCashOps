import os
import csv
import json
import requests
import boto3
from urllib.request import urlopen
from urllib.parse import quote_plus
from botocore.exceptions import ClientError

# -------------------------------------------------
# CONFIGURACIÓN
# -------------------------------------------------
CRAWLBASE_TOKEN = os.getenv(
    "CRAWLBASE_TOKEN",
    "9a_E5QjtbAz2sAbVt2U3vQ"
)

IMAGE_BUCKET = "bestcashproductimages"
TOOLS_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CSV_PATH = os.path.join(TOOLS_DIR, "data", "asins.csv")

s3 = boto3.client("s3")

# -------------------------------------------------
# S3
# -------------------------------------------------
def imagen_existe_en_s3(asin):
    try:
        s3.head_object(Bucket=IMAGE_BUCKET, Key=f"{asin}.jpg")
        return True
    except ClientError:
        return False

def subir_imagen_s3(asin, image_url):
    r = requests.get(image_url, timeout=15)
    r.raise_for_status()

    s3.put_object(
        Bucket=IMAGE_BUCKET,
        Key=f"{asin}.jpg",
        Body=r.content,
        ContentType="image/jpeg"
    )

# -------------------------------------------------
# SCRAPING AMAZON (Crawlbase)
# -------------------------------------------------
def obtener_imagen_principal_amazon(asin):
    url = f"https://www.amazon.de/dp/{asin}"
    encoded_url = quote_plus(url)

    handler = urlopen(
        f"https://api.crawlbase.com/"
        f"?token={CRAWLBASE_TOKEN}"
        f"&scraper=amazon-product-details"
        f"&url={encoded_url}"
    )

    response = handler.read().decode("utf-8")
    parsed = json.loads(response)

    body = parsed.get("body", {})
    return body.get("mainImage")

# -------------------------------------------------
# MAIN
# -------------------------------------------------
if __name__ == "__main__":

    print("🔍 Iniciando scraping de imágenes Amazon → S3")

    with open(CSV_PATH, newline="", encoding="utf-8") as csvfile:
        reader = csv.reader(csvfile)
        asins = [row[0].strip() for row in reader if row]

    print(f"📦 ASINs en CSV: {len(asins)}")

    asins_pendientes = []

    for i, asin in enumerate(asins, start=1):
        print(f"\n➡️ [{i}/{len(asins)}] Procesando ASIN {asin}")

        try:
            # 1. Scraping
            image_url = obtener_imagen_principal_amazon(asin)

            if not image_url:
                print("   ⚠️ No se encontró imagen principal")
                asins_pendientes.append(asin)
                continue

            # 2. Subida a S3
            subir_imagen_s3(asin, image_url)

            print("   ✅ Imagen encontrada y subida correctamente")

        except json.JSONDecodeError as e:
            print(f"   ❌ Error parseando JSON: {e}")
            asins_pendientes.append(asin)

        except Exception as e:
            print(f"   ❌ Error procesando ASIN {asin}: {e}")
            asins_pendientes.append(asin)

    # -------------------------------------------------
    # REESCRIBIR CSV SOLO CON LOS PENDIENTES
    # -------------------------------------------------
    with open(CSV_PATH, "w", newline="", encoding="utf-8") as csvfile:
        writer = csv.writer(csvfile)
        for asin in asins_pendientes:
            writer.writerow([asin])

    print("\n📄 CSV actualizado")
    print(f"   ASINs procesados correctamente: {len(asins) - len(asins_pendientes)}")
    print(f"   ASINs pendientes: {len(asins_pendientes)}")
    print("🎉 Proceso completado.")
