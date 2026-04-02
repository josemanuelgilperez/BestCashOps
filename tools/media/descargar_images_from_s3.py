import boto3
import os

# Configuración
bucket_name = "bestcashproductimages"
local_dir = "imagenes_productos"
TOOLS_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
asins_file = os.path.join(TOOLS_DIR, "data", "asin.txt")

# Cliente S3 (usa las credenciales de 'aws configure')
s3 = boto3.client("s3")

# Crear carpeta local si no existe
os.makedirs(local_dir, exist_ok=True)

# Leer lista de ASIN
with open(asins_file, "r") as f:
    asins = [line.strip() for line in f if line.strip()]

for asin in asins:
    print(f"🔍 Buscando imágenes para {asin}...")
    prefix = asin + "/"
    result = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)

    if "Contents" not in result:
        print(f"⚠️ No se encontraron imágenes para {asin}")
        continue

    asin_dir = os.path.join(local_dir, asin)
    os.makedirs(asin_dir, exist_ok=True)

    for obj in result["Contents"]:
        key = obj["Key"]
        filename = os.path.basename(key)
        local_path = os.path.join(asin_dir, filename)
        print(f"⬇️ Descargando {key} -> {local_path}")
        s3.download_file(bucket_name, key, local_path)

print("✅ Descarga terminada.")
