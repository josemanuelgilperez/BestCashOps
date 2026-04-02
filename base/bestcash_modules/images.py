import logging
import requests
import boto3
from io import BytesIO

from .config import IMAGE_BUCKET

s3 = boto3.client("s3")


def get_existing_images_from_s3(asin):
    try:
        resp = s3.list_objects_v2(Bucket=IMAGE_BUCKET, Prefix=f"{asin}/")
        if "Contents" in resp:
            return [
                f"https://{IMAGE_BUCKET}.s3.amazonaws.com/{obj['Key']}"
                for obj in resp["Contents"]
            ]
        return []
    except Exception as exc:
        logging.warning("Error listando imagenes en S3 para %s: %s", asin, exc)
        return []


def download_and_upload_images(asin, imagenes):
    existentes = get_existing_images_from_s3(asin)
    if existentes:
        return existentes

    urls = []
    for idx, url in enumerate(list(dict.fromkeys(imagenes))[:5]):
        try:
            response = requests.get(url, timeout=10)
            response.raise_for_status()

            filename = f"{asin}_{idx + 1}.jpg"
            s3_key = f"{asin}/{filename}"
            s3.upload_fileobj(
                BytesIO(response.content),
                IMAGE_BUCKET,
                s3_key,
                ExtraArgs={"ContentType": "image/jpeg"},
            )
            urls.append(f"https://{IMAGE_BUCKET}.s3.amazonaws.com/{s3_key}")
        except Exception as exc:
            logging.warning(
                "No se pudo descargar/subir imagen para %s (%s): %s", asin, url, exc
            )

    return urls
