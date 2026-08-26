import logging
import os
import requests
import boto3
from botocore.exceptions import ProfileNotFound
from io import BytesIO

from .config import IMAGE_BUCKET

_s3_client = None


def _get_s3_client():
    """Cliente S3 perezoso: evita fallar al importar y no exige perfil ~/.aws si hay claves en env."""
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
        # p. ej. .env copiado con AWS_PROFILE=bestcash pero sin ese perfil en el servidor
        old_profile = os.environ.pop("AWS_PROFILE", None)
        try:
            _s3_client = boto3.client("s3")
        finally:
            if old_profile is not None:
                os.environ["AWS_PROFILE"] = old_profile

    return _s3_client


def get_existing_images_from_s3(asin):
    try:
        resp = _get_s3_client().list_objects_v2(Bucket=IMAGE_BUCKET, Prefix=f"{asin}/")
        if "Contents" in resp:
            return sorted(
                f"https://{IMAGE_BUCKET}.s3.amazonaws.com/{obj['Key']}"
                for obj in resp["Contents"]
                if not obj["Key"].endswith("/")
            )
        return []
    except Exception as exc:
        logging.warning("Error listando imagenes en S3 para %s: %s", asin, exc)
        return []


def download_and_upload_images(asin, imagenes):
    existentes = get_existing_images_from_s3(asin)
    imagenes_unicas = list(dict.fromkeys(imagenes or []))[:5]
    if existentes and len(existentes) >= len(imagenes_unicas):
        return existentes

    urls = list(existentes)
    existing_filenames = {os.path.basename(url) for url in existentes}
    for idx, url in enumerate(imagenes_unicas):
        filename = f"{asin}_{idx + 1}.jpg"
        if filename in existing_filenames:
            continue
        try:
            response = requests.get(url, timeout=10)
            response.raise_for_status()

            s3_key = f"{asin}/{filename}"
            _get_s3_client().upload_fileobj(
                BytesIO(response.content),
                IMAGE_BUCKET,
                s3_key,
                ExtraArgs={"ContentType": "image/jpeg"},
            )
            urls.append(f"https://{IMAGE_BUCKET}.s3.amazonaws.com/{s3_key}")
            existing_filenames.add(filename)
        except Exception as exc:
            logging.warning(
                "No se pudo descargar/subir imagen para %s (%s): %s", asin, url, exc
            )

    return sorted(urls)
