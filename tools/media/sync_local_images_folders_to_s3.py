import os
import mimetypes
import argparse
import boto3
from botocore.exceptions import ClientError


IMAGE_BUCKET = "bestcashproductimages"
DEFAULT_SOURCE_DIR = "/root/Downloads"
VALID_EXTENSIONS = {".jpg", ".jpeg", ".png", ".webp"}


def object_exists(s3_client, bucket, key):
    try:
        s3_client.head_object(Bucket=bucket, Key=key)
        return True
    except ClientError as exc:
        code = exc.response.get("Error", {}).get("Code", "")
        if code in ("404", "NoSuchKey", "NotFound"):
            return False
        raise


def upload_file(s3_client, local_path, bucket, key):
    content_type, _ = mimetypes.guess_type(local_path)
    extra_args = {"ContentType": content_type or "application/octet-stream"}
    s3_client.upload_file(local_path, bucket, key, ExtraArgs=extra_args)


def sync_folders(source_dir, dry_run=False):
    if not os.path.isdir(source_dir):
        raise FileNotFoundError(f"No existe carpeta origen: {source_dir}")

    s3_client = boto3.client("s3")
    total_folders = 0
    total_files = 0
    uploaded = 0
    skipped = 0
    errors = 0

    for folder_name in sorted(os.listdir(source_dir)):
        folder_path = os.path.join(source_dir, folder_name)
        if not os.path.isdir(folder_path):
            continue

        asin = folder_name.strip()
        if not asin:
            continue

        total_folders += 1
        print(f"\n📂 ASIN folder: {asin}")

        for file_name in sorted(os.listdir(folder_path)):
            local_path = os.path.join(folder_path, file_name)
            if not os.path.isfile(local_path):
                continue

            ext = os.path.splitext(file_name)[1].lower()
            if ext not in VALID_EXTENSIONS:
                continue

            key = f"{asin}/{file_name}"
            total_files += 1

            try:
                if object_exists(s3_client, IMAGE_BUCKET, key):
                    skipped += 1
                    print(f"   ↪️  Skip exists: {key}")
                    continue

                if dry_run:
                    uploaded += 1
                    print(f"   🧪 Dry-run upload: {key}")
                    continue

                upload_file(s3_client, local_path, IMAGE_BUCKET, key)
                uploaded += 1
                print(f"   ✅ Uploaded: {key}")
            except Exception as exc:
                errors += 1
                print(f"   ❌ Error {key}: {exc}")

    print("\n===== RESUMEN =====")
    print(f"Carpetas ASIN detectadas: {total_folders}")
    print(f"Archivos imagen evaluados: {total_files}")
    print(f"Subidos nuevos: {uploaded}")
    print(f"Saltados existentes: {skipped}")
    print(f"Errores: {errors}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Sincroniza carpetas locales por ASIN hacia S3."
    )
    parser.add_argument(
        "--source-dir",
        default=DEFAULT_SOURCE_DIR,
        help="Carpeta local con subcarpetas por ASIN.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Simula subidas sin escribir en S3.",
    )
    args = parser.parse_args()

    sync_folders(args.source_dir, dry_run=args.dry_run)
