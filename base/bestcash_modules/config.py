import os
from dotenv import load_dotenv, find_dotenv

load_dotenv(find_dotenv())

DB_CONFIG = {
    "user": os.getenv("DB_USER", "bestcash_app"),
    "password": os.getenv("DB_PASSWORD", "Bc_TPV_2026!kjDERZtm#82"),
    "host": os.getenv("DB_HOST", "82.223.203.117"),
    "database": os.getenv("DB_NAME", "bestcash_rds"),
}

CRAWLBASE_TOKEN = os.getenv("CRAWLBASE_TOKEN")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
IMAGE_BUCKET = "bestcashproductimages"
IMAGES_FOLDER = "imagenes_productos"
