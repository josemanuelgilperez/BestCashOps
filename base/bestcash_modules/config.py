import os
from dotenv import load_dotenv, find_dotenv

load_dotenv(find_dotenv())

DB_CONFIG = {
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "host": os.getenv("DB_HOST"),
    "database": os.getenv("DB_NAME"),
}

CRAWLBASE_TOKEN = os.getenv("CRAWLBASE_TOKEN")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
IMAGE_BUCKET = "bestcashproductimages"
IMAGES_FOLDER = "imagenes_productos"
