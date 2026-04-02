import os
import sys
import logging
import argparse

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from bestcash_modules import run_pipeline


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)
logging.getLogger("openai").setLevel(logging.WARNING)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Scrapeo y generacion de fichas para ASINs.")
    parser.add_argument("--asin", help="Procesar solo un ASIN concreto.")
    parser.add_argument("--limit", type=int, help="Limitar cantidad de ASINs a procesar.")
    parser.add_argument("--from-csv", dest="from_csv", help="Ruta de CSV con ASINs en primera columna.")
    args = parser.parse_args()

    run_pipeline(asin=args.asin, limit=args.limit, from_csv=args.from_csv, skip_existing=True)