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


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Re-scrape forzado de ASINs concretos desde CSV o TXT."
    )
    parser.add_argument("--asin", help="Re-scrapear un unico ASIN.")
    parser.add_argument("--from-csv", dest="from_csv", help="Ruta de CSV con ASINs en la primera columna.")
    parser.add_argument("--from-txt", dest="from_txt", help="Ruta de TXT con un ASIN por linea.")
    parser.add_argument("--limit", type=int, help="Limitar cantidad de ASINs a procesar.")
    args = parser.parse_args()

    provided_sources = [bool(args.asin), bool(args.from_csv), bool(args.from_txt)]
    if sum(provided_sources) != 1:
        parser.error("Debes indicar exactamente una fuente: --asin, --from-csv o --from-txt.")

    run_pipeline(
        asin=args.asin,
        from_csv=args.from_csv,
        from_txt=args.from_txt,
        limit=args.limit,
        skip_existing=False,
    )
