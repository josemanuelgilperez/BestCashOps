import subprocess
import os
import sys
import logging
from datetime import datetime
from dotenv import load_dotenv

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if BASE_DIR not in sys.path:
    sys.path.append(BASE_DIR)

from db import get_connection
load_dotenv()

# ======================================
# LOGGING SETUP
# ======================================
LOG_DIR = "logs"
os.makedirs(LOG_DIR, exist_ok=True)

log_file = os.path.join(
    LOG_DIR,
    f"pipeline_{datetime.now().strftime('%Y%m%d')}.log"
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[
        logging.FileHandler(log_file, encoding="utf-8"),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger()

def query_scalar(sql):
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(sql)
    result = cur.fetchone()[0]
    cur.close()
    conn.close()
    return result

# ======================================
# RUN SCRIPT
# ======================================
def run(script):
    logger.info(f"🚀 Ejecutando: {script}")
    result = subprocess.run([sys.executable, script], cwd=BASE_DIR)

    if result.returncode != 0:
        logger.error(f"❌ Error en {script}")
        raise RuntimeError(f"Error en {script}")

    logger.info(f"✅ Finalizado: {script}")

# ======================================
# VALIDACIONES
# ======================================
def check_ingest():
    null_asin = query_scalar("""
        SELECT COUNT(*) FROM box_items
        WHERE asin IS NULL OR asin = ''
    """)
    logger.info(f"🔍 Check ingest → ASIN inválidos: {null_asin}")

    if null_asin > 0:
        raise RuntimeError("ASIN inválidos detectados")

def check_enrich():
    count = query_scalar("SELECT COUNT(*) FROM amazon_scraped_products")
    logger.info(f"🔍 Check enrich → productos scrapeados: {count}")

    if count == 0:
        raise RuntimeError("No hay datos en amazon_scraped_products")

    null_pvp = query_scalar("""
        SELECT COUNT(*)
        FROM box_items bi
        JOIN boxes b ON bi.box_code = b.code
        WHERE b.status IN ('Disponible','Reservado')
          AND (bi.pvp_ud IS NULL OR bi.pvp_ud = 0)
    """)
    logger.info(f"🔍 Check enrich → pvp_ud inválidos tras enrich: {null_pvp}")
    if null_pvp > 0:
        raise RuntimeError("pvp_ud inválidos tras enrich")

def check_finance():
    null_prices = query_scalar("""
        SELECT COUNT(*) FROM box_items 
        WHERE precio_lote_ud IS NULL OR precio_lote_ud = 0
    """)
    logger.info(f"🔍 Check finance → precios no calculados: {null_prices}")

    if null_prices > 0:
        raise RuntimeError("precio_lote_ud no calculado")

# ======================================
# MAIN
# ======================================
if __name__ == "__main__":

    start = datetime.now()
    logger.info("=====================================")
    logger.info("🚀 INICIO PIPELINE BESTCASH")
    logger.info("=====================================")

    try:
        os.chdir(BASE_DIR)

        # 1. INGEST
        run("pipeline/ingest.py")
        check_ingest()

        # 2. ENRICH
        run("pipeline/enrich.py")
        check_enrich()

        # 3. FINANCE
        run("pipeline/finance.py")
        check_finance()

        # 4. HTML
        run("web/build_html.py")

        # 5. CATEGORÍAS
        run("web/categories.py")

        # 6. FTP (opcional: si FTP_USER y FTP_PASS están definidos)
        if os.getenv("FTP_USER") and os.getenv("FTP_PASS"):
            run("scripts/upload_ftp.py")
        else:
            logger.info("ℹ️ FTP no configurado, omitiendo subida")

        elapsed = datetime.now() - start

        logger.info("=====================================")
        logger.info(f"🎉 PIPELINE COMPLETO OK ({elapsed})")
        logger.info("=====================================")

    except Exception as e:
        logger.exception("💥 PIPELINE FALLIDO")
        exit(1)