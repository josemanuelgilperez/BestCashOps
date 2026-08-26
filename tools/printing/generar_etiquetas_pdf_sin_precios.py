import argparse
import os

import mysql.connector
from dotenv import find_dotenv, load_dotenv
from reportlab.graphics import renderPDF
from reportlab.graphics.barcode import qr
from reportlab.graphics.shapes import Drawing
from reportlab.lib import colors
from reportlab.lib.units import mm
from reportlab.pdfgen import canvas

load_dotenv(find_dotenv())

DB_CONFIG = {
    "user": os.getenv("DB_USER", "bestcash_app"),
    "password": os.getenv("DB_PASSWORD", ""),
    "host": os.getenv("DB_HOST", "82.223.203.117"),
    "database": os.getenv("DB_NAME", "bestcash"),
}
if os.getenv("DB_PORT"):
    DB_CONFIG["port"] = int(os.getenv("DB_PORT"))

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DEFAULT_INPUT_TXT = os.path.join(BASE_DIR, "pallets.txt")
DEFAULT_OUTPUT_PDF = os.path.join(BASE_DIR, "etiquetas_51x38_sin_precios.pdf")

PAGE_WIDTH = 51 * mm
PAGE_HEIGHT = 38 * mm
MIN_ASIN_FONT_SIZE = 6.5


def font_size_that_fits(c, text, font_name, preferred_size, max_width):
    size = preferred_size
    while size > MIN_ASIN_FONT_SIZE and c.stringWidth(text, font_name, size) > max_width:
        size -= 0.25
    return max(size, MIN_ASIN_FONT_SIZE)


def leer_pallets(path):
    with open(path, "r", encoding="utf-8") as f:
        return [line.strip() for line in f if line.strip()]


def obtener_etiquetas(input_txt):
    pallets = leer_pallets(input_txt)

    if not pallets:
        print("No hay pallets en el archivo de entrada.")
        return []

    placeholders = ",".join(["%s"] * len(pallets))

    query = f"""
    WITH RECURSIVE unit_expansion AS (
        SELECT
            bi.box_code,
            bi.asin,
            bi.quantity,
            1 AS n
        FROM box_items bi
        WHERE bi.box_code IN ({placeholders})

        UNION ALL

        SELECT
            box_code,
            asin,
            quantity,
            n + 1
        FROM unit_expansion
        WHERE n < quantity
    )

    SELECT
        ue.box_code AS pallet_code,
        ue.asin
    FROM unit_expansion ue
    ORDER BY ue.box_code ASC, ue.asin ASC;
    """

    try:
        conn = mysql.connector.connect(**DB_CONFIG)
    except mysql.connector.Error as exc:
        host = DB_CONFIG.get("host")
        port = DB_CONFIG.get("port", 3306)
        raise SystemExit(
            "No se pudo conectar a MySQL "
            f"({host}:{port}, base {DB_CONFIG.get('database')}).\n"
            f"Detalle MySQL: {exc}\n"
            "Ejecutalo en el servidor operativo con su venv, o define "
            "DB_HOST/DB_PORT apuntando a un tunel MySQL accesible desde "
            "este equipo.\n"
            "Ejemplo: ssh root@212.227.90.202 "
            "\"cd /root/BestCashOps && venv/bin/python "
            "tools/printing/generar_etiquetas_pdf_sin_precios.py\""
        ) from exc

    cursor = conn.cursor(dictionary=True)
    try:
        cursor.execute(query, pallets)
        return cursor.fetchall()
    finally:
        cursor.close()
        conn.close()


def draw_label(c, pallet, asin):
    margen_x = 3 * mm
    margen_y = 6 * mm
    gap_texto_qr = 4 * mm
    qr_size = 24 * mm
    qr_x = PAGE_WIDTH - qr_size - margen_x
    qr_y = margen_y
    texto_max_width = qr_x - margen_x - gap_texto_qr

    # Bloque de texto centrado verticalmente respecto al QR.
    asin_font = "Helvetica-Bold"
    asin_size = font_size_that_fits(c, asin, asin_font, 10, texto_max_width)
    pallet_font = "Helvetica"
    pallet_size = 6
    line_gap = 2.5 * mm
    text_block_height = asin_size + line_gap + pallet_size
    block_center_y = qr_y + qr_size / 2
    asin_y = block_center_y + text_block_height / 2 - asin_size
    pallet_y = asin_y - line_gap - pallet_size

    c.setFont(asin_font, asin_size)
    asin_width = c.stringWidth(asin, asin_font, asin_size)
    asin_x = margen_x + (texto_max_width - asin_width) / 2
    c.drawString(asin_x, asin_y, asin)

    c.setFont(pallet_font, pallet_size)
    c.setFillColor(colors.grey)
    pallet_width = c.stringWidth(pallet, pallet_font, pallet_size)
    pallet_x = margen_x + max(0, (texto_max_width - pallet_width) / 2)
    c.drawString(pallet_x, pallet_y, pallet)
    c.setFillColor(colors.black)

    url = f"https://www.google.com/search?q={asin}"
    qr_code = qr.QrCodeWidget(url)
    bounds = qr_code.getBounds()
    width = bounds[2] - bounds[0]
    height = bounds[3] - bounds[1]

    d = Drawing(qr_size, qr_size, transform=[qr_size / width, 0, 0, qr_size / height, 0, 0])
    d.add(qr_code)

    renderPDF.draw(d, c, qr_x, qr_y)


def generar_pdf(input_txt, output_pdf):
    rows = obtener_etiquetas(input_txt)
    c = canvas.Canvas(output_pdf, pagesize=(PAGE_WIDTH, PAGE_HEIGHT))
    etiquetas = 0

    for row in rows:
        asin = (row.get("asin") or "").strip()
        if not asin:
            continue
        pallet = (row.get("pallet_code") or "").strip()

        draw_label(c, pallet, asin)
        c.showPage()
        etiquetas += 1

    c.save()
    print("PDF generado correctamente:", output_pdf)
    print("Total etiquetas generadas:", etiquetas)


def main():
    parser = argparse.ArgumentParser(
        description="Genera PDF de etiquetas 51x38 sin precios desde pallets.txt."
    )
    parser.add_argument(
        "--input-txt",
        default=DEFAULT_INPUT_TXT,
        help="TXT con codigos de pallet, uno por linea.",
    )
    parser.add_argument(
        "--output-pdf",
        default=DEFAULT_OUTPUT_PDF,
        help="Ruta PDF de salida.",
    )
    args = parser.parse_args()

    generar_pdf(args.input_txt, args.output_pdf)


if __name__ == "__main__":
    main()
