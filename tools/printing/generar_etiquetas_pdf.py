import csv
import os
import argparse
from reportlab.pdfgen import canvas
from reportlab.lib.units import mm
from reportlab.lib import colors
from reportlab.graphics.barcode import qr
from reportlab.graphics import renderPDF
from reportlab.graphics.shapes import Drawing

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DEFAULT_INPUT_CSV = os.path.join(BASE_DIR, "pallets_asin_precios_amazon.csv")
DEFAULT_OUTPUT_PDF = os.path.join(BASE_DIR, "etiquetas_51x38.pdf")
DEFAULT_FACTOR = 0.20

PAGE_WIDTH = 51 * mm
PAGE_HEIGHT = 38 * mm

def draw_label(c, pallet, asin, precio_amazon, factor):

    precio_original = float(precio_amazon)
    precio_oferta = round(precio_original * factor, 2)

    margen_x = 3 * mm
    margen_y = 6 * mm

    # PRECIO GRANDE
    c.setFont("Helvetica-Bold", 16)
    precio_text = f"{precio_oferta:.2f} €"
    c.drawString(margen_x, PAGE_HEIGHT - 12 * mm, precio_text)

    # PRECIO ORIGINAL TACHADO
    c.setFont("Helvetica", 9)
    original_text = f"{precio_original:.2f} €"
    y_original = PAGE_HEIGHT - 17 * mm

    # Dibujar texto
    c.drawString(margen_x, y_original, original_text)

    # Calcular ancho real del texto
    text_width = c.stringWidth(original_text, "Helvetica", 9)

    # Línea fina de tachado
    c.setLineWidth(0.4)  # antes usaba grosor por defecto
    c.line(
        margen_x,
        y_original + 3,
        margen_x + text_width,
        y_original + 3
    )
    # ASIN
    c.setFont("Helvetica", 8)
    c.drawString(margen_x, PAGE_HEIGHT - 23 * mm, asin)

    # PALLET
    c.setFont("Helvetica", 6)
    c.setFillColor(colors.grey)
    c.drawString(margen_x, PAGE_HEIGHT - 27 * mm, pallet)
    c.setFillColor(colors.black)

    # QR PEQUEÑO A LA DERECHA
    url = f"https://www.google.com/search?q={asin}"
    qr_code = qr.QrCodeWidget(url)
    bounds = qr_code.getBounds()

    size = 26 * mm  # Tamaño ajustado
    width = bounds[2] - bounds[0]
    height = bounds[3] - bounds[1]

    d = Drawing(size, size, transform=[size/width, 0, 0, size/height, 0, 0])
    d.add(qr_code)

    renderPDF.draw(d, c, PAGE_WIDTH - size - margen_x, margen_y)

def generar_pdf(input_csv, output_pdf, factor):
    c = canvas.Canvas(output_pdf, pagesize=(PAGE_WIDTH, PAGE_HEIGHT))
    etiquetas = 0

    with open(input_csv, newline="", encoding="utf-8") as csvfile:
        reader = csv.DictReader(csvfile)

        for row in reader:
            precio = row.get("precio_amazon")
            if precio in (None, "", "None"):
                continue
            try:
                draw_label(
                    c,
                    row.get("pallet_code", ""),
                    row.get("asin", ""),
                    precio,
                    factor,
                )
                c.showPage()
                etiquetas += 1
            except ValueError:
                continue

    c.save()
    print("PDF generado correctamente:", output_pdf)
    print("Total etiquetas generadas:", etiquetas)


def main():
    parser = argparse.ArgumentParser(description="Genera PDF de etiquetas 51x38.")
    parser.add_argument(
        "--input-csv",
        default=DEFAULT_INPUT_CSV,
        help="CSV con columnas pallet_code, asin, precio_amazon.",
    )
    parser.add_argument(
        "--output-pdf",
        default=DEFAULT_OUTPUT_PDF,
        help="Ruta PDF de salida.",
    )
    parser.add_argument(
        "--factor",
        type=float,
        default=DEFAULT_FACTOR,
        help="Factor multiplicador para precio oferta (ej. 0.20).",
    )
    args = parser.parse_args()

    generar_pdf(args.input_csv, args.output_pdf, args.factor)

if __name__ == "__main__":
    main()