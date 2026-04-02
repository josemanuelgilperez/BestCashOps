import os
import csv
import argparse
import mysql.connector

# ==============================
# CONFIGURACIÓN
# ==============================
DB_CONFIG = {
    'user': os.getenv('DB_USER', 'admin'),
    'password': os.getenv('DB_PASSWORD', '23092023BCdb'),
    'host': os.getenv('DB_HOST', 'bestcash.cvsxzrox0hah.eu-west-1.rds.amazonaws.com'),
    'database': os.getenv('DB_NAME', 'bestcash_rds')
}

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DEFAULT_INPUT_TXT = os.path.join(BASE_DIR, "pallets.txt")
DEFAULT_OUTPUT_FILE = os.path.join(BASE_DIR, "pallets_asin_precios_amazon.csv")

# ==============================
# LEER PALLETS
# ==============================
def leer_pallets(path):
    with open(path, "r", encoding="utf-8") as f:
        pallets = [line.strip() for line in f if line.strip()]
    return pallets

# ==============================
# MAIN
# ==============================
def generar_csv(input_txt, output_file):
    pallets = leer_pallets(input_txt)

    if not pallets:
        print("No hay pallets en el archivo de entrada.")
        return

    placeholders = ",".join(["%s"] * len(pallets))

    QUERY = f"""
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
        ue.asin,
        COALESCE(asp.precio, asp.precio_amazon) AS precio_final
    FROM unit_expansion ue
    LEFT JOIN amazon_scraped_products asp 
        ON ue.asin = asp.asin
    ORDER BY ue.box_code ASC, ue.asin ASC;
    """

    conn = mysql.connector.connect(**DB_CONFIG)
    cursor = conn.cursor()
    try:
        cursor.execute(QUERY, pallets)
        rows = cursor.fetchall()

        with open(output_file, mode="w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(["pallet_code", "asin", "precio_amazon"])
            writer.writerows(rows)
    finally:
        cursor.close()
        conn.close()

    print(f"CSV generado correctamente: {output_file}")
    print(f"Total líneas generadas: {len(rows)}")


def main():
    parser = argparse.ArgumentParser(
        description="Genera CSV de unidades por pallet para impresion."
    )
    parser.add_argument(
        "--input-txt",
        default=DEFAULT_INPUT_TXT,
        help="Ruta del TXT con codigos de pallet, uno por linea.",
    )
    parser.add_argument(
        "--output-csv",
        default=DEFAULT_OUTPUT_FILE,
        help="Ruta del CSV de salida.",
    )
    args = parser.parse_args()

    generar_csv(args.input_txt, args.output_csv)

if __name__ == "__main__":
    main()