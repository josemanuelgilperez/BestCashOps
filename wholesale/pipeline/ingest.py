# ======================================================
# SCRIPT UNIFICADO: PROCESAMIENTO DE PALLETS (XLSX → BD)
# ======================================================
#
# 📥 ENTRADA (OBLIGATORIO)
# --------------------------------------
# 1. Carpeta de entrada:
#    Por defecto: ../../tools/data/new_box_files/ (si existe); si no, ./data/new_box_files/
#    (raíz de datos = WHOLESALE_DATA_DIR en .env o variable de entorno, opcional)
#
#    ➤ Colocar aquí los archivos Excel (.xlsx) descargados
#      (uno por pallet).
#
#    ➤ Formato esperado del nombre del archivo:
#       - Debe contener el código del pallet: MPxxxx o MLxxxx
#       - Ejemplo válido:
#            MP0760_cortinas.xlsx
#            ML1234-electronics.xlsx
#
#    ➤ El script extrae automáticamente el código (MPxxxx)
#
#
# 2. Archivo de mapeo de nombres:
#    Misma raíz: names.csv junto a new_box_files/
#
#    ➤ Formato:
#        codigo,titulo
#
#    ➤ Ejemplo:
#        MP0760,Cortinas
#        ML1234,Electrónica
#
#    ➤ Se usa para asignar el nombre del pallet en la BD
#
#
# 📤 SALIDA
# --------------------------------------
# 1. Base de datos MySQL:
#    - Tabla: boxes
#    - Tabla: box_items
#
# 2. Archivos procesados:
#    Misma raíz: processed/
#
#    ➤ Los Excel se mueven aquí tras procesarse correctamente
#
#
# 🔄 FLUJO AUTOMÁTICO
# --------------------------------------
#   XLSX → lectura directa → BD → mover a processed
#
#
# ⚠️ IMPORTANTE
# --------------------------------------
# - No se generan CSV intermedios
# - Si un pallet ya existe:
#     → se eliminan sus items anteriores
#     → se insertan los nuevos
# - Si falla el procesamiento:
#     → el archivo NO se mueve (queda en new_box_files)
#
#
# 🔧 REQUISITOS
# --------------------------------------
# - Variables de entorno opcionales:
#     DB_USER, DB_PASSWORD, DB_HOST, DB_NAME
#
# - Columnas en Excel (nombres flexibles, sin distinguir mayúsculas / espacios):
#     asin (obligatoria), units | unidades | quantity (obligatoria)
#     peso: totalweight (kg), totalweight, weight…
#     removalreason | removal | reason (opcional)
#
# ======================================================
import os
import re
import shutil
import pandas as pd
from dotenv import load_dotenv
import sys

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
REPO_ROOT = os.path.dirname(BASE_DIR)
for _p in (REPO_ROOT, BASE_DIR):
    if _p not in sys.path:
        sys.path.insert(0, _p)

from db import get_connection
load_dotenv()

# ==============================
# CONFIG
# ==============================


def _resolve_data_root():
    """Raíz con new_box_files/, names.csv y processed/."""
    override = (os.environ.get("WHOLESALE_DATA_DIR") or "").strip()
    if override:
        return os.path.abspath(override)
    tools_data = os.path.join(REPO_ROOT, "tools", "data")
    if os.path.isdir(os.path.join(tools_data, "new_box_files")):
        return tools_data
    return os.path.join(BASE_DIR, "data")


DATA_ROOT = _resolve_data_root()
SOURCE_DIR = os.path.join(DATA_ROOT, "new_box_files")
PROCESSED_DIR = os.path.join(DATA_ROOT, "processed")
NAMES_FILE = os.path.join(DATA_ROOT, "names.csv")

DB_CONFIG = None  # mantenido por compatibilidad si se usa en otros sitios
# ==============================
# UTILIDADES
# ==============================
def load_name_mapping():
    mapping = {}
    if not os.path.exists(NAMES_FILE):
        raise FileNotFoundError(f"No existe {NAMES_FILE}")

    import csv
    with open(NAMES_FILE, encoding='utf-8-sig') as f:
        reader = csv.reader(f)
        for row in reader:
            if len(row) >= 2:
                mapping[row[0].strip()] = row[1].strip()
    return mapping


def extract_code_and_name(filename, mapping):
    base = os.path.basename(filename)
    name_no_ext = re.sub(r'\.xlsx$', '', base, flags=re.IGNORECASE)

    m = re.search(r'(M[PL]\d+)', name_no_ext, flags=re.IGNORECASE)
    if not m:
        raise ValueError(f"Nombre inválido: {base}")

    code = m.group(1).upper()
    name = mapping.get(code, code)

    return code, name


def safe_int(x):
    try:
        return int(str(x).strip())
    except:
        return 0


def safe_float(x):
    try:
        return float(str(x).replace(',', '.'))
    except:
        return 0.0


def classify_reason(r):
    if not r:
        return 'other'
    r = str(r).lower()
    if r in ('overstock', 'vendor damage'):
        return 'overstock'
    if r in ('customer damage', 'defective'):
        return 'devoluciones'
    return 'other'


def _header_key(name):
    return re.sub(r"[^a-z0-9]", "", str(name).strip().lower())


def _resolve_excel_columns(df):
    """
    Devuelve nombres reales de columnas del DataFrame para asin, units, peso y motivo.
    """
    by_key = {}
    for c in df.columns:
        by_key[_header_key(c)] = c

    def pick(*patterns):
        for p in patterns:
            k = _header_key(p)
            if k in by_key:
                return by_key[k]
        return None

    return {
        "asin": pick("asin"),
        "units": pick("units", "unidades", "quantity", "qty", "cantidad"),
        "weight": pick(
            "totalweightkg",
            "totalweight",
            "weightkg",
            "peso",
            "pesokg",
            "totalpeso",
        ),
        "reason": pick("removalreason", "removal", "reason", "removalreasoncode"),
    }


def _cell_str(val):
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return None
    s = str(val).strip()
    if not s or s.lower() == "nan":
        return None
    return s


# ==============================
# PROCESO PRINCIPAL
# ==============================
def process_file(filepath, mapping):
    code, name = extract_code_and_name(filepath, mapping)

    conn = get_connection()
    cursor = conn.cursor()

    price_cache = {}

    # --- BOX ---
    cursor.execute("SELECT COUNT(*) FROM boxes WHERE code=%s", (code,))
    exists = cursor.fetchone()[0] > 0

    if not exists:
        cursor.execute("""
            INSERT INTO boxes (code, name, units, weight, overstock, devoluciones)
            VALUES (%s, %s, 0, 0, 0, 0)
        """, (code, name))
    else:
        cursor.execute("DELETE FROM box_items WHERE box_code=%s", (code,))

    # --- LEER EXCEL ---
    df = pd.read_excel(filepath)
    cols = _resolve_excel_columns(df)
    if not cols["asin"] or not cols["units"]:
        raise ValueError(
            "No se encontraron columnas obligatorias 'asin' y 'units' (o equivalentes). "
            f"Cabeceras en el Excel: {list(df.columns)}"
        )

    print(
        f"   Columnas: asin={cols['asin']!r}, units={cols['units']!r}, "
        f"peso={cols['weight']!r}, motivo={cols['reason']!r}"
    )

    total_units = 0
    total_weight = 0
    overstock = 0
    devoluciones = 0
    filas_insertadas = 0

    for _, row in df.iterrows():

        asin = _cell_str(row.get(cols["asin"]))
        units = safe_int(row.get(cols["units"]))
        weight = safe_float(row.get(cols["weight"])) if cols["weight"] else 0.0
        reason = row.get(cols["reason"]) if cols["reason"] else None

        if not asin or units <= 0:
            total_weight += weight
            continue

        # PRECIO
        if asin in price_cache:
            pvp = price_cache[asin]
        else:
            cursor.execute("""
                SELECT COALESCE(precio, precio_amazon)
                FROM amazon_scraped_products
                WHERE asin=%s
            """, (asin,))
            r = cursor.fetchone()
            pvp = float(r[0]) if r and r[0] else 0.0
            price_cache[asin] = pvp

        # INSERT
        cursor.execute("""
            INSERT INTO box_items (box_code, asin, quantity, pvp_ud)
            VALUES (%s, %s, %s, %s)
        """, (code, asin, units, pvp))

        filas_insertadas += 1
        total_units += units
        total_weight += weight

        cat = classify_reason(reason)
        if cat == 'overstock':
            overstock += units
        elif cat == 'devoluciones':
            devoluciones += units

    # --- UPDATE BOX ---
    pct_over = (overstock / total_units * 100) if total_units else 0
    pct_dev = (devoluciones / total_units * 100) if total_units else 0

    cursor.execute("""
        UPDATE boxes
        SET name=%s, units=%s, weight=%s,
            overstock=%s, devoluciones=%s
        WHERE code=%s
    """, (name, total_units, total_weight, pct_over, pct_dev, code))

    conn.commit()
    cursor.close()
    conn.close()

    if filas_insertadas == 0:
        print(
            f"⚠️ {code}: caja actualizada pero 0 líneas en box_items "
            f"(revisa ASIN vacíos o unidades ≤ 0 en el Excel)."
        )
    else:
        print(f"✅ {code} procesado | {filas_insertadas} líneas en box_items")


def main():
    print(f"📂 Raíz datos (ingest): {DATA_ROOT}")
    if not os.path.exists(PROCESSED_DIR):
        os.makedirs(PROCESSED_DIR)

    mapping = load_name_mapping()

    for file in os.listdir(SOURCE_DIR):
        if file.endswith('.xlsx') and not file.startswith('~$'):

            full = os.path.join(SOURCE_DIR, file)
            print(f"\n🔄 {file}")

            try:
                process_file(full, mapping)
                shutil.move(full, os.path.join(PROCESSED_DIR, file))
                print("📁 Movido a processed")

            except Exception as e:
                print(f"❌ Error: {e}")


if __name__ == "__main__":
    main()