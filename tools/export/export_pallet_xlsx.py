import os
from collections import defaultdict
from mysql.connector import pooling
from openpyxl import Workbook
import re

# ==============================
# CONFIGURACIÓN
# ==============================
DB_CONFIG = {
    "user": os.getenv("DB_USER", "admin"),
    "password": os.getenv("DB_PASSWORD", "23092023BCdb"),
    "host": os.getenv("DB_HOST", "bestcash.cvsxzrox0hah.eu-west-1.rds.amazonaws.com"),
    "database": os.getenv("DB_NAME", "bestcash_rds"),
}

OUTPUT_DIR = "output_xlsx"
os.makedirs(OUTPUT_DIR, exist_ok=True)

# ==============================
# MYSQL POOL
# ==============================
db_pool = pooling.MySQLConnectionPool(
    pool_name="bestcash_pool_xlsx_cliente",
    pool_size=5,
    **DB_CONFIG
)

def get_conn():
    return db_pool.get_connection()

# ==============================
# UTILIDADES
# ==============================
def calcular_precio_venta(total_pvp, discount):
    total_pvp = float(total_pvp or 0)
    discount = float(discount or 0)
    return round(total_pvp * 0.10 * (1 - discount / 100), 2)

# ==============================
# CARGA DATOS
# ==============================
def cargar_pallet(code):
    conn = get_conn()
    cur = conn.cursor(dictionary=True)

    cur.execute("""
        SELECT code, name, discount, category
        FROM boxes
        WHERE code = %s
    """, (code,))

    pallet = cur.fetchone()
    cur.close()
    conn.close()
    return pallet

def cargar_items(code):
    conn = get_conn()
    cur = conn.cursor(dictionary=True)

    cur.execute("""
        SELECT
            bi.asin, bi.quantity, bi.size, bi.color,
            asp.titulo_breve, asp.descripcion_tecnica,
            asp.precio, asp.precio_amazon,
            asp.imagen_principal
        FROM box_items bi
        LEFT JOIN amazon_scraped_products asp ON bi.asin = asp.asin
        WHERE bi.box_code = %s
    """, (code,))

    rows = cur.fetchall()
    cur.close()
    conn.close()
    return rows

# ==============================
# AGRUPAR PRODUCTOS
# ==============================
def agrupar_productos(rows):
    g = defaultdict(lambda: {
        "quantity": 0,
        "titulo_breve": "",
        "descripcion_tecnica": "",
        "pvp_ud": 0.0,
        "imagen_principal": "",
        "size": "",
        "color": ""
    })

    for r in rows:
        a = g[r["asin"]]
        q = int(r["quantity"] or 0)
        a["quantity"] += q

        a["titulo_breve"] = a["titulo_breve"] or r.get("titulo_breve") or ""
        a["descripcion_tecnica"] = a["descripcion_tecnica"] or r.get("descripcion_tecnica") or ""
        a["imagen_principal"] = a["imagen_principal"] or r.get("imagen_principal") or ""

        if not a["pvp_ud"]:
            raw = r.get("precio") or r.get("precio_amazon") or 0
            clean = re.sub(r"[^\d.,]", "", str(raw))
            a["pvp_ud"] = float(clean.replace(",", ".") or 0)

        if r.get("size") and r["size"] != "N/A":
            a["size"] = a["size"] or r["size"]

        if r.get("color") and r["color"] != "N/A":
            a["color"] = a["color"] or r["color"]

    return g

# ==============================
# EXPORTAR XLSX
# ==============================
def exportar_hoja_pallet(wb, pallet, productos):

    # Excel limita nombre de hoja a 31 caracteres
    sheet_name = pallet["code"][:31]

    ws = wb.create_sheet(title=sheet_name)

    ws.append([
        "Pallet Code",
        "Pallet Name",
        "Category",
        "ASIN",
        "Titulo",
        "Descripcion",
        "Cantidad",
        "PVP Unidad",
        "PVP Total",
        "Size",
        "Color",
        "Imagen URL"
    ])

    total_pvp = 0
    total_units = 0

    for asin, d in productos.items():
        q = d["quantity"]
        pvp_total = d["pvp_ud"] * q

        total_units += q
        total_pvp += pvp_total

        ws.append([
            pallet["code"],
            pallet["name"],
            pallet.get("category"),
            asin,
            d["titulo_breve"],
            d["descripcion_tecnica"],
            q,
            round(d["pvp_ud"], 2),
            round(pvp_total, 2),
            d["size"],
            d["color"],
            d["imagen_principal"]
        ])

    ws.append([])
    ws.append(["TOTAL UNIDADES", total_units])
    ws.append(["TOTAL PVP", round(total_pvp, 2)])
    ws.append(["PRECIO VENTA PALLET",
               calcular_precio_venta(total_pvp, pallet["discount"])])

def leer_codigos_desde_txt(ruta):
    if not os.path.exists(ruta):
        print("❌ Archivo TXT no encontrado")
        return []

    with open(ruta, "r", encoding="utf-8") as f:
        lines = f.readlines()

    return [line.strip().upper() for line in lines if line.strip()]

# ==============================
# MAIN
# ==============================
if __name__ == "__main__":

    modo = input("¿Usar TXT? (s/n): ").strip().lower()

    if modo == "s":
        ruta_txt = input("Ruta del archivo TXT: ").strip()
        codes = leer_codigos_desde_txt(ruta_txt)
    else:
        entrada = input("Códigos de pallet (separados por coma): ").strip().upper()
        codes = [c.strip() for c in entrada.split(",") if c.strip()]

    if not codes:
        print("❌ No se han proporcionado códigos")
        exit()

    print(f"\n📦 Pallets a procesar: {len(codes)}\n")

    wb = Workbook()
    wb.remove(wb.active)

    procesados = 0

    for code in codes:

        print(f"🔍 Procesando {code}...")

        pallet = cargar_pallet(code)
        if not pallet:
            print(f"   ❌ Pallet no encontrado")
            continue

        rows = cargar_items(code)
        productos = agrupar_productos(rows)

        if not productos:
            print(f"   ⚠️ Pallet sin productos")
            continue

        exportar_hoja_pallet(wb, pallet, productos)
        procesados += 1

    if procesados == 0:
        print("❌ No se generó ningún pallet válido")
        exit()

    filename = os.path.join(OUTPUT_DIR, "pallets_cliente.xlsx")
    wb.save(filename)

    print(f"\n✅ Archivo consolidado generado: {filename}")