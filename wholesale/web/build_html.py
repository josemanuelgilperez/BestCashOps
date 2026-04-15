# =================================================
# BUILD HTML — BESTCASH (CORREGIDO)
# =================================================

import os
import sys
import shutil
from collections import defaultdict
from jinja2 import Environment, FileSystemLoader
from slugify import slugify
import json
from decimal import Decimal
from dotenv import load_dotenv

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
REPO_ROOT = os.path.dirname(BASE_DIR)
for _p in (REPO_ROOT, BASE_DIR):
    if _p not in sys.path:
        sys.path.insert(0, _p)

from db import get_pool, DB_CONFIG

load_dotenv()

TEMPLATE_DIR = os.path.join(BASE_DIR, "web", "templates")
OUTPUT_DIR = os.path.join(BASE_DIR, "web", "output")
LOTES_DIR = os.path.join(OUTPUT_DIR, "lotes")
CATEGORIAS_DIR = os.path.join(OUTPUT_DIR, "categorias")
ASSETS_SRC = os.path.join(BASE_DIR, "web", "assets")
ASSETS_DST = os.path.join(OUTPUT_DIR, "assets")

os.makedirs(OUTPUT_DIR, exist_ok=True)
os.makedirs(ASSETS_DST, exist_ok=True)
os.makedirs(LOTES_DIR, exist_ok=True)
os.makedirs(CATEGORIAS_DIR, exist_ok=True)

def _sync_assets_to_output():
    """
    Copia todo wholesale/web/assets → output/assets (ficheros y subcarpetas como img/).
    Rutas en plantillas: lotes/* y categorias/* usan ../assets/; resumen_general usa assets/.
    """
    if not os.path.isdir(ASSETS_SRC):
        return
    os.makedirs(ASSETS_DST, exist_ok=True)
    for name in os.listdir(ASSETS_SRC):
        src = os.path.join(ASSETS_SRC, name)
        dst = os.path.join(ASSETS_DST, name)
        if os.path.isdir(src):
            shutil.copytree(src, dst, dirs_exist_ok=True)
        elif os.path.isfile(src):
            shutil.copy2(src, dst)


_sync_assets_to_output()

# =================================================
# TEMPLATES
# =================================================
env = Environment(loader=FileSystemLoader(TEMPLATE_DIR))
tmpl_lotes = env.get_template("lotes.html")
tmpl_pallet = env.get_template("pallet_detail.html")
tmpl_resumen = env.get_template("summary_template.html")

# =================================================
# MYSQL POOL (compartido)
# =================================================
db_pool = get_pool(pool_name="bestcash_pool", pool_size=5)


def get_conn():
    return db_pool.get_connection()

# =================================================
# UTILIDADES
# =================================================
def norm_status(v):
    if not v:
        return "Disponible"
    v = str(v).strip().lower()
    if v in ("disponible", "1", "true", "si", "sí"):
        return "Disponible"
    if v == "reservado":
        return "Reservado"
    return "Vendido"

def slug_categoria(cat):
    return slugify(cat) if cat else ""


def _coalesce_num(value, default=0.0):
    """Evita None en plantillas Jinja (format %.2f)."""
    if value is None:
        return default
    if isinstance(value, Decimal):
        return float(value)
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def pallet_a_dataset_entry(p):
    """Convierte un pallet de BD al formato dataset (para index) sin generar HTML."""
    raw = p.get("category")
    cat = str(raw).strip() if raw else "Sin categoría"
    slug = slug_categoria(cat)
    return {
        "code": p["code"],
        "name": p["name"],
        "category_name": cat,
        "category_page": f"../categorias/{slug}.html",
        "total_units": p.get("total_units") or 0,
        "pvp_total": _coalesce_num(p.get("pvp_total")),
        "precio_final": _coalesce_num(p.get("precio_final")),
        "status": norm_status(p.get("status")),
        "discount": _coalesce_num(p.get("discount")),
        "weight": p.get("weight"),
        "devoluciones": p.get("devoluciones"),
        "overstock": p.get("overstock"),
        "filename": f"lotes/{p['code']}.html",
    }


def json_safe(obj):
    if isinstance(obj, Decimal):
        return float(obj)
    if isinstance(obj, dict):
        return {k: json_safe(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [json_safe(v) for v in obj]
    return obj


# =================================================
# CARGA PALLETS
# =================================================
def cargar_pallets():
    conn = get_conn()
    cur = conn.cursor(dictionary=True)

    cur.execute("""
        SELECT
            b.code,
            b.name,
            b.status,
            b.category,
            b.units AS total_units,
            b.pvp_total,
            b.precio_final,
            b.discount,
            b.weight,
            b.devoluciones,
            b.overstock
        FROM boxes b
        WHERE b.show_pallet = 1
          AND b.status IN ('Disponible', 'Reservado')
    """)

    rows = cur.fetchall()
    cur.close()
    conn.close()

    print(f"📦 Pallets encontrados: {len(rows)}")
    return rows


def cargar_pallets_por_codigos(codes):
    """Carga pallets solo para los códigos dados (Disponible o Reservado)."""
    if not codes:
        return []
    placeholders = ",".join(["%s"] * len(codes))
    conn = get_conn()
    cur = conn.cursor(dictionary=True)
    cur.execute(f"""
        SELECT
            b.code, b.name, b.status, b.category,
            b.units AS total_units, b.pvp_total, b.precio_final,
            b.discount, b.weight, b.devoluciones, b.overstock
        FROM boxes b
        WHERE b.show_pallet = 1
          AND b.status IN ('Disponible', 'Reservado')
          AND b.code IN ({placeholders})
    """, tuple(codes))
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return rows


def cargar_pallets_por_codigos_todos(codes):
    """Carga pallets para los códigos dados con cualquier estado público."""
    if not codes:
        return []
    placeholders = ",".join(["%s"] * len(codes))
    conn = get_conn()
    cur = conn.cursor(dictionary=True)
    cur.execute(f"""
        SELECT
            b.code, b.name, b.status, b.category,
            b.units AS total_units, b.pvp_total, b.precio_final,
            b.discount, b.weight, b.devoluciones, b.overstock
        FROM boxes b
        WHERE b.show_pallet = 1
          AND b.code IN ({placeholders})
    """, tuple(codes))
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return rows


def cargar_items_por_codigos(codes):
    """Carga items solo para los box_code dados."""
    if not codes:
        return []
    placeholders = ",".join(["%s"] * len(codes))
    conn = get_conn()
    cur = conn.cursor(dictionary=True)
    cur.execute(f"""
        SELECT bi.box_code, bi.asin, bi.quantity, bi.size, bi.color,
               bi.pvp_ud, bi.precio_lote_ud,
               asp.titulo_breve, asp.descripcion_tecnica, asp.imagen_principal
        FROM box_items bi
        LEFT JOIN amazon_scraped_products asp ON bi.asin = asp.asin
        WHERE bi.box_code IN ({placeholders})
    """, tuple(codes))
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return rows


# =================================================
# CARGA BOXES PARA RESUMEN (con coste_total)
# =================================================
def cargar_boxes_resumen():
    conn = get_conn()
    cur = conn.cursor(dictionary=True)
    cur.execute("""
        SELECT
            code,
            name,
            units AS total_units,
            pvp_total,
            cost AS coste_total,
            status
        FROM boxes
        WHERE status IN ('Disponible', 'Reservado', 'Vendido')
        ORDER BY code
    """)
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return rows


def generar_resumen():
    boxes = cargar_boxes_resumen()
    resumen_path = os.path.join(OUTPUT_DIR, "resumen_general.html")
    with open(resumen_path, "w", encoding="utf-8") as f:
        f.write(tmpl_resumen.render(boxes=boxes))
    print(f"📋 Resumen generado: {len(boxes)} cajas")


# =================================================
# CARGA ITEMS
# =================================================
def cargar_items():
    conn = get_conn()
    cur = conn.cursor(dictionary=True)

    cur.execute("""
        SELECT
            bi.box_code,
            bi.asin,
            bi.quantity,
            bi.size,
            bi.color,
            bi.pvp_ud,
            bi.precio_lote_ud,
            asp.titulo_breve,
            asp.descripcion_tecnica,
            asp.imagen_principal
        FROM box_items bi
        LEFT JOIN amazon_scraped_products asp ON bi.asin = asp.asin
    """)

    rows = cur.fetchall()
    cur.close()
    conn.close()

    print(f"📦 Items cargados: {len(rows)}")
    return rows

def agrupar_items(rows):
    agrupado = defaultdict(list)
    for r in rows:
        agrupado[r["box_code"]].append(r)
    return agrupado


def agregar_items_por_asin(items):
    """
    Agrupa items por ASIN y suma las unidades. Evita filas duplicadas en la tabla.
    Devuelve una lista con un único registro por ASIN y quantity = suma de todas las Uds.
    """
    por_asin = defaultdict(list)
    for it in items:
        asin = (it.get("asin") or "").strip()
        if not asin:
            continue
        por_asin[asin].append(it)

    agregados = []
    for asin, grupo in por_asin.items():
        total_qty = sum(int(it.get("quantity") or 0) for it in grupo)
        primero = grupo[0].copy()
        primero["quantity"] = total_qty
        agregados.append(primero)
    return agregados


# =================================================
# GENERACIÓN HTML PALLET
# =================================================
def generar_ficha_pallet(p, items):

    html_path = os.path.join(LOTES_DIR, f"{p['code']}.html")
    category_slug = slug_categoria(p["category"])

    items = agregar_items_por_asin(items)

    for it in items:

        # ✔ PRECIO DESDE BD (NO CÁLCULO)
        it["precio_cliente"] = it.get("precio_lote_ud")

        # ✔ IMAGEN
        img = it.get("imagen_principal")
        if img and str(img).startswith("http"):
            it["imagen_url"] = img
        elif img:
            it["imagen_url"] = f"https://bestcashproductimages.s3.amazonaws.com/{img}"
        else:
            it["imagen_url"] = None

    html = tmpl_pallet.render(
        pallet_code=p["code"],
        pallet_name=p["name"],
        items=items,
        total_units=p["total_units"] or 0,
        pvp_total=p["pvp_total"] or 0,
        precio_final=p["precio_final"] or 0,
        status=norm_status(p["status"]),
        discount=p["discount"],
        home_url="../index.html",
        categoria_url=f"../categorias/{category_slug}.html"
    )

    with open(html_path, "w", encoding="utf-8") as f:
        f.write(html)

    return {
        "code": p["code"],
        "name": p["name"],
        "category_name": p["category"],
        "category_page": f"../categorias/{category_slug}.html",
        "total_units": p.get("total_units") or 0,
        "pvp_total": _coalesce_num(p.get("pvp_total")),
        "precio_final": _coalesce_num(p.get("precio_final")),
        "status": norm_status(p["status"]),
        "discount": _coalesce_num(p.get("discount")),
        "weight": p.get("weight"),
        "devoluciones": p.get("devoluciones"),
        "overstock": p.get("overstock"),
        "filename": f"lotes/{p['code']}.html",
    }

# =================================================
# MAIN
# =================================================
if __name__ == "__main__":

    pallets = cargar_pallets()
    items = agrupar_items(cargar_items())

    dataset = []

    for p in pallets:
        res = generar_ficha_pallet(p, items.get(p["code"], []))
        if res:
            dataset.append(res)

    # JSON
    json_path = os.path.join(OUTPUT_DIR, "pallets.json")
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(json_safe(dataset), f, ensure_ascii=False, indent=2)

    # INDEX
    with open(os.path.join(LOTES_DIR, "index.html"), "w", encoding="utf-8") as f:
        f.write(tmpl_lotes.render(pallets=dataset))

    # RESUMEN
    generar_resumen()

    print("🎉 HTML generado correctamente (sin lógica de negocio)")
