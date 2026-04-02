# =================================================
# GENERATE CATEGORY PAGES — BESTCASH
# =================================================

import os
import json
from jinja2 import Environment, FileSystemLoader
from slugify import slugify

# =================================================
# BASE DIR
# =================================================
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

TEMPLATE_DIR = os.path.join(BASE_DIR, "web", "templates")
OUTPUT_DIR = os.path.join(BASE_DIR, "web", "output", "categorias")
DATA_FILE = os.path.join(BASE_DIR, "web", "output", "pallets.json")

os.makedirs(OUTPUT_DIR, exist_ok=True)

# =================================================
# LOAD DATA
# =================================================
def cargar_pallets():
    if not os.path.exists(DATA_FILE):
        raise RuntimeError("No existe pallets.json. Ejecuta build_html.py primero")

    with open(DATA_FILE, "r", encoding="utf-8") as f:
        return json.load(f)

# =================================================
# GENERATE
# =================================================
def generar_paginas_categoria(pallets, solo_categorias=None):
    """
    Genera páginas de categoría.
    solo_categorias: si se pasa, solo genera esas categorías (set o list de nombres).
    """
    env = Environment(loader=FileSystemLoader(TEMPLATE_DIR))
    template = env.get_template("index_category.html")

    categorias = sorted({
        p["category_name"]
        for p in pallets
        if p.get("category_name")
    })
    if solo_categorias is not None:
        solo = set(solo_categorias)
        categorias = [c for c in categorias if c in solo]

    for categoria in categorias:
        slug = slugify(categoria)

        subset = [p for p in pallets if p["category_name"] == categoria]

        html = template.render(
            pallets=subset,
            category_name=categoria,
            category_page=f"{slug}.html"
        )

        path = os.path.join(OUTPUT_DIR, f"{slug}.html")

        with open(path, "w", encoding="utf-8") as f:
            f.write(html)

        print(f"✔ {categoria} → {slug}.html")

# =================================================
# MAIN
# =================================================
if __name__ == "__main__":
    import glob
    for f in glob.glob(os.path.join(OUTPUT_DIR, "*.html")):
        try:
            os.remove(f)
        except OSError:
            pass

    pallets = cargar_pallets()
    generar_paginas_categoria(pallets)
    print("🎉 Categorías generadas correctamente")