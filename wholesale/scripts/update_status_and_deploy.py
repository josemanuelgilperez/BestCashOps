#!/usr/bin/env python3
# =================================================
# UPDATE STATUS + REBUILD INCREMENTAL + FTP
# =================================================
# 1) Lee wholesale/data/update_status.csv y actualiza boxes.status
# 2) Regenera SOLO las páginas afectadas por los códigos del CSV:
#    - lotes/index.html, resumen_general.html (índices generales)
#    - lotes/{code}.html solo para los del CSV que están Disponible/Reservado
#    - categorias/{slug}.html solo para las categorías de los códigos afectados
# 3) Sube por FTP solo esos archivos
#
# Uso: python scripts/update_status_and_deploy.py
# =================================================

import os
import sys
import csv
import json
from pathlib import Path

from dotenv import load_dotenv

BASE_DIR = Path(__file__).resolve().parent.parent
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

load_dotenv()

# Importar después de path
from db import get_connection
from web.build_html import (
    cargar_pallets,
    cargar_pallets_por_codigos,
    cargar_items_por_codigos,
    generar_ficha_pallet,
    generar_resumen,
    pallet_a_dataset_entry,
    agrupar_items,
    json_safe,
    slug_categoria,
    tmpl_lotes,
    tmpl_resumen,
    OUTPUT_DIR,
    LOTES_DIR,
)
from web.categories import generar_paginas_categoria
from slugify import slugify
from scripts.upload_ftp import subir_archivos_especificos, FTP_HOST

DATA_DIR = BASE_DIR / "data"
CSV_PATH = DATA_DIR / "update_status.csv"


def _norm_status(v: str) -> str:
    if not v:
        return "Disponible"
    v = str(v).strip().lower()
    if v in ("disponible", "1", "true", "si", "sí"):
        return "Disponible"
    if v == "reservado":
        return "Reservado"
    return "Vendido"


def actualizar_estados_desde_csv():
    """
    Actualiza boxes.status y devuelve el set de códigos afectados.
    Si no hay CSV, devuelve set vacío.
    """
    if not CSV_PATH.exists():
        print("ℹ️ No existe wholesale/data/update_status.csv, nada que hacer")
        return set()

    conn = get_connection()
    cur = conn.cursor()
    affected = set()

    with open(CSV_PATH, "r", encoding="utf-8-sig") as f:
        reader = csv.reader(f)
        for row in reader:
            if len(row) >= 2:
                code = row[0].strip()
                if not code:
                    continue
                status = _norm_status(row[1])
                cur.execute("UPDATE boxes SET status=%s WHERE code=%s", (status, code))
                affected.add(code)

    conn.commit()
    cur.close()
    conn.close()
    print(f"✅ Estados actualizados: {len(affected)} cajas en CSV")
    return affected


def obtener_categorias_de_codigos(codes):
    """Devuelve los nombres de categoría de los códigos dados."""
    if not codes:
        return set()
    placeholders = ",".join(["%s"] * len(codes))
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        f"SELECT DISTINCT category FROM boxes WHERE code IN ({placeholders})",
        tuple(codes),
    )
    cats = set()
    for r in cur.fetchall():
        raw = r[0]
        cats.add(str(raw).strip() if raw else "Sin categoría")
    cur.close()
    conn.close()
    return cats


def build_incremental(affected_codes: set):
    """
    Regenera solo las páginas afectadas.
    affected_codes: códigos que aparecen en el CSV.
    """
    if not affected_codes:
        return []

    # Palettes a regenerar (solo Disponible/Reservado del CSV)
    pallets_afectados = cargar_pallets_por_codigos(affected_codes)
    codes_a_regenerar = {p["code"] for p in pallets_afectados}

    # 1) Fichas individuales solo para los del CSV que están Disponible/Reservado
    archivos_subir = []
    if pallets_afectados:
        items = agrupar_items(cargar_items_por_codigos(codes_a_regenerar))
        for p in pallets_afectados:
            generar_ficha_pallet(p, items.get(p["code"], []))
            archivos_subir.append(f"lotes/{p['code']}.html")
        print(f"📄 Regeneradas {len(pallets_afectados)} fichas de pallet")

    # 2) Lista completa para index y pallets.json
    todos_pallets = cargar_pallets()
    dataset = [pallet_a_dataset_entry(p) for p in todos_pallets]

    # pallets.json (categories lo usa; mantener consistencia)
    json_path = os.path.join(OUTPUT_DIR, "pallets.json")
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(json_safe(dataset), f, ensure_ascii=False, indent=2)

    # 3) Index lotes
    with open(os.path.join(LOTES_DIR, "index.html"), "w", encoding="utf-8") as f:
        f.write(tmpl_lotes.render(pallets=dataset))
    archivos_subir.append("lotes/index.html")

    # 4) Categorías afectadas
    categorias_afectadas = obtener_categorias_de_codigos(affected_codes)
    if categorias_afectadas:
        generar_paginas_categoria(dataset, solo_categorias=categorias_afectadas)
        for cat in categorias_afectadas:
            slug = slugify(cat) if cat else "sin-categoria"
            archivos_subir.append(f"categorias/{slug}.html")
        print(f"📁 Regeneradas {len(categorias_afectadas)} categorías")

    # 5) Resumen
    generar_resumen()
    archivos_subir.append("resumen_general.html")

    return list(dict.fromkeys(archivos_subir))  # sin duplicados, orden preservado


def main():
    affected = actualizar_estados_desde_csv()
    if not affected:
        return

    archivos = build_incremental(affected)

    ftp_user = os.getenv("FTP_USER")
    ftp_pass = os.getenv("FTP_PASS")
    if not ftp_user or not ftp_pass:
        print("ℹ️ FTP no configurado, omitiendo subida")
        return

    from ftplib import FTP

    print(f"\n📤 Subiendo {len(archivos)} archivos por FTP...")
    try:
        ftp = FTP(FTP_HOST, timeout=30)
        ftp.login(ftp_user, ftp_pass)
        ftp.set_pasv(True)
        n = subir_archivos_especificos(ftp, archivos)
        ftp.quit()
        print(f"\n🎉 {n} archivos subidos correctamente")
    except Exception as e:
        print(f"\n❌ Error FTP: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
