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
from datetime import date, datetime
from pathlib import Path

from dotenv import load_dotenv

BASE_DIR = Path(__file__).resolve().parent.parent
REPO_ROOT = BASE_DIR.parent
for _p in (str(REPO_ROOT), str(BASE_DIR)):
    if _p not in sys.path:
        sys.path.insert(0, _p)

load_dotenv()

# Importar después de path
from db import get_connection
from slugify import slugify
from scripts.upload_ftp import subir_archivos_especificos, FTP_HOST

DATA_DIR = BASE_DIR / "data"
CSV_PATH = DATA_DIR / "update_status.csv"
DATE_DEFAULT_YEAR = int(os.getenv("BESTCASH_STATUS_DATE_YEAR", str(date.today().year)))


def _clean(v):
    if v is None:
        return ""
    return str(v).strip()


def _pick(row, *names):
    for name in names:
        if name in row:
            return _clean(row.get(name))
    lower = {str(k).strip().lower(): v for k, v in row.items()}
    for name in names:
        key = name.strip().lower()
        if key in lower:
            return _clean(lower.get(key))
    return ""


def _norm_status(v: str) -> str:
    if not v:
        return "Disponible"
    v = str(v).strip().lower()
    if v in ("disponible", "1", "true", "si", "sí"):
        return "Disponible"
    if v in ("reservado", "reservada"):
        return "Reservado"
    if v in ("vendido", "vendida"):
        return "Vendido"
    raise ValueError(f"Estado no reconocido: {v!r}")


def _parse_date(raw: str, *, row_num: int, field_name: str):
    raw = _clean(raw)
    if not raw:
        return None

    for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%d/%m/%y"):
        try:
            return datetime.strptime(raw, fmt).date()
        except ValueError:
            pass

    parts = raw.split("/")
    if len(parts) == 2:
        try:
            day, month = (int(p) for p in parts)
            return date(DATE_DEFAULT_YEAR, month, day)
        except ValueError:
            pass

    raise ValueError(
        f"Fila {row_num}: fecha invalida en {field_name}: {raw!r}. "
        "Usa DD/MM, DD/MM/YYYY o YYYY-MM-DD."
    )


def _require(value, *, row_num: int, field_name: str, status: str):
    value = _clean(value)
    if not value:
        raise ValueError(f"Fila {row_num}: {status} requiere {field_name}.")
    return value


def _iter_status_rows():
    with open(CSV_PATH, "r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        if not reader.fieldnames:
            return

        for row_num, row in enumerate(reader, start=2):
            code = _pick(row, "code", "codigo", "código", "pallet").upper()
            if not code:
                continue

            status = _norm_status(_pick(row, "status", "estado"))
            reservado_para = ""
            reservado_por = ""
            fecha_reserva = None
            fecha_venta = None

            if status == "Reservado":
                reservado_para = _require(
                    _pick(row, "reservado_para", "reservado para"),
                    row_num=row_num,
                    field_name="reservado_para",
                    status=status,
                )
                reservado_por = _require(
                    _pick(row, "reservado_por", "reservado por"),
                    row_num=row_num,
                    field_name="reservado_por",
                    status=status,
                )
                fecha_reserva = _parse_date(
                    _require(
                        _pick(row, "fecha_reserva", "fecha de reserva"),
                        row_num=row_num,
                        field_name="fecha_reserva",
                        status=status,
                    ),
                    row_num=row_num,
                    field_name="fecha_reserva",
                )
            elif status == "Vendido":
                reservado_para = _pick(row, "reservado_para", "reservado para")
                reservado_por = _pick(row, "reservado_por", "reservado por")
                raw_fecha_reserva = _pick(row, "fecha_reserva", "fecha de reserva")
                if raw_fecha_reserva:
                    fecha_reserva = _parse_date(
                        raw_fecha_reserva,
                        row_num=row_num,
                        field_name="fecha_reserva",
                    )
                fecha_venta = _parse_date(
                    _require(
                        _pick(row, "fecha_venta", "fecha de venta", "fecha vendido"),
                        row_num=row_num,
                        field_name="fecha_venta",
                        status=status,
                    ),
                    row_num=row_num,
                    field_name="fecha_venta",
                )

            yield {
                "row_num": row_num,
                "code": code,
                "status": status,
                "reservado_para": reservado_para or None,
                "reservado_por": reservado_por or None,
                "fecha_reserva": fecha_reserva,
                "fecha_venta": fecha_venta,
            }


def _ensure_status_metadata_columns(cur):
    cur.execute(
        """
        SELECT COLUMN_NAME
        FROM information_schema.COLUMNS
        WHERE TABLE_SCHEMA = DATABASE()
          AND TABLE_NAME = 'boxes'
          AND COLUMN_NAME IN (
              'reservado_para',
              'reservado_por',
              'fecha_reserva',
              'fecha_venta'
          )
        """
    )
    existing = {r[0] for r in cur.fetchall()}
    additions = 0
    if "reservado_para" not in existing:
        cur.execute("ALTER TABLE boxes ADD COLUMN reservado_para VARCHAR(150) NULL AFTER status")
        existing.add("reservado_para")
        additions += 1
    if "reservado_por" not in existing:
        cur.execute("ALTER TABLE boxes ADD COLUMN reservado_por VARCHAR(150) NULL AFTER reservado_para")
        existing.add("reservado_por")
        additions += 1
    if "fecha_reserva" not in existing:
        cur.execute("ALTER TABLE boxes ADD COLUMN fecha_reserva DATE NULL AFTER reservado_por")
        existing.add("fecha_reserva")
        additions += 1
    if "fecha_venta" not in existing:
        cur.execute("ALTER TABLE boxes ADD COLUMN fecha_venta DATE NULL AFTER fecha_reserva")
        existing.add("fecha_venta")
        additions += 1

    if additions:
        print(f"🧱 Columnas de estado añadidas a boxes: {additions}")


def actualizar_estados_desde_csv():
    """
    Actualiza boxes.status y sus metadatos y devuelve el set de códigos afectados.
    Si no hay CSV, devuelve set vacío.
    """
    if not CSV_PATH.exists():
        print("ℹ️ No existe wholesale/data/update_status.csv, nada que hacer")
        return set()

    rows = list(_iter_status_rows())
    if not rows:
        print("ℹ️ CSV de estados vacío, nada que hacer")
        return set()

    conn = get_connection()
    cur = conn.cursor()
    _ensure_status_metadata_columns(cur)
    affected = set()

    for row in rows:
        code = row["code"]
        status = row["status"]
        if status == "Vendido":
            cur.execute(
                """
                UPDATE boxes
                SET status = %s,
                    reservado_para = COALESCE(%s, reservado_para),
                    reservado_por = COALESCE(%s, reservado_por),
                    fecha_reserva = COALESCE(%s, fecha_reserva),
                    fecha_venta = %s
                WHERE code = %s
                """,
                (
                    status,
                    row["reservado_para"],
                    row["reservado_por"],
                    row["fecha_reserva"],
                    row["fecha_venta"],
                    code,
                ),
            )
        else:
            cur.execute(
                """
                UPDATE boxes
                SET status = %s,
                    reservado_para = %s,
                    reservado_por = %s,
                    fecha_reserva = %s,
                    fecha_venta = %s
                WHERE code = %s
                """,
                (
                    status,
                    row["reservado_para"],
                    row["reservado_por"],
                    row["fecha_reserva"],
                    row["fecha_venta"],
                    code,
                ),
            )
        if cur.rowcount == 0:
            cur.execute("SELECT 1 FROM boxes WHERE code=%s", (code,))
            if cur.fetchone() is None:
                conn.rollback()
                cur.close()
                conn.close()
                raise ValueError(f"Fila {row['row_num']}: no existe boxes.code={code!r}.")
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

    from web.build_html import (
        cargar_pallets,
        cargar_pallets_por_codigos_todos,
        cargar_items_por_codigos,
        generar_ficha_pallet,
        generar_resumen,
        pallet_a_dataset_entry,
        agrupar_items,
        json_safe,
        slug_categoria,
        tmpl_lotes,
        OUTPUT_DIR,
        LOTES_DIR,
    )
    from web.categories import generar_paginas_categoria

    # Pallets afectados con cualquier estado: su ficha debe reflejar el estado nuevo.
    pallets_afectados_todos = cargar_pallets_por_codigos_todos(affected_codes)
    codes_a_regenerar = {p["code"] for p in pallets_afectados_todos}

    # 1) Fichas individuales para todos los del CSV, incluido Vendido.
    archivos_subir = []
    if pallets_afectados_todos:
        items = agrupar_items(cargar_items_por_codigos(codes_a_regenerar))
        for p in pallets_afectados_todos:
            generar_ficha_pallet(p, items.get(p["code"], []))
            archivos_subir.append(f"lotes/{p['code']}.html")
        print(f"📄 Regeneradas {len(pallets_afectados_todos)} fichas de pallet")

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
