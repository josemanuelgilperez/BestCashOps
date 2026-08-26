# Por defecto solo recalcula pallets pendientes (datos NULL en boxes o precio_lote_ud NULL).
# Recálculo completo: python3 wholesale/pipeline/finance.py --full
# =================================================
# IMPORTS
# =================================================
import argparse
import os
import re
import sys
import time
from pathlib import Path
from dotenv import load_dotenv

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
REPO_ROOT = os.path.dirname(BASE_DIR)
for _p in (REPO_ROOT, BASE_DIR):
    if _p not in sys.path:
        sys.path.insert(0, _p)

from db import get_connection
load_dotenv()

# =================================================
# CONFIGURACIÓN BASE DE DATOS
# =================================================
db_config = None  # mantenido por compatibilidad, pero no se usa directamente


def _normalize_box_code(value):
    value = (value or "").strip().upper()
    return value if re.fullmatch(r"M[PL]\d+", value) else None


def _read_lines(path):
    values = []
    with open(path, encoding="utf-8") as file_obj:
        for line in file_obj:
            value = (line or "").strip()
            if value and not value.startswith("#"):
                values.append(value)
    return values


def _codes_from_processed_files():
    processed_dir = Path(REPO_ROOT) / "wholesale" / "data" / "processed"
    codes = set()
    for path in processed_dir.glob("*.xlsx"):
        match = re.search(r"(M[PL]\d+)", path.name, flags=re.IGNORECASE)
        if match:
            codes.add(match.group(1).upper())
    return sorted(codes)


def _codes_from_asins_file(path):
    asins = sorted({line.strip().upper() for line in _read_lines(path) if line.strip()})
    if not asins:
        return []

    conn = get_connection()
    cur = conn.cursor()
    try:
        placeholders = ",".join(["%s"] * len(asins))
        cur.execute(
            f"""
            SELECT DISTINCT box_code
            FROM box_items
            WHERE asin IN ({placeholders})
              AND box_code IS NOT NULL
              AND box_code <> ''
            ORDER BY box_code
            """,
            asins,
        )
        return [row[0] for row in cur.fetchall()]
    finally:
        cur.close()
        conn.close()


def resolve_scope_codes(boxes=None, from_asins=None, new_pallets=False):
    codes = set()
    for raw in boxes or []:
        for part in raw.split(","):
            code = _normalize_box_code(part)
            if code:
                codes.add(code)

    if from_asins:
        codes.update(_codes_from_asins_file(from_asins))

    if new_pallets:
        codes.update(_codes_from_processed_files())

    return sorted(codes)

# =================================================
# CONSULTA DATOS FINANCIEROS POR PALLET
# =================================================
def _filtro_solo_pendientes_sql():
    """
    Pallets que aún no tienen el bloque financiero aplicado en boxes
    o que tienen líneas sin precio_lote_ud (p. ej. nuevos ítems tras ingest).
    """
    return """
        AND (
            b.pvp_total IS NULL
            OR b.precio_final IS NULL
            OR b.base_price IS NULL
            OR b.cost IS NULL
            OR b.rate IS NULL
            OR EXISTS (
                SELECT 1 FROM box_items bi_need
                WHERE bi_need.box_code = b.code
                  AND bi_need.precio_lote_ud IS NULL
            )
        )
    """


def _filtro_codes_sql(codes):
    if not codes:
        return "", []
    placeholders = ",".join(["%s"] * len(codes))
    return f" AND b.code IN ({placeholders})", codes


def obtener_datos_pallets(full_recalc: bool = False, box_codes=None):
    code_filter, params = _filtro_codes_sql(box_codes or [])
    filtro = "" if full_recalc else _filtro_solo_pendientes_sql()
    filtro += code_filter

    query = f"""
        SELECT 
            b.code,
            b.margin_percent,
            b.discount,
            IFNULL(SUM(
                bi.quantity *
                CAST(
                    REPLACE(
                        CASE 
                            WHEN TRIM(COALESCE(asp.precio, '')) REGEXP '^-?[0-9]+([,.][0-9]+)?$'
                                THEN TRIM(asp.precio)
                            WHEN TRIM(COALESCE(asp.precio_amazon, '')) REGEXP '^-?[0-9]+([,.][0-9]+)?$'
                                THEN TRIM(asp.precio_amazon)
                            ELSE '0'
                        END
                    , ',', '.') AS DECIMAL(10,2)
                )
            ), 0) AS pvp_total,

            IFNULL(SUM(
                bi.quantity *
                (
                    CASE
                        WHEN ad.UnitRecovery IS NOT NULL 
                             AND ad.UnitRecovery > 0 
                            THEN ad.UnitRecovery
                        WHEN TRIM(COALESCE(asp.precio_coste, '')) REGEXP '^-?[0-9]+([,.][0-9]+)?$'
                            THEN CAST(REPLACE(TRIM(asp.precio_coste), ',', '.') AS DECIMAL(10,2))
                        ELSE 0.07 *
                             CAST(
                                REPLACE(
                                    CASE 
                                        WHEN TRIM(COALESCE(asp.precio, '')) REGEXP '^-?[0-9]+([,.][0-9]+)?$'
                                            THEN TRIM(asp.precio)
                                        WHEN TRIM(COALESCE(asp.precio_amazon, '')) REGEXP '^-?[0-9]+([,.][0-9]+)?$'
                                            THEN TRIM(asp.precio_amazon)
                                        ELSE '0'
                                    END
                                , ',', '.') AS DECIMAL(10,2)
                             )
                    END
                )
            ), 0) AS coste_total,

            IFNULL(SUM(bi.quantity),0) AS total_units

        FROM boxes b
        LEFT JOIN box_items bi ON b.code = bi.box_code
        LEFT JOIN amazon_scraped_products asp ON bi.asin = asp.asin
        LEFT JOIN (
            SELECT ASIN, MAX(UnitRecovery) AS UnitRecovery
            FROM amazon_delivery
            GROUP BY ASIN
        ) ad ON bi.asin = ad.ASIN

        WHERE b.status IN ('Disponible','Reservado')
        {filtro}
        GROUP BY b.code, b.margin_percent, b.discount;
    """

    conn = get_connection()
    cursor = conn.cursor(dictionary=True)
    cursor.execute(query, params)
    rows = cursor.fetchall()
    cursor.close()
    conn.close()

    return rows


# =================================================
# RECÁLCULO Y UPDATE
# =================================================
def recalcular_finanzas(full_recalc: bool = False, box_codes=None):
    try:
        sys.stdout.reconfigure(line_buffering=True)
    except Exception:
        pass

    t0 = time.perf_counter()
    scope = f" | scope={len(box_codes)} pallets" if box_codes else ""
    modo = "TODOS Disponible/Reservado (--full)" if full_recalc else "solo pendientes (sin datos financieros o ítems sin precio_lote_ud)"
    print(
        f"\n🚀 INICIO recálculo financiero — modo: {modo}{scope}\n"
        "   (boxes: PVP total, precio final… + box_items.precio_lote_ud)\n",
        flush=True,
    )

    print(
        "📡 Consultando agregados por pallet (JOIN grande; puede tardar varios segundos)…",
        flush=True,
    )
    t_q = time.perf_counter()
    datos = obtener_datos_pallets(full_recalc=full_recalc, box_codes=box_codes)
    print(
        f"📦 Pallets seleccionados: {len(datos)} ({time.perf_counter() - t_q:.1f}s)\n",
        flush=True,
    )

    if not datos:
        if full_recalc:
            msg = "no hay cajas Disponible/Reservado."
        else:
            msg = (
                "ningún pallet pendiente (todos tienen pvp_total/precio_final/base_price/cost/rate "
                "y precio_lote_ud en ítems). Usa --full para recalcular todos."
            )
        print(f"ℹ️  Sin filas: {msg}\n", flush=True)
        return

    conn = get_connection()
    cursor = conn.cursor()
    n_items_total = 0

    try:
        t_loop = time.perf_counter()
        for i, d in enumerate(datos, start=1):

            code = d["code"]
            pvp_total = float(d["pvp_total"] or 0)
            coste_total = float(d["coste_total"] or 0)
            total_units = int(d["total_units"] or 0)

            margin = float(d["margin_percent"] or 10.0)
            discount = float(d["discount"] or 0.0)

            # =========================
            # PALLET
            # =========================
            base_price = round(pvp_total * (margin / 100), 2)
            precio_final = round(base_price * (1 - discount / 100), 2)
            rate = round((coste_total / pvp_total) * 100, 2) if pvp_total > 0 else 0.0

            cursor.execute(
                """
                UPDATE boxes
                SET units        = %s,
                    pvp_total    = %s,
                    base_price   = %s,
                    precio_final = %s,
                    cost         = %s,
                    rate         = %s
                WHERE code = %s
                """,
                (
                    total_units,
                    pvp_total,
                    base_price,
                    precio_final,
                    coste_total,
                    rate,
                    code,
                ),
            )

            # =========================
            # ITEMS
            # =========================
            cursor.execute(
                """
                SELECT asin, pvp_ud
                FROM box_items
                WHERE box_code = %s
                """,
                (code,),
            )

            items = cursor.fetchall()
            n_items_total += len(items)

            for asin, pvp_ud in items:

                pvp_ud = float(pvp_ud or 0)

                precio_lote_ud = round(
                    pvp_ud * (margin / 100) * (1 - discount / 100),
                    2,
                )

                cursor.execute(
                    """
                    UPDATE box_items
                    SET precio_lote_ud = %s
                    WHERE box_code = %s AND asin = %s
                    """,
                    (precio_lote_ud, code, asin),
                )

            print(
                f"✔ [{i}/{len(datos)}] {code} | uds={total_units} | "
                f"PVP total={pvp_total:.2f} € | precio final={precio_final:.2f} € | "
                f"ítems={len(items)}",
                flush=True,
            )

        print("\n💾 Confirmando transacción (COMMIT)…", flush=True)
        conn.commit()
        print(
            f"\n🎉 Finanzas completadas | {len(datos)} pallets | {n_items_total} líneas en box_items | "
            f"bucle {time.perf_counter() - t_loop:.1f}s | total {time.perf_counter() - t0:.1f}s\n",
            flush=True,
        )
    except Exception as e:
        conn.rollback()
        print(f"\n❌ Error en recálculo financiero (ROLLBACK): {e}\n", flush=True)
        raise
    finally:
        cursor.close()
        conn.close()


# =================================================
# MAIN
# =================================================
if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Recálculo financiero de pallets (boxes + box_items.precio_lote_ud)."
    )
    parser.add_argument(
        "--full",
        action="store_true",
        help="Recalcular todos los pallets Disponible/Reservado (ignora incremental).",
    )
    parser.add_argument(
        "--boxes",
        action="append",
        help="Códigos de pallets separados por coma. Puede repetirse.",
    )
    parser.add_argument(
        "--from-asins",
        help="TXT con ASINs; recalcula los pallets que contengan esos ASINs.",
    )
    parser.add_argument(
        "--new-pallets",
        action="store_true",
        help="Recalcula los pallets detectados en wholesale/data/processed/*.xlsx.",
    )
    args = parser.parse_args()
    scope_codes = resolve_scope_codes(
        boxes=args.boxes,
        from_asins=args.from_asins,
        new_pallets=args.new_pallets,
    )
    recalcular_finanzas(full_recalc=args.full or bool(scope_codes), box_codes=scope_codes)
