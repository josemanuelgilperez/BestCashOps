# =================================================
# IMPORTS
# =================================================
import os
import sys
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

# =================================================
# CONSULTA DATOS FINANCIEROS POR PALLET
# =================================================
def obtener_datos_pallets():

    query = """
        SELECT 
            b.code,
            b.margin_percent,
            b.discount,
            IFNULL(SUM(
                bi.quantity *
                CAST(
                    REPLACE(
                        CASE 
                            WHEN asp.precio IS NOT NULL 
                                 AND asp.precio <> '' 
                                 AND asp.precio <> 'None'
                                THEN asp.precio
                            WHEN asp.precio_amazon IS NOT NULL 
                                 AND asp.precio_amazon <> '' 
                                 AND asp.precio_amazon <> 'None'
                                THEN asp.precio_amazon
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
                        WHEN asp.precio_coste IS NOT NULL 
                             AND asp.precio_coste <> '' 
                             AND asp.precio_coste <> 'None'
                            THEN CAST(REPLACE(asp.precio_coste, ',', '.') AS DECIMAL(10,2))
                        ELSE 0.07 *
                             CAST(
                                REPLACE(
                                    CASE 
                                        WHEN asp.precio IS NOT NULL 
                                             AND asp.precio <> '' 
                                             AND asp.precio <> 'None'
                                            THEN asp.precio
                                        WHEN asp.precio_amazon IS NOT NULL 
                                             AND asp.precio_amazon <> '' 
                                             AND asp.precio_amazon <> 'None'
                                            THEN asp.precio_amazon
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
        GROUP BY b.code, b.margin_percent, b.discount;
    """

    conn = get_connection()
    cursor = conn.cursor(dictionary=True)
    cursor.execute(query)
    rows = cursor.fetchall()
    cursor.close()
    conn.close()

    return rows


# =================================================
# RECÁLCULO Y UPDATE
# =================================================
def recalcular_finanzas():

    print("\n🚀 INICIO RECÁLCULO FINANCIERO PALLETS\n")

    datos = obtener_datos_pallets()

    conn = get_connection()
    cursor = conn.cursor()

    for d in datos:

        code = d['code']
        pvp_total = float(d['pvp_total'] or 0)
        coste_total = float(d['coste_total'] or 0)
        total_units = int(d['total_units'] or 0)

        margin = float(d['margin_percent'] or 10.0)
        discount = float(d['discount'] or 0.0)

        # =========================
        # PALLET
        # =========================
        base_price = round(pvp_total * (margin / 100), 2)
        precio_final = round(base_price * (1 - discount / 100), 2)
        rate = round((coste_total / pvp_total) * 100, 2) if pvp_total > 0 else 0.0

        cursor.execute("""
            UPDATE boxes
            SET units        = %s,
                pvp_total    = %s,
                base_price   = %s,
                precio_final = %s,
                cost         = %s,
                rate         = %s
            WHERE code = %s
        """, (
            total_units,
            pvp_total,
            base_price,
            precio_final,
            coste_total,
            rate,
            code
        ))

        # =========================
        # ITEMS (NUEVO BLOQUE)
        # =========================
        cursor.execute("""
            SELECT asin, pvp_ud
            FROM box_items
            WHERE box_code = %s
        """, (code,))

        items = cursor.fetchall()

        for asin, pvp_ud in items:

            pvp_ud = float(pvp_ud or 0)

            precio_lote_ud = round(
                pvp_ud * (margin / 100) * (1 - discount / 100),
                2
            )

            cursor.execute("""
                UPDATE box_items
                SET precio_lote_ud = %s
                WHERE box_code = %s AND asin = %s
            """, (precio_lote_ud, code, asin))

        print(f"✔ {code} actualizado")

    conn.commit()
    cursor.close()
    conn.close()

    print("\n🎉 FINANZAS COMPLETADAS\n")


# =================================================
# MAIN
# =================================================
if __name__ == "__main__":
    recalcular_finanzas()