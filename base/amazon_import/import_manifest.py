import csv
import os
import glob
import re
from datetime import datetime
import mysql.connector
from dotenv import find_dotenv, load_dotenv

load_dotenv(find_dotenv())

# -----------------------------
# CONFIGURACIÓN BASE DE DATOS
# -----------------------------
db = mysql.connector.connect(
    host=os.getenv("DB_HOST", "82.223.203.117"),
    user=os.getenv("DB_USER", "bestcash_app"),
    password=os.getenv("DB_PASSWORD", "Bc_TPV_2026!kjDERZtm#82"),
    database=os.getenv("DB_NAME", "bestcash")
)
cursor = db.cursor()

# -----------------------------
# DIRECTORIO DE ENTRADA
# -----------------------------
DIRECTORIO = os.path.join(os.path.dirname(__file__), "procesar")
PROCESADOS_DIR = os.path.join(os.path.dirname(__file__), "procesados")
os.makedirs(PROCESADOS_DIR, exist_ok=True)
EXTENSION = "*.txt"
tabla_destino = "amazon_delivery"
MANIFEST_RE = re.compile(
    r"^Liq_FBA_WeeklyManifest_V3_(DE|ES|FR|IT)_(\d{8})_ND72B\.txt$"
)

# -----------------------------
# COLUMNAS DEL MANIFIESTO AMAZON (46)
# -----------------------------
cols_archivo = [
    "LiquidatorVendorCode","InventoryLocation","FC","IOG","RemovalReason",
    "ShipmentClosed","BOL","Carrier","ShipToCity","RemovalOrderID",
    "ReturnID","ItemId","ShipmentRequestID","PkgID","GL","GLDesc",
    "CategoryCode","CategoryDesc","SubcatCode","SubcatDesc","Asin",
    "UPC","EAN","FCSku","ItemDesc","Units","ItemPkgWeight",
    "ItemPkgWeightUOM","CostSource","CurrencyCode","UnitCost",
    "AmazonPrice","UnitRecovery","TotalCost","TotalRecovery",
    "RecoveryRate","RecoveryRateType","AdjTotalRecovery","AdjRecoveryRate",
    "AdjReason","FNSku","LPN","TaxAmount","InvoiceNumber",
    "CommodityCode","ExportControlCode"
]

cols_metadata = [
    "manifest_country",
    "manifest_date",
    "source_file",
    "imported_at",
]


def parse_manifest_filename(filename):
    match = MANIFEST_RE.match(filename)
    if not match:
        raise ValueError(
            "Nombre de manifiesto no reconocido. Formato esperado: "
            "Liq_FBA_WeeklyManifest_V3_<DE|ES|FR|IT>_<YYYYMMDD>_ND72B.txt"
        )

    country, manifest_date_raw = match.groups()
    manifest_date = datetime.strptime(manifest_date_raw, "%Y%m%d").date().isoformat()
    return country, manifest_date

# -----------------------------
# SQL BASE
# -----------------------------
cols_destino = cols_archivo + cols_metadata
placeholders = ",".join(["%s"] * len(cols_destino))
col_names = ",".join(cols_destino)
sql = f"INSERT IGNORE INTO {tabla_destino} ({col_names}) VALUES ({placeholders})"

# -----------------------------
# PROCESAR ARCHIVOS EN "procesar/"
# -----------------------------
archivos = sorted(glob.glob(os.path.join(DIRECTORIO, EXTENSION)))
if not archivos:
    print(f"❌ No se encontraron archivos .txt en el directorio '{DIRECTORIO}'.")
    cursor.close()
    db.close()
    exit(0)

print(f"📂 Directorio de trabajo: {DIRECTORIO}")
print(f"📦 Archivos encontrados: {len(archivos)}")
cursor.execute(f"SELECT COUNT(*) FROM {tabla_destino}")
row_count_before = cursor.fetchone()[0]
print(f"📊 Filas actuales en {tabla_destino}: {row_count_before}")
print("-------------------------------------------------------------")

resumen = []
for archivo in archivos:
    total, insertadas = 0, 0
    unidades = 0
    asins = set()
    nombre = os.path.basename(archivo)
    print(f"\n➡️ Procesando: {nombre}")

    try:
        manifest_country, manifest_date = parse_manifest_filename(nombre)
        imported_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        metadata = [manifest_country, manifest_date, nombre, imported_at]

        with open(archivo, "r", encoding="utf-8", errors="ignore") as f:
            reader = csv.reader(f, delimiter='\t', quotechar='"', escapechar='\\')
            header = next(reader)  # cabecera
            if header != cols_archivo:
                raise ValueError("La cabecera del archivo no coincide con el formato esperado.")

            asin_idx = cols_archivo.index("Asin")
            units_idx = cols_archivo.index("Units")
            batch = []

            for row in reader:
                if len(row) != len(cols_archivo):
                    raise ValueError(
                        f"Fila {total + 2} con {len(row)} columnas; se esperaban {len(cols_archivo)}."
                    )

                total += 1
                if row[asin_idx]:
                    asins.add(row[asin_idx])
                if row[units_idx]:
                    unidades += int(row[units_idx])

                batch.append(row + metadata)
                if len(batch) >= 500:
                    cursor.executemany(sql, batch)
                    db.commit()
                    insertadas += cursor.rowcount
                    batch = []

            if batch:
                cursor.executemany(sql, batch)
                db.commit()
                insertadas += cursor.rowcount

        duplicadas = total - insertadas
        print(
            f"   ✅ Fecha manifiesto: {manifest_date} | País: {manifest_country} | "
            f"Leídas: {total} | Insertadas nuevas: {insertadas} | Duplicadas: {duplicadas} | "
            f"ASIN distintos: {len(asins)} | Unidades: {unidades}"
        )
        resumen.append(
            {
                "nombre": nombre,
                "manifest_country": manifest_country,
                "manifest_date": manifest_date,
                "total": total,
                "insertadas": insertadas,
                "duplicadas": duplicadas,
                "asins": len(asins),
                "unidades": unidades,
            }
        )

        # mover archivo a procesados
        destino = os.path.join(PROCESADOS_DIR, nombre)
        if os.path.exists(destino):
            destino = os.path.join(PROCESADOS_DIR, f"{nombre}_{int(os.path.getmtime(archivo))}")

        os.rename(archivo, destino)
        print(f"   📁 Movido a procesados: {destino}")

    except Exception as e:
        print(f"   ❌ Error procesando {nombre}: {e}")

# -----------------------------
# RESUMEN FINAL
# -----------------------------
print("\n📊 RESUMEN FINAL")
print("-------------------------------------------------------------")
total_leidas = sum(item["total"] for item in resumen)
total_insertadas = sum(item["insertadas"] for item in resumen)
total_duplicadas = sum(item["duplicadas"] for item in resumen)
total_unidades = sum(item["unidades"] for item in resumen)

for item in resumen:
    print(
        f"{item['nombre']:45} → Fecha: {item['manifest_date']} | País: {item['manifest_country']} | "
        f"Nuevas: {item['insertadas']:5d} | Duplicadas: {item['duplicadas']:5d} | "
        f"ASIN: {item['asins']:5d} | Unidades: {item['unidades']:5d}"
    )
print("-------------------------------------------------------------")
print(f"✅ Archivos procesados: {len(resumen)}")
print(f"✅ Filas leídas: {total_leidas}")
print(f"✅ Filas insertadas nuevas: {total_insertadas}")
print(f"✅ Filas duplicadas: {total_duplicadas}")
print(f"✅ Unidades totales leídas: {total_unidades}")

cursor.execute(f"SELECT COUNT(*) FROM {tabla_destino}")
row_count_after = cursor.fetchone()[0]
print(f"✅ Filas en {tabla_destino} antes/después: {row_count_before} / {row_count_after}")

source_files = [item["nombre"] for item in resumen]
if source_files:
    print("\n📊 INFORME AMAZON_DELIVERY POR MANIFIESTO")
    print("-------------------------------------------------------------")
    report_placeholders = ",".join(["%s"] * len(source_files))
    cursor.execute(
        f"""
        SELECT
            manifest_date,
            manifest_country,
            source_file,
            COUNT(*) AS rows_db,
            COUNT(DISTINCT ItemId) AS itemids_db,
            COUNT(DISTINCT Asin) AS asins_db,
            COALESCE(SUM(Units), 0) AS units_db,
            MIN(ShipmentClosed) AS shipment_min,
            MAX(ShipmentClosed) AS shipment_max
        FROM {tabla_destino}
        WHERE source_file IN ({report_placeholders})
        GROUP BY manifest_date, manifest_country, source_file
        ORDER BY manifest_date, manifest_country, source_file
        """,
        source_files,
    )
    report_rows = cursor.fetchall()
    for (
        manifest_date,
        manifest_country,
        source_file,
        rows_db,
        itemids_db,
        asins_db,
        units_db,
        shipment_min,
        shipment_max,
    ) in report_rows:
        print(
            f"{source_file:45} → Fecha: {manifest_date} | País: {manifest_country} | "
            f"Filas DB: {rows_db:5d} | ItemId: {itemids_db:5d} | ASIN: {asins_db:5d} | "
            f"Unidades DB: {int(units_db):5d} | ShipmentClosed: {shipment_min}..{shipment_max}"
        )
    print("-------------------------------------------------------------")

cursor.close()
db.close()
