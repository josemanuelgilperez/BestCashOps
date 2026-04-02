import csv
import os
import glob
import mysql.connector

# -----------------------------
# CONFIGURACIÓN BASE DE DATOS
# -----------------------------
db = mysql.connector.connect(
    host="bestcash.cvsxzrox0hah.eu-west-1.rds.amazonaws.com",
    user=os.getenv("DB_USER", "admin"),
    password=os.getenv("DB_PASSWORD", "23092023BCdb"),
    database=os.getenv("DB_NAME", "bestcash_rds")
)
cursor = db.cursor()

# -----------------------------
# DIRECTORIO DE ENTRADA
# -----------------------------
DIRECTORIO = os.path.join(os.path.dirname(__file__), "procesar")
EXTENSION = "*.txt"
tabla_destino = "amazon_delivery"

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

# -----------------------------
# SQL BASE
# -----------------------------
placeholders = ",".join(["%s"] * len(cols_archivo))
col_names = ",".join(cols_archivo)
sql = f"INSERT IGNORE INTO {tabla_destino} ({col_names}) VALUES ({placeholders})"

# -----------------------------
# PROCESAR ARCHIVOS EN "procesar/"
# -----------------------------
archivos = sorted(glob.glob(os.path.join(DIRECTORIO, EXTENSION)))
if not archivos:
    print(f"❌ No se encontraron archivos .txt en el directorio '{DIRECTORIO}'.")
    exit(1)

print(f"📂 Directorio de trabajo: {DIRECTORIO}")
print(f"📦 Archivos encontrados: {len(archivos)}")
print("-------------------------------------------------------------")

resumen = []
for archivo in archivos:
    total, insertadas = 0, 0
    nombre = os.path.basename(archivo)
    print(f"\n➡️ Procesando: {nombre}")

    try:
        with open(archivo, "r", encoding="utf-8", errors="ignore") as f:
            reader = csv.reader(f, delimiter='\t', quotechar='"', escapechar='\\')
            next(reader)  # cabecera
            batch = []

            for row in reader:
                total += 1
                batch.append(row)
                if len(batch) >= 500:
                    cursor.executemany(sql, batch)
                    db.commit()
                    insertadas += cursor.rowcount
                    batch = []

            if batch:
                cursor.executemany(sql, batch)
                db.commit()
                insertadas += cursor.rowcount

        print(f"   ✅ Leídas: {total} | Insertadas nuevas: {insertadas} | Duplicadas: {total - insertadas}")
        resumen.append((nombre, total, insertadas, total - insertadas))

    except Exception as e:
        print(f"   ❌ Error procesando {nombre}: {e}")

# -----------------------------
# RESUMEN FINAL
# -----------------------------
print("\n📊 RESUMEN FINAL")
print("-------------------------------------------------------------")
for nombre, total, insertadas, duplicadas in resumen:
    print(f"{nombre:45} → Nuevas: {insertadas:5d} | Duplicadas: {duplicadas:5d}")
print("-------------------------------------------------------------")
print(f"✅ Archivos procesados: {len(resumen)}")

cursor.close()
db.close()
