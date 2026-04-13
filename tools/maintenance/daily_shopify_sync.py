import os
import sys
import time
import glob
import shutil
import csv
import requests
import mysql.connector
from datetime import datetime

# =====================================================
# CONFIG
# =====================================================
SHOP = "bestcash-outlet.myshopify.com"
API_VERSION = "2024-10"
ACCESS_TOKEN = os.getenv("SHOPIFY_TOKEN")

if not ACCESS_TOKEN:
    print("❌ SHOPIFY_TOKEN not set")
    sys.exit(1)

BASE_URL = f"https://{SHOP}/admin/api/{API_VERSION}"
HEADERS = {
    "X-Shopify-Access-Token": ACCESS_TOKEN,
    "Content-Type": "application/json"
}

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CSV_DIR = os.path.join(BASE_DIR, "wallapop_input")
PROCESSED_DIR = os.path.join(CSV_DIR, "processed")

os.makedirs(CSV_DIR, exist_ok=True)
os.makedirs(PROCESSED_DIR, exist_ok=True)

REQUEST_TIMEOUT = 30
RATE_LIMIT_SLEEP = 0.4

# =====================================================
# DB CONNECTION
# =====================================================
def get_db_connection():
    return mysql.connector.connect(
        host="bestcash.cvsxzrox0hah.eu-west-1.rds.amazonaws.com",
        user="admin",
        password="23092023BCdb",
        database="bestcash_rds",
        autocommit=True
    )

try:
    db = get_db_connection()
    cursor = db.cursor(dictionary=True)
    print("✅ Database connected")
except Exception as e:
    print("❌ Database connection failed:", e)
    sys.exit(1)

# =====================================================
# GET LOCATION_ID
# =====================================================
cursor.execute("SELECT DISTINCT location_id FROM shopify_mapping LIMIT 1")
row = cursor.fetchone()

if not row:
    print("❌ No location_id found in shopify_mapping")
    sys.exit(1)

LOCATION_ID = row["location_id"]
print("📍 Using LOCATION_ID:", LOCATION_ID)

# =====================================================
# 1️⃣ PROCESS WALLAPOP CSVs
# =====================================================
csv_files = glob.glob(os.path.join(CSV_DIR, "*.csv"))
affected_asins = set()

if not csv_files:
    print("ℹ️ No Wallapop CSV files found.")

for csv_path in csv_files:
    print(f"\n📦 Processing CSV: {os.path.basename(csv_path)}")

    try:
        with open(csv_path, newline='', encoding='utf-8') as f:
            reader = csv.reader(f)

            for row in reader:
                if len(row) < 2:
                    print(f"⚠️ Skipping malformed row: {row}")
                    continue

                try:
                    asin = row[0].strip()
                    item_code = row[1].strip().zfill(8)

                    cursor.execute(
                        "DELETE FROM items_info WHERE code = %s",
                        (item_code,)
                    )

                    affected_asins.add(asin)

                except Exception as e:
                    print(f"⚠️ Error processing row {row}: {e}")

        shutil.move(
            csv_path,
            os.path.join(PROCESSED_DIR, os.path.basename(csv_path))
        )

        print("   ✔ CSV processed and moved")

    except Exception as e:
        print(f"❌ Error processing file {csv_path}: {e}")

print(f"🧮 ASINs affected by CSV: {len(affected_asins)}")


# =====================================================
# 2️⃣ RECALCULATE REAL STOCK
# =====================================================
print("\n🔎 Recalculating real stock...")

cursor.execute("""
    SELECT sm.sku, COUNT(ii.id) as stock
    FROM shopify_mapping sm
    LEFT JOIN products_info pi ON pi.asin = sm.sku
    LEFT JOIN references_info ri ON ri.product_id = pi.id
    LEFT JOIN items_info ii ON ii.reference_id = ri.id AND ii.ok_online = 1
    GROUP BY sm.sku;
    """)

real_stock = {row["sku"]: row["stock"] for row in cursor.fetchall()}
print(f"   ✔ Calculated stock for {len(real_stock)} ASINs")

# =====================================================
# 3️⃣ LOAD PREVIOUS SYNC STATE
# =====================================================
cursor.execute("SELECT asin, last_stock FROM stock_sync_log")
previous_stock = {row["asin"]: row["last_stock"] for row in cursor.fetchall()}

# =====================================================
# 4️⃣ DETECT DIFFERENCES
# =====================================================
asins_to_update = [
    (asin, stock)
    for asin, stock in real_stock.items()
    if previous_stock.get(asin) != stock
]

print(f"🔄 ASINs requiring update: {len(asins_to_update)}")

# =====================================================
# 5️⃣ UPDATE SHOPIFY
# =====================================================
for asin, stock_real in asins_to_update:

    try:
        cursor.execute("""
            SELECT shopify_product_id, inventory_item_id
            FROM shopify_mapping
            WHERE sku = %s
        """, (asin,))
        mapping = cursor.fetchone()

        if not mapping:
            print(f"⚠️ No mapping for {asin}")
            continue

        product_id = mapping["shopify_product_id"]
        inventory_item_id = mapping["inventory_item_id"]

        # ---- INVENTORY UPDATE ----
        inventory_payload = {
            "location_id": LOCATION_ID,
            "inventory_item_id": inventory_item_id,
            "available": stock_real
        }

        inv_response = requests.post(
            f"{BASE_URL}/inventory_levels/set.json",
            headers=HEADERS,
            json=inventory_payload,
            timeout=REQUEST_TIMEOUT
        )

        if inv_response.status_code == 429:
            print("⚠️ Rate limit hit. Sleeping 5 seconds...")
            time.sleep(5)
            continue

        if inv_response.status_code not in (200, 201):
            print(f"❌ Inventory update failed for {asin}: {inv_response.text}")
            continue

        # ---- STATUS UPDATE ----
        new_status = "active" if stock_real > 0 else "draft"

        status_payload = {
            "product": {
                "id": product_id,
                "status": new_status
            }
        }

        status_response = requests.put(
            f"{BASE_URL}/products/{product_id}.json",
            headers=HEADERS,
            json=status_payload,
            timeout=REQUEST_TIMEOUT
        )

        if status_response.status_code == 429:
            print("⚠️ Rate limit hit. Sleeping 5 seconds...")
            time.sleep(5)
            continue

        if status_response.status_code not in (200, 201):
            print(f"❌ Status update failed for {asin}: {status_response.text}")
            continue

        # ---- UPDATE LOG ----
        cursor.execute("""
            INSERT INTO stock_sync_log (asin, last_stock, last_synced_at)
            VALUES (%s, %s, %s)
            ON DUPLICATE KEY UPDATE
                last_stock = VALUES(last_stock),
                last_synced_at = VALUES(last_synced_at)
        """, (asin, stock_real, datetime.now()))

        print(f"   ✅ {asin} → stock {stock_real} → {new_status}")

        time.sleep(RATE_LIMIT_SLEEP)

    except requests.exceptions.RequestException as e:
        print(f"❌ Network error for {asin}: {e}")
    except mysql.connector.Error as e:
        print(f"❌ MySQL error for {asin}: {e}")
    except Exception as e:
        print(f"❌ Unexpected error for {asin}: {e}")

print("\n🎯 Daily Shopify sync completed.")

cursor.close()
db.close()
print("🔒 Database connection closed.")
