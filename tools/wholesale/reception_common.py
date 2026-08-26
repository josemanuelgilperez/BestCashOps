import os
import secrets
import sys
from collections import Counter
from decimal import Decimal


REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from db import get_connection


VALID_UNIT_STATUSES = {"pending", "received", "missing", "damaged"}
INCIDENT_STATUSES = {"missing", "damaged"}


def make_token():
    return secrets.token_urlsafe(48)[:64]


def read_pallet_codes(path):
    with open(path, "r", encoding="utf-8") as fh:
        return [line.strip().upper() for line in fh if line.strip()]


def run_schema(schema_path=None):
    schema_path = schema_path or os.path.join(os.path.dirname(__file__), "reception_schema.sql")
    with open(schema_path, "r", encoding="utf-8") as fh:
        statements = [part.strip() for part in fh.read().split(";") if part.strip()]

    conn = get_connection()
    cur = conn.cursor()
    try:
        for statement in statements:
            cur.execute(statement)
        conn.commit()
    finally:
        cur.close()
        conn.close()


def ensure_client(name, token=None):
    name = " ".join((name or "").strip().split())
    if not name:
        raise ValueError("El nombre del cliente no puede estar vacio.")

    conn = get_connection()
    cur = conn.cursor(dictionary=True)
    try:
        cur.execute("SELECT id, name, token, status FROM wholesale_clients WHERE name = %s LIMIT 1", (name,))
        row = cur.fetchone()
        if row:
            if row["status"] != "active":
                cur.execute("UPDATE wholesale_clients SET status = 'active' WHERE id = %s", (row["id"],))
                conn.commit()
                row["status"] = "active"
            return row

        token = token or make_token()
        cur.execute(
            "INSERT INTO wholesale_clients (name, token) VALUES (%s, %s)",
            (name, token),
        )
        conn.commit()
        return {"id": cur.lastrowid, "name": name, "token": token, "status": "active"}
    finally:
        cur.close()
        conn.close()


def get_client_by_token(token):
    conn = get_connection()
    cur = conn.cursor(dictionary=True)
    try:
        cur.execute(
            """
            SELECT id, name, token, status
            FROM wholesale_clients
            WHERE token = %s AND status = 'active'
            LIMIT 1
            """,
            (token,),
        )
        return cur.fetchone()
    finally:
        cur.close()
        conn.close()


def get_or_create_client_pallet(client_id, box_code):
    conn = get_connection()
    cur = conn.cursor(dictionary=True)
    try:
        cur.execute(
            """
            SELECT cp.id, wc.name AS client_name
            FROM client_pallets cp
            JOIN wholesale_clients wc ON wc.id = cp.client_id
            WHERE cp.box_code = %s
              AND cp.client_id <> %s
              AND cp.status = 'active'
              AND wc.status = 'active'
            LIMIT 1
            """,
            (box_code, client_id),
        )
        existing_owner = cur.fetchone()
        if existing_owner:
            raise ValueError(
                f"{box_code} ya esta asignado a {existing_owner['client_name']}."
            )

        cur.execute(
            """
            SELECT id, client_id, box_code, status
            FROM client_pallets
            WHERE client_id = %s AND box_code = %s
            LIMIT 1
            """,
            (client_id, box_code),
        )
        row = cur.fetchone()
        if row:
            if row["status"] != "active":
                cur.execute("UPDATE client_pallets SET status = 'active' WHERE id = %s", (row["id"],))
                conn.commit()
                row["status"] = "active"
            return row

        cur.execute(
            "INSERT INTO client_pallets (client_id, box_code) VALUES (%s, %s)",
            (client_id, box_code),
        )
        conn.commit()
        return {"id": cur.lastrowid, "client_id": client_id, "box_code": box_code, "status": "active"}
    finally:
        cur.close()
        conn.close()


def load_pallet_items(box_code):
    conn = get_connection()
    cur = conn.cursor(dictionary=True)
    try:
        cur.execute(
            """
            SELECT
              bi.box_code,
              bi.asin,
              SUM(bi.quantity) AS quantity,
              MAX(asp.titulo_breve) AS title,
              MAX(asp.imagen_principal) AS image
            FROM box_items bi
            LEFT JOIN amazon_scraped_products asp ON asp.asin = bi.asin
            WHERE bi.box_code = %s
            GROUP BY bi.box_code, bi.asin
            ORDER BY bi.asin
            """,
            (box_code,),
        )
        return cur.fetchall()
    finally:
        cur.close()
        conn.close()


def ensure_reception_units(client_pallet_id, box_code):
    rows = load_pallet_items(box_code)
    conn = get_connection()
    cur = conn.cursor()
    created = 0
    try:
        for row in rows:
            asin = (row.get("asin") or "").strip()
            quantity = int(row.get("quantity") or 0)
            if not asin or quantity <= 0:
                continue

            for index in range(1, quantity + 1):
                cur.execute(
                    """
                    INSERT IGNORE INTO pallet_reception_units
                      (client_pallet_id, box_code, asin, unit_index, unit_total)
                    VALUES (%s, %s, %s, %s, %s)
                    """,
                    (client_pallet_id, box_code, asin, index, quantity),
                )
                created += cur.rowcount

            cur.execute(
                """
                UPDATE pallet_reception_units
                SET unit_total = %s
                WHERE client_pallet_id = %s AND asin = %s
                """,
                (quantity, client_pallet_id, asin),
            )
        conn.commit()
        return created
    finally:
        cur.close()
        conn.close()


def image_url(raw):
    if not raw:
        return ""
    raw = str(raw)
    if raw.startswith("http://") or raw.startswith("https://"):
        return raw
    return f"https://bestcashproductimages.s3.amazonaws.com/{raw.lstrip('/')}"


def normalize_number(value, default=0):
    if value is None:
        return default
    if isinstance(value, Decimal):
        return float(value)
    return value


def reception_label(counts):
    total = sum(counts.values())
    incidents = counts.get("missing", 0) + counts.get("damaged", 0)
    touched = counts.get("received", 0) + incidents
    if incidents:
        return "Con incidencias"
    if total and counts.get("received", 0) == total:
        return "Completado"
    if touched:
        return "En curso"
    return "Pendiente"


def load_client_pallets(client_id):
    conn = get_connection()
    cur = conn.cursor(dictionary=True)
    try:
        cur.execute(
            """
            SELECT
              cp.id AS client_pallet_id,
              cp.box_code,
              b.name,
              b.category,
              COUNT(pru.id) AS total_units,
              SUM(pru.status = 'pending') AS pending_units,
              SUM(pru.status = 'received') AS received_units,
              SUM(pru.status = 'missing') AS missing_units,
              SUM(pru.status = 'damaged') AS damaged_units
            FROM client_pallets cp
            LEFT JOIN boxes b ON b.code = cp.box_code
            LEFT JOIN pallet_reception_units pru ON pru.client_pallet_id = cp.id
            WHERE cp.client_id = %s AND cp.status = 'active'
            GROUP BY cp.id, cp.box_code, b.name, b.category
            ORDER BY cp.box_code
            """,
            (client_id,),
        )
        pallets = []
        for row in cur.fetchall():
            counts = Counter(
                {
                    "pending": int(row.get("pending_units") or 0),
                    "received": int(row.get("received_units") or 0),
                    "missing": int(row.get("missing_units") or 0),
                    "damaged": int(row.get("damaged_units") or 0),
                }
            )
            total = int(row.get("total_units") or 0)
            done = counts["received"] + counts["missing"] + counts["damaged"]
            row["total_units"] = total
            row["done_units"] = done
            row["pending_units"] = counts["pending"]
            row["received_units"] = counts["received"]
            row["missing_units"] = counts["missing"]
            row["damaged_units"] = counts["damaged"]
            row["reception_status"] = reception_label(counts)
            row["progress_percent"] = int(round((done / total) * 100)) if total else 0
            pallets.append(row)
        return pallets
    finally:
        cur.close()
        conn.close()


def get_client_pallet(client_id, box_code):
    conn = get_connection()
    cur = conn.cursor(dictionary=True)
    try:
        cur.execute(
            """
            SELECT
              cp.id AS client_pallet_id,
              cp.box_code,
              b.name,
              b.category
            FROM client_pallets cp
            LEFT JOIN boxes b ON b.code = cp.box_code
            WHERE cp.client_id = %s AND cp.box_code = %s AND cp.status = 'active'
            LIMIT 1
            """,
            (client_id, box_code),
        )
        return cur.fetchone()
    finally:
        cur.close()
        conn.close()


def load_reception_units(client_pallet_id):
    conn = get_connection()
    cur = conn.cursor(dictionary=True)
    try:
        cur.execute(
            """
            SELECT
              pru.id,
              pru.box_code,
              pru.asin,
              pru.unit_index,
              pru.unit_total,
              pru.status,
              pru.note,
              asp.titulo_breve AS title,
              asp.imagen_principal AS image
            FROM pallet_reception_units pru
            LEFT JOIN amazon_scraped_products asp ON asp.asin = pru.asin
            WHERE pru.client_pallet_id = %s
            ORDER BY pru.asin, pru.unit_index
            """,
            (client_pallet_id,),
        )
        rows = cur.fetchall()
        for row in rows:
            row["image_url"] = image_url(row.get("image"))
            row["title"] = row.get("title") or row.get("asin") or ""
        return rows
    finally:
        cur.close()
        conn.close()


def update_unit_status(client_id, unit_id, status, note=None):
    if status not in VALID_UNIT_STATUSES:
        raise ValueError("Estado de unidad no valido.")

    conn = get_connection()
    cur = conn.cursor(dictionary=True)
    try:
        cur.execute(
            """
            SELECT pru.id
            FROM pallet_reception_units pru
            JOIN client_pallets cp ON cp.id = pru.client_pallet_id
            WHERE pru.id = %s AND cp.client_id = %s AND cp.status = 'active'
            LIMIT 1
            """,
            (unit_id, client_id),
        )
        if not cur.fetchone():
            return False

        cur.execute(
            """
            UPDATE pallet_reception_units
            SET status = %s, note = %s
            WHERE id = %s
            """,
            (status, note, unit_id),
        )
        conn.commit()
        return True
    finally:
        cur.close()
        conn.close()
