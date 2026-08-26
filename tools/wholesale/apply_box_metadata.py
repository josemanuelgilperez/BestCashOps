#!/usr/bin/env python3
import argparse
import csv
import os
import sys
from pathlib import Path


REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)


def parse_args():
    parser = argparse.ArgumentParser(
        description="Aplica name/category de boxes desde un CSV codigo,titulo,categoria."
    )
    parser.add_argument(
        "--input",
        default="tools/data/new_pallet_categories.csv",
        help="CSV con columnas codigo,titulo,categoria.",
    )
    parser.add_argument(
        "--fail-on-missing",
        action="store_true",
        help="Falla si algun codigo del CSV no existe en boxes.",
    )
    return parser.parse_args()


def load_rows(path):
    rows = []
    with Path(path).open(encoding="utf-8-sig", newline="") as file_obj:
        reader = csv.DictReader(file_obj)
        required = {"codigo", "titulo", "categoria"}
        missing = required - set(reader.fieldnames or [])
        if missing:
            raise SystemExit(f"Faltan columnas en {path}: {sorted(missing)}")
        for row in reader:
            code = (row.get("codigo") or "").strip().upper()
            title = (row.get("titulo") or "").strip()
            category = (row.get("categoria") or "").strip()
            if code and title and category:
                rows.append((code, title, category))
    if not rows:
        raise SystemExit(f"No hay filas validas en {path}")
    return rows


def main():
    args = parse_args()

    from db import get_connection

    rows = load_rows(args.input)
    conn = get_connection()
    cur = conn.cursor()
    updated = 0
    unchanged = 0
    missing_codes = []
    try:
        for code, title, category in rows:
            cur.execute(
                "UPDATE boxes SET name=%s, category=%s WHERE code=%s",
                (title, category, code),
            )
            if cur.rowcount:
                updated += cur.rowcount
            else:
                cur.execute("SELECT 1 FROM boxes WHERE code=%s LIMIT 1", (code,))
                if cur.fetchone():
                    unchanged += 1
                else:
                    missing_codes.append(code)
        if args.fail_on_missing and missing_codes:
            conn.rollback()
            raise SystemExit(f"No existen en boxes: {', '.join(missing_codes)}")
        conn.commit()
    finally:
        cur.close()
        conn.close()

    print(f"input_rows={len(rows)}")
    print(f"updated_rows={updated}")
    print(f"unchanged_rows={unchanged}")
    print(f"missing_codes={','.join(missing_codes)}")


if __name__ == "__main__":
    main()
