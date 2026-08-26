#!/usr/bin/env python3
"""Add Amazon manifest metadata columns used by import_manifest.py."""

import os

import mysql.connector
from dotenv import find_dotenv, load_dotenv

load_dotenv(find_dotenv())

TABLE = "amazon_delivery"

COLUMNS = [
    ("manifest_country", "CHAR(2) NULL AFTER ExportControlCode"),
    ("manifest_date", "DATE NULL AFTER manifest_country"),
    ("source_file", "VARCHAR(255) NULL AFTER manifest_date"),
    ("imported_at", "DATETIME NULL AFTER source_file"),
]

INDEXES = [
    ("idx_amazon_delivery_manifest", "(manifest_date, manifest_country)"),
    ("idx_amazon_delivery_source_file", "(source_file)"),
]


def main():
    db = mysql.connector.connect(
        host=os.getenv("DB_HOST", "82.223.203.117"),
        user=os.getenv("DB_USER", "bestcash_app"),
        password=os.getenv("DB_PASSWORD", ""),
        database=os.getenv("DB_NAME", "bestcash"),
    )
    cursor = db.cursor()

    cursor.execute(f"SHOW COLUMNS FROM {TABLE}")
    existing_columns = {row[0] for row in cursor.fetchall()}

    for column, definition in COLUMNS:
        if column in existing_columns:
            print(f"Column already exists: {column}")
            continue

        cursor.execute(f"ALTER TABLE {TABLE} ADD COLUMN {column} {definition}")
        print(f"Added column: {column}")

    cursor.execute(f"SHOW INDEX FROM {TABLE}")
    existing_indexes = {row[2] for row in cursor.fetchall()}

    for index_name, index_columns in INDEXES:
        if index_name in existing_indexes:
            print(f"Index already exists: {index_name}")
            continue

        cursor.execute(f"ALTER TABLE {TABLE} ADD INDEX {index_name} {index_columns}")
        print(f"Added index: {index_name}")

    db.commit()
    cursor.close()
    db.close()


if __name__ == "__main__":
    main()
