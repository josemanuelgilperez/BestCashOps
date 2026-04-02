import mysql.connector

from .config import DB_CONFIG


def get_connection():
    return mysql.connector.connect(**DB_CONFIG)


def get_asins(conn):
    cursor = conn.cursor(dictionary=True)
    cursor.execute("SELECT DISTINCT Asin FROM amazon_delivery WHERE Asin IS NOT NULL")
    results = [row["Asin"] for row in cursor.fetchall()]
    cursor.close()
    return results


def get_pending_asins(conn):
    cursor = conn.cursor(dictionary=True)
    cursor.execute(
        """
        SELECT DISTINCT d.Asin
        FROM amazon_delivery d
        WHERE d.Asin IS NOT NULL
          AND d.Asin <> ''
          AND NOT EXISTS (
              SELECT 1
              FROM amazon_scraped_products asp
              WHERE asp.asin = d.Asin
          )
        """
    )
    results = [row["Asin"] for row in cursor.fetchall()]
    cursor.close()
    return results


def filter_missing_asins(conn, asins):
    if not asins:
        return []

    deduped = []
    seen = set()
    for asin in asins:
        value = (asin or "").strip()
        if value and value not in seen:
            deduped.append(value)
            seen.add(value)

    missing = []
    cursor = conn.cursor()
    chunk_size = 500
    for idx in range(0, len(deduped), chunk_size):
        chunk = deduped[idx : idx + chunk_size]
        placeholders = ",".join(["%s"] * len(chunk))
        query = (
            f"SELECT asin FROM amazon_scraped_products WHERE asin IN ({placeholders})"
        )
        cursor.execute(query, tuple(chunk))
        existing = {row[0] for row in cursor.fetchall()}
        missing.extend([asin for asin in chunk if asin not in existing])
    cursor.close()
    return missing


def get_delivery(conn, asin):
    cursor = conn.cursor(dictionary=True)
    cursor.execute(
        """
        SELECT ItemDesc, UnitCost, UnitRecovery, RecoveryRate, ItemPkgWeight, GLDesc
        FROM amazon_delivery
        WHERE Asin=%s
        LIMIT 1
        """,
        (asin,),
    )
    row = cursor.fetchone()
    cursor.close()
    return row


def upsert_scraped_product(conn, data):
    cursor = conn.cursor()
    query = """
    INSERT INTO amazon_scraped_products (
        asin,
        scraping_domain,
        categoria,
        titulo_amazon,
        marca,
        precio,
        precio_coste,
        precio_amazon,
        rate,
        dimensiones,
        peso,
        peso_amazon,
        imagen_principal,
        imagenes_adicionales,
        caracteristicas,
        titulo_breve,
        descripcion,
        descripcion_tecnica,
        hashtags,
        handle,
        vendor,
        seo_title,
        seo_description,
        fecha_scraping
    ) VALUES (
        %(asin)s,
        %(scraping_domain)s,
        %(categoria)s,
        %(titulo_amazon)s,
        %(marca)s,
        %(precio)s,
        %(precio_coste)s,
        %(precio_amazon)s,
        %(rate)s,
        %(dimensiones)s,
        %(peso)s,
        %(peso_amazon)s,
        %(imagen_principal)s,
        %(imagenes_adicionales)s,
        %(caracteristicas)s,
        %(titulo_breve)s,
        %(descripcion)s,
        %(descripcion_tecnica)s,
        %(hashtags)s,
        %(handle)s,
        %(vendor)s,
        %(seo_title)s,
        %(seo_description)s,
        %(fecha_scraping)s
    )
    ON DUPLICATE KEY UPDATE
        scraping_domain = VALUES(scraping_domain),
        categoria = VALUES(categoria),
        titulo_amazon = VALUES(titulo_amazon),
        marca = VALUES(marca),
        precio = VALUES(precio),
        precio_coste = VALUES(precio_coste),
        precio_amazon = VALUES(precio_amazon),
        rate = VALUES(rate),
        dimensiones = VALUES(dimensiones),
        peso = VALUES(peso),
        peso_amazon = VALUES(peso_amazon),
        caracteristicas = VALUES(caracteristicas),
        titulo_breve = VALUES(titulo_breve),
        descripcion = VALUES(descripcion),
        descripcion_tecnica = VALUES(descripcion_tecnica),
        hashtags = VALUES(hashtags),
        imagen_principal = VALUES(imagen_principal),
        imagenes_adicionales = VALUES(imagenes_adicionales),
        handle = VALUES(handle),
        vendor = VALUES(vendor),
        seo_title = VALUES(seo_title),
        seo_description = VALUES(seo_description),
        fecha_scraping = VALUES(fecha_scraping)
    """
    cursor.execute(query, data)
    conn.commit()
    cursor.close()
