import os
import time
import re
import mysql.connector
from openai import OpenAI

client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

DB_CONFIG = {
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD'),
    'host': os.getenv('DB_HOST'),
    'database': os.getenv('DB_NAME')
}

BATCH_SIZE = 50


# ======================================================
# PROMPT
# ======================================================
def construir_prompt(titulo, caracteristicas, asin):

    return f"""
Genera contenido optimizado para Wallapop.

Devuelve EXACTAMENTE en este formato:

[TITULO]
Título optimizado (máx 70 caracteres)
[/TITULO]

[DESCRIPCION]
Descripción optimizada con:
- bullets ✔️
- formato escaneable
- usos y público objetivo
- máximo 5 hashtags

Terminar con:
Ref. BestCash {asin}
[/DESCRIPCION]

DATOS:
Título Amazon: {titulo}
Características: {caracteristicas}

No inventar datos.
No mencionar Amazon.
"""


# ======================================================
# PARSER ROBUSTO
# ======================================================
def extraer_bloques(texto):

    titulo = re.search(r"\[TITULO\](.*?)\[/TITULO\]", texto, re.DOTALL)
    descripcion = re.search(r"\[DESCRIPCION\](.*?)\[/DESCRIPCION\]", texto, re.DOTALL)

    return (
        titulo.group(1).strip() if titulo else None,
        descripcion.group(1).strip() if descripcion else None
    )


# ======================================================
# OPENAI
# ======================================================
def generar_contenido(prompt):

    response = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[{"role": "user", "content": prompt}],
        temperature=0.7
    )

    return response.choices[0].message.content.strip()


# ======================================================
# MAIN
# ======================================================
def main():

    conn = mysql.connector.connect(**DB_CONFIG)
    cursor = conn.cursor(dictionary=True)

    cursor.execute(f"""
        SELECT asin, titulo_amazon, caracteristicas
        FROM amazon_scraped_products
        WHERE (descripcion IS NULL OR descripcion = '')
        AND titulo_amazon IS NOT NULL
        LIMIT {BATCH_SIZE}
    """)

    rows = cursor.fetchall()

    print(f"\n📦 Procesando {len(rows)} productos\n")

    for row in rows:

        asin = row["asin"]
        print(f"🔍 {asin}")

        prompt = construir_prompt(
            row["titulo_amazon"],
            row.get("caracteristicas", ""),
            asin
        )

        try:
            respuesta = generar_contenido(prompt)

            titulo, descripcion = extraer_bloques(respuesta)

            if not titulo or not descripcion:
                print("❌ Error parseo")
                continue

            titulo = titulo[:70]

            if len(descripcion) < 50:
                print("⚠️ Descripción corta")
                continue

            cursor.execute("""
                UPDATE amazon_scraped_products
                SET titulo_wallapop = %s,
                    descripcion = %s
                WHERE asin = %s
            """, (titulo, descripcion, asin))

            conn.commit()

            print("✅ OK")

            time.sleep(0.8)

        except Exception as e:
            print(f"❌ Error: {e}")
            conn.rollback()

    cursor.close()
    conn.close()


if __name__ == "__main__":
    main()