# process_size_color_missing.py
import os
import csv
import mysql.connector
from openai import OpenAI
import re
from dotenv import load_dotenv

# Cargar variables de entorno
load_dotenv()

# Configuración de conexión a la base de datos
destination_db_config = {
    'user': os.getenv('DB_USER', 'admin'),
    'password': os.getenv('DB_PASSWORD', '23092023BCdb'),
    'host': os.getenv('DB_HOST', 'bestcash.cvsxzrox0hah.eu-west-1.rds.amazonaws.com'),
    'database': os.getenv('DB_NAME', 'bestcash_rds')
}

# Inicializar OpenAI
client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

def get_completion(prompt):
    try:
        completion = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": "Eres un asistente experto en redacción y clasificación de productos. Responde siempre en español, de forma clara y precisa."},
                {"role": "user", "content": prompt}
            ]
        )
        return completion.choices[0].message.content.strip()
    except Exception as e:
        print(f"❌ Error con OpenAI: {e}")
        return ""

def extract_size_and_color(description):
    print(f"🧠 Solicitando a OpenAI para descripción: {description[:60]}...")
    prompt = f"""
El siguiente texto describe un producto, y puede estar en cualquier idioma (alemán, francés, italiano, etc.).

Tu tarea es:
1. Detectar la talla y el color del producto.
2. Traducir ambos al español (por ejemplo, "Schwarz" debe convertirse en "Negro").
3. Si no puedes identificar alguno de los dos datos, indica "N/A".

Responde solo con este formato exacto:

Talla: [valor o N/A]  
Color: [valor o N/A]

Texto del producto: {description}
"""
    response = get_completion(prompt)
    print(f"📝 Respuesta OpenAI: {response}")

    size_match = re.search(r'Talla:\s*(.*)', response)
    color_match = re.search(r'Color:\s*(.*)', response)
    size = size_match.group(1).strip() if size_match else "N/A"
    color = color_match.group(1).strip() if color_match else "N/A"
    return size, color

def get_box_code_from_filename(filepath):
    """
    Extrae el código del box desde el inicio del nombre: ML#### o MP####.
    Ejemplos válidos:
      ML0001 Tablas Termomix.csv
      MP0001 Ropa-Calzado Invierno.csv
    """
    base = os.path.basename(filepath)
    name_no_ext = re.sub(r'\.csv$', '', base, flags=re.IGNORECASE)

    m = re.match(r'^(M[PL]\d{4,})', name_no_ext, flags=re.IGNORECASE)
    if m:
        return m.group(1).upper()

    # Fallback (por si hubiera algún separador raro): busca en cualquier parte
    m2 = re.search(r'(M[PL]\d{4,})', name_no_ext, flags=re.IGNORECASE)
    if m2:
        return m2.group(1).upper()

    raise ValueError(f"No se pudo extraer box_code del archivo: {base}")

def needs_processing(db_size, db_color):
    """
    Devuelve True si hay que procesar (faltan size/color):
    - NULL / vacío / 'N/A' (cualquier combinación)
    """
    def empty_or_na(x):
        if x is None:
            return True
        s = str(x).strip()
        return (s == "" or s.upper() == "N/A")
    return empty_or_na(db_size) or empty_or_na(db_color)

def procesar_csv(filepath):
    try:
        box_code = get_box_code_from_filename(filepath)
    except ValueError as e:
        print(f"❌ {e}")
        return

    print(f"\n📦 Procesando archivo: {filepath} | Box code: {box_code}")

    try:
        connection = mysql.connector.connect(**destination_db_config)
        cursor = connection.cursor()
    except Exception as e:
        print(f"❌ Error conectando a la base de datos: {e}")
        return

    try:
        with open(filepath, newline='', encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile)
            rows = list(reader)
            print(f"📄 Filas en el archivo: {len(rows)}")

            # Recupera id, size, color para ese box_code en el mismo orden que antes (id ASC)
            cursor.execute("""
                SELECT id, size, color
                FROM box_items
                WHERE box_code = %s
                ORDER BY id ASC
            """, (box_code,))
            db_rows = cursor.fetchall()  # [(id, size, color), ...]

            if len(db_rows) != len(rows):
                print(f"⚠️ Atención: número de filas en base de datos ({len(db_rows)}) y en CSV ({len(rows)}) no coinciden.")
                return

            for idx, row in enumerate(rows):
                asin = row.get('Asin', '').strip()
                item_desc = row.get('ItemDesc', '').strip()

                record_id, db_size, db_color = db_rows[idx]

                print(f"\n➡️ [{idx+1}/{len(rows)}] ASIN: {asin} (ID: {record_id})")

                # Solo procesar si faltan talla o color
                if not needs_processing(db_size, db_color):
                    print("⏭️  Ya tiene talla y color. Se omite.")
                    continue

                size, color = extract_size_and_color(item_desc)
                print(f"🎯 Resultado → Talla: {size} | Color: {color}")

                try:
                    update_sql = "UPDATE box_items SET size = %s, color = %s WHERE id = %s"
                    cursor.execute(update_sql, (size, color, record_id))
                    connection.commit()
                    print("✅ Fila actualizada y guardada en la base de datos.")
                except Exception as e:
                    connection.rollback()
                    print(f"❌ Error al actualizar fila {idx+1} (id={record_id}): {e}")

    except Exception as e:
        print(f"❌ Error procesando el archivo {filepath}: {e}")
    finally:
        try:
            cursor.close()
            connection.close()
        except Exception:
            pass

def main():
    project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    lista_path = os.path.join(project_root, 'tools', 'data', 'lista_archivos.csv')
    base_folder = os.path.join(project_root, 'boxes', 'files', 'processed')

    if not os.path.exists(lista_path):
        print(f"❌ No se encontró el archivo lista: {lista_path}")
        return

    with open(lista_path, newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            filename = row['filename'].strip()
            full_path = os.path.join(base_folder, filename)
            print(f"\n🔍 Verificando archivo: {full_path}")
            if os.path.exists(full_path):
                procesar_csv(full_path)
            else:
                print(f"❌ Archivo no encontrado: {full_path}")

if __name__ == "__main__":
    main()
