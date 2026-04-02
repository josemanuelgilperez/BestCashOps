import os
import pandas as pd
import mysql.connector
from dotenv import load_dotenv

# Cargar variables de entorno si estás usando .env
load_dotenv()

# Configuración de la base de datos
destination_db_config = {
    'user': os.getenv('DB_USER', 'admin'),
    'password': os.getenv('DB_PASSWORD', '23092023BCdb'),
    'host': os.getenv('DB_HOST', 'bestcash.cvsxzrox0hah.eu-west-1.rds.amazonaws.com'),
    'database': os.getenv('DB_NAME', 'bestcash_rds')
}

# Ruta al archivo CSV exportado desde Google Sheets
TOOLS_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
csv_file = os.path.join(TOOLS_DIR, 'data', 'faltan.csv')

# Leer datos desde el CSV
df = pd.read_csv(csv_file)

# Conexión a la base de datos
conn = mysql.connector.connect(**destination_db_config)
cursor = conn.cursor()

# Contador de actualizaciones
updates = 0

# Iterar y actualizar la tabla
for index, row in df.iterrows():
    asin = str(row['asin']).strip()
    precio = row['precio']
    precio_coste = row['precio_coste']
    precio_amazon = row['precio_amazon']
    rate = row['rate']

    if asin:  # Solo si el ASIN no está vacío
        sql = """
        UPDATE amazon_scraped_products
        SET precio = %s,
            precio_coste = %s,
            precio_amazon = %s,
            rate = %s
        WHERE asin = %s
        """
        values = (precio, precio_coste, precio_amazon, rate, asin)
        cursor.execute(sql, values)
        updates += 1

# Confirmar cambios
conn.commit()
cursor.close()
conn.close()

print(f"{updates} filas actualizadas en amazon_scraped_products.")
