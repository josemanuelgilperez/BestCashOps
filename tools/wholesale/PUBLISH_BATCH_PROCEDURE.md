# Procedimiento de publicacion de tandas de pallets

Este procedimiento es el orden obligatorio para cada tanda nueva.

1. Validar el CSV recibido: `codigo,titulo,archivo_xlsx`.
2. Confirmar que todos los `.xlsx` existen y tienen cabeceras:
   `Asin,Units,ItemDesc,RemovalReason,TotalCost,TotalWeight (KG)`.
3. Actualizar `tools/data/names.csv`.
4. Crear `tools/data/new_pallet_categories_YYYYMMDD.csv` con:
   `codigo,titulo,categoria`.
5. Subir `names.csv` y los `.xlsx` limpios al servidor con:
   `tools/wholesale/deploy_boxes_inputs.sh`.
6. En servidor, ejecutar ingest, metadatos, bootstrap, rescrape, precios aproximados si faltan, finanzas, HTML y categorias.
7. Antes del FTP, escribir `wholesale/data/new_published_pallets.txt` con exactamente los codigos de la tanda.
8. Antes del FTP, ejecutar:
   `.venv/bin/python tools/wholesale/mark_new_lots.py --site wholesale/web/output --new-codes-file wholesale/data/new_published_pallets.txt`
9. Verificar en `wholesale/web/output`:
   - `index.html` contiene `Ver nuevos publicados (N)`.
   - `lotes/index.html` contiene `Nuevos (N)`.
   - `lotes/index.html` tiene `2 * N` apariciones de `data-new-lot="1"` porque hay tabla desktop y tarjetas mobile.
10. Ejecutar FTP.
11. Verificar publicamente con cache-buster:
   - `https://ventadelotes.bestcash.es/index.html?v=YYYYMMDD`
   - `https://ventadelotes.bestcash.es/lotes/index.html?v=YYYYMMDD`

Notas:
- El generador mayorista no crea siempre `web/output/index.html`; `mark_new_lots.py` descarga la portada publica actual si falta, actualiza el contador de nuevos y la incluye en el siguiente FTP.
- No usar el script Node `ventadelotes_add_new_filters.js` en servidor: Node no esta disponible en el PATH. El flujo soportado en servidor es `mark_new_lots.py`.
- `publish_wholesale.py --new-pallets` ya escribe `new_published_pallets.txt` y aplica `mark_new_lots.py` antes del FTP.
