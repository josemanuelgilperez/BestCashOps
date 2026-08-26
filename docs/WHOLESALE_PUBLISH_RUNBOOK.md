# Wholesale publish runbook

Guia operativa para colocar nuevos pallets mayoristas como disponibles en BestCash.

## Contexto

- VPS de trabajo: `root@212.227.90.202`
- Ruta del repo en el VPS: `/root/BestCashOps`
- Base de datos: `bestcash` en `82.223.203.117`
- Entrada de nuevos pallets en el VPS: `wholesale/data/new_box_files`
- Pallets ya ingeridos en el VPS: `wholesale/data/processed`
- Mapeo de nombres: `wholesale/data/names.csv`
- HTML generado: `wholesale/web/output`
- Publicacion FTP: `wholesale/scripts/upload_ftp.py`

El `.env` del VPS debe tener configuradas las variables `DB_*`, `OPENAI_API_KEY`,
`CRAWLBASE_TOKEN`, credenciales AWS/S3 y `FTP_*`. No guardar secretos en este
runbook.

## Resumen del flujo

1. Preparar en local los Excel de pallets y `names.csv`.
2. Subir esos inputs al VPS.
3. Ejecutar `ingest.py`: inserta/actualiza `boxes` y `box_items`, y mueve los
   Excel a `processed`.
4. Ejecutar `enrich.py`: crea fichas nuevas en `amazon_scraped_products`, sube
   imagenes a S3 cuando puede y rellena `box_items.pvp_ud`.
5. Revisar calidad de los nuevos pallets.
6. Ejecutar `publish_wholesale.py --new-pallets`: calcula finanzas, regenera
   HTML/categorias, vuelve a revisar calidad y sube por FTP.
7. Comprobar que la web publica muestra los pallets esperados.

## Preparacion local

En el equipo local, colocar:

- `tools/data/names.csv`
- `tools/data/new_box_files/*.xlsx`

`names.csv` debe tener al menos dos columnas sin cabecera obligatoria:

```text
codigo,titulo
MP1237,Titulo visible del pallet
```

Cada Excel debe contener el codigo del pallet en el nombre, por ejemplo:

```text
Pallet_MP1237_Items.xlsx
MP1237_ropa.xlsx
```

Columnas esperadas en cada Excel:

- `asin`
- `units`, `unidades`, `quantity`, `qty` o `cantidad`
- peso opcional: `totalweight (kg)`, `totalweight`, `weight`, `peso`, etc.
- motivo opcional: `removalreason`, `removal`, `reason`

## Subir inputs al VPS

Desde el repo local:

```bash
bash tools/wholesale/deploy_boxes_inputs.sh
```

El script:

- limpia `wholesale/data/new_box_files/*.xlsx` en el VPS
- limpia `wholesale/data/processed/*.xlsx` en el VPS
- sube `tools/data/names.csv` como `wholesale/data/names.csv`
- sube los Excel locales a `wholesale/data/new_box_files`

Si se quiere usar otra carpeta local:

```bash
bash tools/wholesale/deploy_boxes_inputs.sh \
  --local-names "/ruta/names.csv" \
  --local-xlsx-dir "/ruta/new_box_files"
```

## Entrar al VPS

```bash
ssh root@212.227.90.202
cd /root/BestCashOps
```

Comprobar entradas antes de ingerir:

```bash
ls -la wholesale/data/new_box_files
head -n 5 wholesale/data/names.csv
```

## Ingesta

```bash
.venv/bin/python wholesale/pipeline/ingest.py
```

Que hace:

- lee los Excel de `wholesale/data/new_box_files`
- extrae el codigo `MPxxxx` o `MLxxxx` del nombre del archivo
- usa `wholesale/data/names.csv` para asignar el nombre
- si el pallet no existe, crea la fila en `boxes`
- si el pallet ya existe, borra sus `box_items` anteriores y carga los nuevos
- actualiza unidades, peso, overstock/devoluciones
- mueve cada Excel procesado a `wholesale/data/processed`

Comprobar resultado:

```bash
ls -la wholesale/data/new_box_files
ls -lt wholesale/data/processed | head
```

Si un Excel falla, queda en `new_box_files`; corregir el archivo/nombre/columnas
y repetir `ingest.py`.

## Enriquecimiento

```bash
.venv/bin/python wholesale/pipeline/enrich.py
```

Importante: `enrich.py` no trabaja por `--new-pallets`. Busca ASIN de cualquier
pallet `Disponible` o `Reservado` que todavia no existan en
`amazon_scraped_products`.

Que hace:

- intenta scraping con Crawlbase en varios dominios Amazon
- genera textos/categoria con OpenAI cuando hay datos de producto
- usa `amazon_delivery` como fallback si no hay scraping
- sube imagenes a S3 si encuentra imagenes utiles
- inserta/actualiza `amazon_scraped_products`
- al final rellena `box_items.pvp_ud` desde scraping o `amazon_delivery`

Opciones utiles:

```bash
.venv/bin/python wholesale/pipeline/enrich.py --limit 20
.venv/bin/python wholesale/pipeline/enrich.py --skip-images
.venv/bin/python wholesale/pipeline/enrich.py --only-pvp-update
```

Si el proceso es largo, usar `tmux`:

```bash
tmux new-session -s wholesale_enrich
cd /root/BestCashOps
.venv/bin/python wholesale/pipeline/enrich.py
```

## Calidad antes de publicar

```bash
.venv/bin/python tools/wholesale/quality_report.py --new-pallets
```

Genera:

```text
tools/data/quality_missing_by_asin.tsv
tools/data/quality_summary_by_pallet.tsv
```

Campos principales:

- `falta_precio`: el ASIN no tiene precio util
- `falta_imagen`: no hay imagen util o aparece `image_not_found`
- `falta_ambos`: faltan precio e imagen
- `scraping_domain = similar_price`: precio estimado desde producto similar

Si hay demasiadas faltas, no publicar todavia. Reintentar faltantes o aplicar
precios similares segun los apartados siguientes.

## Publicar nuevos pallets

Validacion sin FTP:

```bash
.venv/bin/python tools/wholesale/publish_wholesale.py --new-pallets --no-ftp
```

Publicacion real:

```bash
.venv/bin/python tools/wholesale/publish_wholesale.py --new-pallets
```

Ese comando ejecuta:

```text
quality_report pre
finance.py --new-pallets
build_html.py
categories.py
quality_report post
upload_ftp.py
```

Notas:

- `finance.py --new-pallets` toma los codigos desde
  `wholesale/data/processed/*.xlsx`.
- `build_html.py` solo lista pallets con `show_pallet = 1` y estado
  `Disponible` o `Reservado`.
- `upload_ftp.py` sube `wholesale/web/output` y omite fichas de pallets con
  estado `Vendido`.

## Checks despues de publicar

En el VPS:

```bash
.venv/bin/python tools/wholesale/quality_report.py \
  --new-pallets \
  --output-prefix tools/data/quality_post_publish
```

Comprobar HTML generado:

```bash
ls -lt wholesale/web/output/lotes | head
ls -lt wholesale/web/output/categorias | head
```

Abrir en la web publica algunos pallets nuevos:

```text
https://www.bestcash.es/lotes/MP1237.html
```

## Publicar pallets concretos

Si solo han cambiado algunos pallets:

```bash
.venv/bin/python tools/wholesale/publish_wholesale.py --boxes MP1198,MP1230
```

Si han cambiado ASIN concretos y quieres recalcular los pallets afectados:

```bash
.venv/bin/python tools/wholesale/publish_wholesale.py --from-asins tools/data/asins_modificados.txt
```

## Recalculo financiero manual

Recalcular solo nuevos pallets:

```bash
.venv/bin/python wholesale/pipeline/finance.py --new-pallets
```

Recalcular pallets concretos:

```bash
.venv/bin/python wholesale/pipeline/finance.py --boxes MP1198,MP1230
```

Recalcular pallets afectados por una lista de ASIN:

```bash
.venv/bin/python wholesale/pipeline/finance.py --from-asins tools/data/asins_modificados.txt
```

Recalculo completo, solo si hace falta:

```bash
.venv/bin/python wholesale/pipeline/finance.py --full
```

## Re-scraping de faltantes

Para reintentar ASIN sin precio o imagen:

```bash
.venv/bin/python tools/wholesale/rescrape_missing_price_image.py \
  --from-txt tools/data/asins_faltantes.txt \
  --only-if-missing \
  --update-pvp
```

Para acotar dominios y evitar procesos largos:

```bash
.venv/bin/python tools/wholesale/rescrape_missing_price_image.py \
  --from-txt tools/data/asins_faltantes.txt \
  --only-if-missing \
  --update-pvp \
  --domains es,de,fr,it \
  --crawl-timeout 6 \
  --crawl-retries 1
```

## Precios por producto similar

Solo usar como ultimo recurso, y revisar candidatos antes de aplicar.

Generar sugerencias:

```bash
.venv/bin/python tools/wholesale/suggest_prices_from_similar_products.py
```

Aplicar seleccionados y coste al 7%:

```bash
.venv/bin/python tools/wholesale/apply_similar_prices.py --cost-rate 0.07
```

Despues recalcular los pallets afectados:

```bash
.venv/bin/python wholesale/pipeline/finance.py --from-asins tools/data/asins_modificados.txt
```

## HTML y FTP manual

Si no se usa `publish_wholesale.py`:

```bash
.venv/bin/python wholesale/web/build_html.py
.venv/bin/python wholesale/web/categories.py
.venv/bin/python wholesale/scripts/upload_ftp.py
```

## Actualizar estados de pallets

Para marcar pallets como `Disponible`, `Reservado` o `Vendido` de forma
incremental, usar:

```bash
.venv/bin/python wholesale/scripts/update_status_and_deploy.py
```

Entrada:

```text
wholesale/data/update_status.csv
```

Formato:

```text
code,status,reservado_para,reservado_por,fecha_reserva,fecha_venta
MP1237,Disponible,,,,
MP1238,Reservado,Cliente ejemplo,Jose Manuel,06/08/2026,
MP1239,Vendido,Cliente ejemplo,Jose Manuel,,06/08/2026
```

Reglas:

- `Disponible`: no lleva datos de reserva ni venta; el script limpia esos campos.
- `Reservado`: requiere `reservado_para`, `reservado_por` y `fecha_reserva`.
- `Vendido`: requiere `fecha_venta`; si se informan `reservado_para`,
  `reservado_por` o `fecha_reserva`, el script los guarda. Si esos campos
  vienen vacios, conserva los datos de reserva previos si los habia.

Las fechas aceptan `DD/MM`, `DD/MM/YYYY` o `YYYY-MM-DD`. Si se usa `DD/MM`, el
año se toma del año actual del entorno, o de `BESTCASH_STATUS_DATE_YEAR` si está
definido.

El script actualiza `boxes.status`, `boxes.reservado_para`,
`boxes.reservado_por`, `boxes.fecha_reserva` y `boxes.fecha_venta`; regenera
fichas/listados/categorias afectadas y sube por FTP solo esos archivos.

## Renombrar pallets desde la ficha

La ficha HTML permite renombrar pallets en modo admin. El cambio se guarda
inmediatamente en `boxes.name`.

Por seguridad, arrancar el API solo en localhost del VPS y acceder por tunel SSH.

En el VPS:

```bash
cd /root/BestCashOps
export PALLET_ADMIN_TOKEN="elige-un-token-largo"
.venv/bin/python tools/wholesale/pallet_admin_api.py --host 127.0.0.1 --port 8091
```

En el equipo local:

```bash
ssh -L 8091:127.0.0.1:8091 root@212.227.90.202
```

Abrir la ficha del pallet con `admin=1`, por ejemplo:

```text
https://www.bestcash.es/lotes/MP1237.html?admin=1
```

La primera vez pedira:

- URL API admin: `http://127.0.0.1:8091`
- Token admin: el valor de `PALLET_ADMIN_TOKEN`

Al guardar:

- actualiza `boxes.name` en la base de datos
- actualiza el titulo de la ficha abierta en el navegador
- los listados/categorias publicos quedan actualizados en la siguiente ejecucion
  de `publish_wholesale.py`

## Problemas habituales

- `No existe names.csv`: subir o corregir `tools/data/names.csv`.
- `Nombre invalido`: el Excel no contiene un codigo `MPxxxx` o `MLxxxx`.
- `No se encontraron columnas obligatorias`: revisar cabeceras del Excel.
- `enrich.py` tarda mucho: relanzar con `--limit`, `--skip-images` o en `tmux`.
- Muchos ASIN sin precio/imagen: usar reporte de calidad, re-scraping y, solo si
  procede, precios por producto similar.
- FTP falla: validar `FTP_*` en `.env` y repetir `publish_wholesale.py --new-pallets`
  o solo `wholesale/scripts/upload_ftp.py` si HTML/finanzas ya estan correctos.
