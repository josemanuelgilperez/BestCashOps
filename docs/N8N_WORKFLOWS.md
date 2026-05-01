# N8N Workflows

Documento vivo para inventariar y describir los workflows de `n8n` usados en BestCash.

## Resumen

Estado actual conocido:

- Numero de workflows documentados: `1`
- Estado general: workflow existente pero aun no completado
- Servidor de `n8n`: `Servidor A` (`212.227.110.239`)
- Servidor de ejecucion de scripts: `Servidor B` (`212.227.90.202`)

## Workflow: Setup Wallapop Publish

### Estado

- Nombre en `n8n`: `Setup Wallapop Publish`
- Estado funcional: parcialmente implementado
- Objetivo inferido:
  - tomar productos marcados como pendientes para Wallapop desde una Google Sheet
  - preparar un CSV con productos para insertar en BestCash
  - copiar ese CSV al Servidor B
  - ejecutar un script Python en el repo `BestCashOps`
  - generar o alimentar una hoja auxiliar con datos enriquecidos para publicacion manual en Wallapop
  - incorporar una rama adicional para descarga de imagenes desde S3 a Dropbox remoto
  - marcar los items de origen como `PROCESADO` solo cuando la parte remota haya terminado correctamente

### Sistemas implicados

- `n8n` en Servidor A
- Google Sheets
- MySQL
- SSH entre servidores
- Repo `BestCashOps` en Servidor B

### Hojas de Google implicadas

#### Hoja de entrada

- Documento:
  - `https://docs.google.com/spreadsheets/d/1_5MVaQYjzRVgayvNxYjGEwT3G2agsob6nzEBIWnG2rY/edit`
- Pestaña:
  - `test`
- Uso:
  - leer filas de entrada
  - filtrar por `procesar_wallapop = PENDIENTE`
  - actualizar cada `item` a `procesar_wallapop = PROCESADO`

Columnas observadas en esta hoja:

- `item`
- `estantería`
- `asin`
- `precio`
- `procedencia`
- `procesar_wallapop`
- `estado producto`

#### Hoja de salida para publicacion

- Documento:
  - `https://docs.google.com/spreadsheets/d/1gBv5tF61ZYwXGuvPWWD1E0eMUz0_5E5AbELhEpoftt4/edit`
- Pestaña:
  - `PUBLICAR`
- Uso:
  - append de filas enriquecidas para preparacion manual de la publicacion en Wallapop

Columnas observadas en esta hoja:

- `asin`
- `titulo_breve`
- `descripcion`
- `UD.`
- `PVP (60%)`
- `peso`
- `peso_amazon`
- `dimensiones`
- `REF.`
- `marca`
- `titulo_amazon`
- `caracteristicas`
- `descripcion_tecnica`
- `estado`

### Flujo funcional

#### 1. Disparo

- Nodo: `Schedule Trigger`
- Cadencia deseada:
  - todos los dias a las `00:00`
- Nota operativa:
  - conviene fijar explicitamente la timezone del workflow a `Europe/Madrid` para que las `00:00` sean locales y no dependan de la timezone por defecto de la instancia

#### 2. Lectura de la hoja de entrada

- Nodo: `Get row(s) in sheet`
- Lee la hoja `test` del documento de entrada

#### 3. Filtro de filas pendientes

- Nodo: `If`
- Condicion:
  - `procesar_wallapop == "PENDIENTE"`

Desde aqui salen varias ramas en paralelo.

### Rama A: preparacion de CSV y ejecucion remota en Servidor B

#### 4A. Normalizacion minima de campos

- Nodo: `Edit Fields`
- Construye un registro con:
  - `item`
  - `asin`
  - `precio`
  - `tienda = 5`
  - `ok_online = 1`

#### 5A. Conversion a fichero

- Nodo: `Convert to File`
- Convierte los items a binario para preparar el CSV

#### 6A. Resolucion de path interno en n8n

- Nodo: `Code in JavaScript3`
- Construye una ruta de fichero interna a partir del almacenamiento binario:
  - base observada: `/home/node/.n8n/storage/...`

#### 7A. Copia al contenedor de n8n

- Nodo: `Execute a command1`
- Comando:

```bash
docker exec ai-stack-n8n-1 sh -c "cp {{$json.path}} /files/nuevos_productos.csv"
```

- Interpretacion:
  - copia el fichero binario generado al volumen `/files/nuevos_productos.csv` dentro del contenedor `n8n`
  - esto parece conectar con `/opt/n8n_files` del host

#### 8A. Copia del CSV al Servidor B

- Nodo: `Execute a command2`
- Comando:

```bash
scp /opt/n8n_files/nuevos_productos.csv root@212.227.90.202:/root/BestCashOps/tpv/nuevos_productos.csv
```

- Interpretacion:
  - mueve el CSV ya materializado desde el Servidor A al repo del Servidor B
  - el CSV debe contener todos los pendientes de insertar en `items_info`

#### 9A. Ejecucion del script Python remoto

- Nodo: `Execute a command`
- Comando:

```bash
ssh root@212.227.90.202 "pwd && ls /root/BestCashOps && sed -i '1s/^\\xEF\\xBB\\xBF//' /root/BestCashOps/tpv/nuevos_productos.csv && source /root/BestCashOps/venv/bin/activate && python3 /root/BestCashOps/tpv/insert_new_items_shops.py /root/BestCashOps/tpv/nuevos_productos.csv"
```

- Lo que hace:
  - conecta al Servidor B
  - sanea BOM UTF-8 del CSV si existe
  - activa el `venv` del repo
  - ejecuta:

```bash
python3 /root/BestCashOps/tpv/insert_new_items_shops.py /root/BestCashOps/tpv/nuevos_productos.csv
```

- Interpretacion:
  - inserta nuevos registros operativos en la base de datos del TPV para `shop_id = 5`
  - si el `asin` no existe en `products_info`, crea producto
  - si no existe referencia para ese producto, crea registro en `references_info`
  - si el `code` no existe en `items_info`, crea el item
  - si el `code` ya existe, lo salta

### Rama B: enriquecimiento desde Amazon scraped products

#### 4B. Construccion de query 1

- Nodo: `Code in JavaScript`
- Funcion:
  - toma los `asin` unicos
  - lanza error si no hay ASIN validos
  - construye esta query:

```sql
SELECT asin,titulo_amazon,caracteristicas,descripcion_tecnica,peso,peso_amazon,dimensiones,marca
FROM bestcash_rds.amazon_scraped_products
WHERE asin IN (...)
```

#### 5B. Ejecucion en MySQL

- Nodo: `Execute a SQL query`

### Rama C: enriquecimiento de precio y referencias

#### 4C. Construccion de query 2

- Nodo: `Code in JavaScript1`
- Funcion:
  - toma los `asin` unicos
  - lanza error si no hay ASIN validos
  - construye esta query:

```sql
SELECT
    pi.asin,
    ii.bestcash_price,
    COUNT(DISTINCT ii.code) AS num_codes_distintos,
    GROUP_CONCAT(DISTINCT ii.code ORDER BY ii.code SEPARATOR ',') AS codes
FROM bestcash_rds.products_info  AS pi
JOIN bestcash_rds.references_info AS ri ON ri.product_id  = pi.id
JOIN bestcash_rds.items_info      AS ii ON ii.reference_id = ri.id
WHERE pi.asin IN (...)
AND ii.shop_id = 5
GROUP BY pi.asin
ORDER BY pi.asin;
```

#### 5C. Ejecucion en MySQL

- Nodo: `Execute a SQL query1`

### Rama D: merge y construccion de hoja PUBLICAR

#### 6D. Merge por ASIN

- Nodo: `Merge`
- Configuracion:
  - `mode = combine`
  - cruce por campo `asin`
  - `joinMode = keepEverything`

#### 7D. Construccion del payload final

- Nodo: `Code in JavaScript2`
- Genera por cada ASIN:
  - `titulo_breve`: primeros 60 caracteres de `titulo_amazon`
  - `descripcion`: `descripcion_tecnica`
  - `UD.`: numero de codigos distintos
  - `PVP (60%)`: `bestcash_price * 0.6`
  - `peso`
  - `peso_amazon`
  - `dimensiones`
  - `REF.`: codigos concatenados con prefijo `'`
  - `marca`
  - `titulo_amazon`
  - `caracteristicas`
  - `descripcion_tecnica`
  - `estado = NUEVO`

#### 8D. Loteado

- Nodo: `Loop Over Items`
- `batchSize = 10`

#### 9D. Espera intermedia

- Nodo: `Wait`
- Observacion:
  - no se aprecia una condicion funcional clara; puede estar puesto para rate limiting o para ejecucion manual por lotes

#### 10D. Append en hoja PUBLICAR

- Nodo: `Append row in sheet`
- Inserta filas en la pestaña `PUBLICAR`
- Regla funcional confirmada:
  - no es necesario limpiar ni deduplicar la hoja antes de cada ejecucion
  - las nuevas filas deben anadirse al final

### Rama F: imagenes para publicacion

- Estado:
  - pendiente de integrar en el workflow
- Objetivo confirmado:
  - descarga de imagenes desde S3 a Dropbox remoto
- Nota:
  - esta rama forma parte del workflow deseado, pero no aparece todavia desarrollada en el export revisado
  - existe ya un script relacionado en el repo: `tools/maintenance/images_s3_to_dropbox.sh`
  - el script ya esta preparado para uso parametrico:
    - `--asins-file`
    - `--dropbox-base`
    - `--date-folder`
    - `--strict`

### Rama E: marcado de origen como procesado

#### 4E. Loteado de items originales

- Nodo: `Loop Over Items1`

#### 5E. Espera intermedia

- Nodo: `Wait1`
- Parametro observado:
  - `amount = 1`

#### 6E. Actualizacion de la hoja de entrada

- Nodo: `Append or update row in sheet`
- Matching por:
  - `item`
- Actualiza:
  - `procesar_wallapop = PROCESADO`
- Regla funcional confirmada:
  - este marcado debe ocurrir solo si el script remoto termina bien

### Dependencias y credenciales observadas

- Credencial Google Sheets:
  - `Google Sheets account`
- Credencial MySQL:
  - `MySQL account`
- Credencial SSH:
  - `SSH Password 212.227.110.239`

### Observaciones tecnicas

- El workflow trabaja con `shop_id = 5`.
- El nodo SSH final ejecuta comandos contra el Servidor B desde el entorno de `n8n`.
- Hay una dependencia fuerte del path:
  - `/opt/n8n_files/nuevos_productos.csv`
- Parece existir una dependencia entre el volumen `/files` del contenedor `n8n` y `/opt/n8n_files` del host.
- Este mecanismo se monto como solucion practica para mover el CSV desde el binario interno de `n8n` al sistema de ficheros del host.
- Queda pendiente revisar si esta es la forma mas simple y robusta o si puede simplificarse.
- El workflow mezcla dos objetivos:
- El workflow mezcla dos objetivos:
  - carga operativa de nuevos productos en BestCash
  - preparacion de material para publicacion manual en Wallapop
- Falta una tercera linea funcional:
  - preparacion o transferencia de imagenes para la publicacion
- La publicacion automatica en Wallapop seria deseable, pero no es el alcance actual del workflow.

### Partes que parecen incompletas o fragiles

- El schedule real no queda documentado claramente en el export.
- El horario deseado ya esta definido: todos los dias a las `00:00`.
- No hay manejo visible de errores ni ramas de fallback.
- La hoja `PUBLICAR` funciona por `append` y ese comportamiento ya esta confirmado como deseado.
- Los `Wait` y `Loop Over Items` parecen haber sido introducidos para evitar saturacion, pero no esta demostrado todavia que sean necesarios.
- `tpv/insert_new_items_shops.py` ya esta identificado funcionalmente: inserta productos, referencias e items en la base de datos del TPV.
- La publicacion automatica en Wallapop no forma parte del alcance actual.

### Preguntas abiertas para completar esta ficha

1. Revisar si la rama que marca `PROCESADO` depende de verdad del exito de la rama SSH o si ahora mismo puede ejecutarse en paralelo antes de tiempo.
2. Definir la condicion exacta de "exito real" antes de marcar `PROCESADO`.
3. Documentar e integrar la rama de imagenes S3 -> Dropbox remoto.
4. Revisar si `Loop Over Items` sigue siendo necesario en la rama Google Sheets tras haber quitado los `Wait`.

### Siguiente iteracion recomendada

1. Configurar el `Schedule Trigger` a diario a las `00:00` con timezone `Europe/Madrid`.
2. Encadenar el marcado `PROCESADO` despues del exito de:
   - insercion remota en TPV
   - generacion de filas en `PUBLICAR`
   - rama de imagenes S3 -> Dropbox, si se considera obligatoria para el exito global
3. Integrar la rama de imagenes reutilizando como base `tools/maintenance/images_s3_to_dropbox.sh`.
