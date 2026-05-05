# N8N Workflows

Documento vivo para inventariar y describir los workflows de `n8n` usados en BestCash.

## Resumen

Estado actual conocido:

- Numero de workflows documentados: `2`
- Estado general: workflow operativo en produccion
- Servidor de `n8n`: `Servidor A` (`212.227.110.239`)
- Servidor de ejecucion de scripts: `Servidor B` (`212.227.90.202`)

## Workflow: Setup Wallapop Publish

### Estado

- Nombre en `n8n`: `Setup Wallapop Publish`
- Estado funcional: operativo en produccion
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
  - `stock`
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
- Lee la hoja `stock` del documento de entrada

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
- Uso actual en produccion:
  - se mantiene para evitar problemas cuando hay volumen alto de filas
  - el nodo `PUBLICAR | Append Rows to Sheet` devuelve el control de nuevo a `PUBLICAR | Batch Rows` para procesar todos los lotes, no solo el primero
  - esta correccion se introdujo al pasar de `test` a `stock`, al detectarse que con volumen alto solo se estaban procesando las primeras `10` filas

#### 9D. Append en hoja PUBLICAR

- Nodo: `Append row in sheet`
- Inserta filas en la pestaña `PUBLICAR`
- Regla funcional confirmada:
  - no es necesario limpiar ni deduplicar la hoja antes de cada ejecucion
  - las nuevas filas deben anadirse al final
  - en ejecuciones grandes, el append se realiza por lotes de `10` filas mediante loopback con `PUBLICAR | Batch Rows`
  - la salida de este nodo alimenta tanto el loopback del batch como el gate de exito del proceso

### Rama F: imagenes para publicacion

- Estado:
  - integrada y operativa en produccion
- Objetivo confirmado:
  - descarga de imagenes desde S3 a Dropbox remoto
- Nota:
  - existe ya un script relacionado en el repo: `tools/maintenance/images_s3_to_dropbox.sh`
  - el script ya esta preparado para uso parametrico:
    - `--asins-file`
    - `--dropbox-base`
    - `--date-folder`
    - `--strict`
  - en produccion se ejecuta desde el Servidor B con:

```bash
cd /root/BestCashOps && AWS_PROFILE=bestcash ./tools/maintenance/images_s3_to_dropbox.sh --asins-file /tmp/wallapop_asins.txt --dropbox-base /BESTCASH/WALLAPOP --date-folder "$(date +%F)"
```

### Rama E: marcado de origen como procesado

#### 4E. Restauracion de items originales

- Nodo: `PROCESADO | Restore Source Rows`
- Funcion:
  - recuperar todos los rows originales del lote despues del gate de exito `TPV + PUBLICAR`

#### 5E. Actualizacion de la hoja de entrada

- Nodo: `Append or update row in sheet`
- Matching por:
  - `item`
- Actualiza:
  - `procesar_wallapop = PROCESADO`
- Regla funcional confirmada:
  - este marcado debe ocurrir solo si terminan bien `TPV` y `PUBLICAR`
  - el campo `item` se mapea por expresion `{{$json["item"]}}` para que el matching contra la hoja `stock` funcione correctamente

### Dependencias y credenciales observadas

- Credencial Google Sheets:
  - `Google Sheets account`
- Credencial MySQL:
  - `MySQL account`
- Credencial SSH:
  - `SSH Password 212.227.110.239`

## Estado actual en produccion

Configuracion operativa actual confirmada:

- trigger diario a las `00:00`
- timezone del workflow: `Europe/Madrid`
- hoja de entrada activa: `stock`
- hoja de salida de publicacion: `PUBLICAR`
- servidor de orquestacion: `Servidor A`
- servidor de ejecucion de scripts y subida a Dropbox: `Servidor B`

Comportamiento esperado del proceso en produccion:

1. Lee filas `PENDIENTE` de la hoja `stock`.
2. Inserta los items nuevos en `items_info` y, si hace falta, crea `products_info` y `references_info`.
3. Genera filas nuevas en `PUBLICAR` para preparacion manual de anuncios en Wallapop.
   - si hay muchas filas, las procesa por lotes de `10`
4. Sube imagenes desde S3 a Dropbox en `/BESTCASH/WALLAPOP/<fecha>/<ASIN>/...`.
5. Marca las filas de origen como `PROCESADO` solo si las ramas obligatorias `TPV` y `PUBLICAR` terminan bien.

Observacion operativa:

- tras el cambio a `stock`, se corrigieron dos puntos importantes:
  - `PUBLICAR` solo procesaba el primer lote de `10` filas hasta que se anadio el loopback del batch
  - `PROCESADO` no hacia buen matching hasta mapear `item` por expresion `{{$json["item"]}}`

Punto a vigilar en futuras ejecuciones grandes:

- como `PUBLICAR | Append Rows to Sheet` alimenta a la vez el loopback y el gate `TPV + PUBLICAR`, conviene observar si el marcado `PROCESADO` se produce cuando ya se han completado todos los lotes de `PUBLICAR`
- si en algun momento se detecta que `PROCESADO` se adelanta al ultimo lote publicado, habra que introducir un cierre explicito de fin de batch para la rama `PUBLICAR`

Interpretacion funcional actual de `UD.` y `REF.` en `PUBLICAR`:

- `UD.` representa el numero total actual de unidades detectadas para ese ASIN en `items_info` para `shop_id = 5`
- `REF.` representa el conjunto total actual de codigos detectados para ese ASIN en `items_info` para `shop_id = 5`
- esto es intencional y no se limita al lote nuevo procesado en la ejecucion actual

## Trabajo futuro

### Registro de ASINs ya publicados en Wallapop

Necesidad detectada:

- hoy el workflow puede preparar filas en `PUBLICAR`, pero no distingue de forma fiable entre:
  - ASIN nuevo que requiere crear anuncio
  - ASIN ya publicado en Wallapop al que solo habria que ajustar unidades o revisar stock

Problema actual:

- aunque `UD.` y `REF.` reflejan correctamente el stock total actual, no existe una fuente de verdad interna que indique si ese ASIN ya fue publicado en Wallapop

Mejora propuesta:

- mantener un registro propio de publicaciones Wallapop por ASIN

Opciones de implementacion futura:

- Google Sheet auxiliar, por ejemplo `WALLAPOP_PUBLICADOS`
- tabla en base de datos

Campos minimos sugeridos:

- `asin`
- `wallapop_publicado`
- `wallapop_listing_id` o URL del anuncio
- `fecha_publicacion`
- `ultima_revision`
- `estado`

Uso futuro en el workflow:

- clasificar cada fila generada en `PUBLICAR` como:
  - `NUEVO`
  - `YA_PUBLICADO`
  - `ACTUALIZAR_UNIDADES`

Beneficio esperado:

- evitar republicar manualmente productos ya existentes en Wallapop
- permitir al operador centrarse en actualizar unidades o revisar anuncios ya creados

## Workflow: Amazon Pipeline Auto

### Estado

- Nombre en `n8n`: `Amazon Pipeline Auto`
- Estado funcional: operativo
- Objetivo:
  - lanzar automaticamente el pipeline de Amazon mediante un wrapper remoto

### Sistemas implicados

- `n8n` en Servidor A
- SSH hacia el servidor remoto configurado en el nodo
- script wrapper remoto `run_amazon_pipeline.sh`

### Estructura del workflow

El workflow es muy simple y consta de dos nodos:

1. `Schedule Trigger`
2. `Execute a command`

### Disparo programado

- Nodo: `Schedule Trigger`
- Regla observada en el export:
  - `field = weeks`
  - `triggerAtDay = [2]`
  - `triggerAtHour = 14`

Nota:

- el export indica una programacion semanal, pero para interpretar con exactitud el dia y la hora hay que tener en cuenta la timezone configurada en el workflow o en la instancia `n8n`

### Ejecucion remota

- Nodo: `Execute a command`
- Comando:

```bash
bash /root/run_amazon_pipeline.sh
```

- `cwd`:

```bash
/root
```

### Contexto conocido

- En la auditoria del Servidor A aparece el archivo:
  - `/root/run_amazon_pipeline.sh`
- Contenido confirmado de `/root/run_amazon_pipeline.sh`:

```bash
#!/bin/bash

echo "=== INICIO PIPELINE AMAZON ==="

ssh root@212.227.90.202 "/root/run_import_pipeline.sh"

echo "=== FIN ORQUESTADOR ==="
```

- Interpretacion:
  - el workflow se dispara en el Servidor A
  - el Servidor A actua solo como orquestador
  - el trabajo real se ejecuta en el Servidor B

- Contenido confirmado de `/root/run_import_pipeline.sh` en el Servidor B:

```bash
#!/bin/bash

cd /root/BestCashOps || exit
source venv/bin/activate

FILES=$(ls base/amazon_import/procesar/*.txt 2>/dev/null | wc -l)

if [ "$FILES" -eq 0 ]; then
    echo "No hay manifests. Abortando."
    exit 0
fi

echo "[1] Importando manifests..."
python3 base/amazon_import/import_manifest.py

echo "[2] Lanzando pipeline..."

tmux kill-session -t pipeline_run 2>/dev/null

tmux new -d -s pipeline_run "python3 base/amazon_import/sync_delivery_to_scraped_products.py"

echo "Pipeline lanzado"
tmux ls
```

- Interpretacion funcional:
  1. entra en `/root/BestCashOps`
  2. activa el `venv`
  3. comprueba si existen manifests `.txt` en `base/amazon_import/procesar/`
  4. si no hay manifests, termina sin error
  5. ejecuta `python3 base/amazon_import/import_manifest.py`
  6. mata cualquier sesion previa `tmux` llamada `pipeline_run`
  7. lanza en segundo plano una nueva sesion `tmux` con:

```bash
python3 base/amazon_import/sync_delivery_to_scraped_products.py
```

- Implicacion importante:
  - este workflow no espera a que termine el pipeline completo
  - solo orquesta su lanzamiento en segundo plano mediante `tmux`

### Riesgos y preguntas abiertas

1. Confirmar la timezone efectiva del trigger para interpretar bien el horario semanal.
2. Documentar con mas detalle que hace `import_manifest.py`.
3. Documentar con mas detalle el flujo de `sync_delivery_to_scraped_products.py`.
4. Decidir si el lanzamiento en `tmux` necesita monitorizacion o comprobacion posterior desde `n8n`.

## Workflow: New Products TPV Auto

### Estado

- Nombre previsto en `n8n`: `New Products TPV Auto`
- Estado funcional: implementacion avanzada, aun no listo para produccion
- Objetivo:
  - leer nuevas filas pendientes desde varias hojas de Google Sheets
  - construir un CSV unificado para el TPV
  - copiar ese CSV al Servidor B
  - ejecutar `tpv/insert_new_items_shops.py`
  - marcar las filas procesadas como `PROCESADO`

### Estado actual validado

- `Schedule Trigger` diario a las `04:00`
- Lectura de las tres hojas:
  - `Lanzarote`
  - `LaVina`
  - `Europolis`
- Filtro por `Estado = PENDIENTE` ya configurado en los nodos de Google Sheets
- Transformacion por tienda ya definida:
  - `Lanzarote` -> `tienda = 1`
  - `LaVina` -> `tienda = 2`
  - `Europolis` -> `tienda = 3`
- `ok_online = 0` cuando `Observaciones` contiene `no apto online`
- `Convert to File` ya esta generando el CSV con forma correcta:
  - cabecera `item,asin,precio,tienda,ok_online`
  - filas correctas sin `csv_text` ni comillas rotas
- El fichero se ha comprobado correctamente en:
  - `Servidor A`: `/opt/n8n_files/nuevos_productos.csv`
  - `Servidor B`: `/root/BestCashOps/tpv/nuevos_productos.csv`
- `tpv/insert_new_items_shops.py` se ha endurecido para leer CSV con BOM usando `utf-8-sig`
- La subida estable al `Servidor B` ya no necesita depender del `scp` desde `Servidor A`:
  - `Convert to File` genera el binario CSV
  - un nodo `SSH` con operacion `Upload File` lo deja directamente en `/root/BestCashOps/tpv/nuevos_productos.csv`
- El script de insercion ya se ha validado manualmente en `Servidor B` con salida correcta:
  - conexion a MySQL correcta
  - items existentes insertados en `items_info`

### Riesgo actual importante

- La parte principal que queda por validar de punta a punta es el marcado `PROCESADO` en las tres hojas.
- Ya se detecto un problema previo:
  - `Item` llegaba con salto de linea final en los nodos `appendOrUpdate`
  - eso hacia que Google Sheets anadiera filas nuevas en vez de actualizar las existentes
- Los nodos `PROCESADO | Lanzarote`, `PROCESADO | LaVina` y `PROCESADO | Europolis` deben usar `Item` limpio, sin `\\n`.

### Nodos temporales de comprobacion

- `Code in JavaScript`, `TPV | Build Copy Command` y los nodos de comprobacion asociados quedaron como apoyo durante el diagnostico del movimiento del CSV
- `Execute a command` se esta usando como comprobacion temporal en `Servidor A`
- `Execute a command1` se esta usando como comprobacion temporal en `Servidor B`

### Antes de darlo por cerrado

1. Confirmar que el marcado `PROCESADO` funciona en las tres hojas tras exito real del script.
2. Limpiar o renombrar los nodos temporales de comprobacion cuando dejen de ser necesarios.
3. Decidir si se eliminan ya los nodos auxiliares del camino antiguo de copia local del CSV.

### Sistemas implicados

- `n8n` en Servidor A
- Google Sheets
- SSH entre servidores
- Repo `BestCashOps` en Servidor B
- script `tpv/insert_new_items_shops.py`

### Hoja de origen

- Documento:
  - `https://docs.google.com/spreadsheets/d/19DkYpM-t6izmJL617yZPKb0VAkXdSmvcRTe8uTGFhrU/edit`
- Nombre funcional:
  - `Traspasos Tiendas`
- Pestañas implicadas:
  - `Lanzarote`
  - `LaVina`
  - `Europolis`

### Regla de lectura

De cada pestaña se deben leer las filas donde la columna `F` (`Estado`) tenga el valor:

- `PENDIENTE`

### Campos de origen

De cada fila seleccionada:

- columna `A`: texto
- columna `B`: texto
- columna `C`: numero o texto con simbolo `€`
- columna `D`: texto auxiliar para decidir si el item es apto online
- columna `F`: estado

### Transformacion al CSV

El CSV final debe contener cinco columnas:

1. columna `A` de la hoja
2. columna `B` de la hoja
3. columna `C` normalizada como numero
   - si viene con simbolo `€`, hay que quitarlo
4. identificador de tienda:
   - `1` para `Lanzarote`
   - `2` para `LaVina`
   - `3` para `Europolis`
5. indicador `ok_online`:
   - `1` por defecto
   - `0` si la columna `D` de la hoja contiene el texto `no apto online`

### Salida remota

El CSV debe copiarse al Servidor B en:

- `/root/BestCashOps/tpv/`

Nombre sugerido:

- `/root/BestCashOps/tpv/nuevos_productos.csv`

### Ejecucion remota

Despues de copiar el CSV, el workflow debe ejecutar en el Servidor B:

```bash
sed -i '1s/^\xEF\xBB\xBF//' /root/BestCashOps/tpv/nuevos_productos.csv && cd /root/BestCashOps && source venv/bin/activate && python3 tpv/insert_new_items_shops.py /root/BestCashOps/tpv/nuevos_productos.csv
```

### Marcado final

Una vez terminada correctamente la ejecucion remota:

- todas las filas que estaban en `PENDIENTE` deben pasar a `PROCESADO`

### Programacion deseada

- diario a las `04:00`
- timezone `Europe/Madrid`

### Nota de implementacion

Este workflow es distinto de `Setup Wallapop Publish`.

- `Wallapop` trabaja sobre una hoja de stock y construye datos de publicacion
- `New Products TPV Auto` trabajara sobre `Traspasos Tiendas` y solo insertara productos nuevos en TPV

### Criterio pendiente de cerrar

- si una fila falla en la insercion remota, hay que decidir si:
  - se deja toda la tanda sin marcar `PROCESADO`
  - o se marcan solo las filas realmente insertadas con exito

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

## Propuesta de siguiente version del workflow

### Criterio de exito confirmado

Marcar `procesar_wallapop = PROCESADO` solo si se cumplen estas dos condiciones:

- la rama TPV termina bien
- la rama `PUBLICAR` termina bien

La rama de imagenes `S3 -> Dropbox` queda como auxiliar:

- si termina bien, perfecto
- si falla, debe dejar aviso, pero no bloquear `PROCESADO`

### Cambio de horario

Configuracion recomendada del trigger:

- timezone: `Europe/Madrid`
- hora: `00:00`
- frecuencia: diaria

No conviene mantener `America/New_York`, porque `00:00` alli no corresponde a las `00:00` de Madrid.

### Estructura recomendada

#### Bloque 1: entrada y filtrado

- `Schedule Trigger`
- `Get row(s) in sheet`
- `If procesar_wallapop == PENDIENTE`

#### Bloque 2: rama TPV obligatoria

- `Edit Fields`
- `Convert to File`
- `Code in JavaScript3`
- `Execute a command1`
- `Execute a command2`
- `Execute a command`

Salida esperada:

- CSV copiado al Servidor B
- `tpv/insert_new_items_shops.py` ejecutado con exito

#### Bloque 3: rama PUBLICAR obligatoria

- `Code in JavaScript`
- `Execute a SQL query`
- `Code in JavaScript1`
- `Execute a SQL query1`
- `Merge`
- `Code in JavaScript2`
- `Append row in sheet`

Salida esperada:

- filas anadidas en la hoja `PUBLICAR`

#### Bloque 4: rama imagenes auxiliar

Objetivo:

- subir imagenes desde S3 a Dropbox remoto para los ASIN del lote actual

Implementacion recomendada:

1. Crear un fichero temporal con los ASIN unicos del lote actual.
2. Lanzar por SSH el script:

```bash
AWS_PROFILE=bestcash /root/BestCashOps/tools/maintenance/images_s3_to_dropbox.sh \
  --asins-file /tmp/wallapop_asins.txt \
  --dropbox-base /BESTCASH/WALLAPOP \
  --date-folder 2026-05-01
```

Notas:

- en `n8n`, la fecha no debe ir fija; conviene generarla dinamicamente
- esta rama no deberia usar `--strict` si no queremos bloquear `PROCESADO`

#### Bloque 5: marcado final de PROCESADO

Solo debe ejecutarse despues de que:

- la rama TPV haya terminado OK
- la rama PUBLICAR haya terminado OK

Implementacion recomendada:

- unir la salida de TPV y PUBLICAR con un `Merge` de control
- desde ese `Merge`, disparar `Append or update row in sheet`

La rama de imagenes puede quedar en paralelo desde el mismo filtro inicial o desde la lista de ASINs, pero no debe alimentar ese `Merge` final si no quieres que bloquee el marcado.

### Nodos nuevos recomendados

Para integrar imagenes de forma limpia, probablemente haran falta estos nodos adicionales:

- un `Code` node para sacar ASINs unicos
- un nodo para convertir esa lista en fichero de texto
- uno o dos nodos `SSH` para:
  - copiar el fichero de ASINs a ruta temporal
  - ejecutar `images_s3_to_dropbox.sh`

### Punto delicado

La rama `Append or update row in sheet` no deberia colgar directamente de `Edit Fields` como en el export original, porque asi puede marcar `PROCESADO` sin esperar a que TPV y `PUBLICAR` acaben realmente.
