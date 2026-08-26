# Shopify Sync

Sistema incremental para sincronizar catalogo BestCash con Shopify.

## Fase 1

Esta fase creo la base modular sin publicar productos nuevos todavia.

Componentes creados:

- `base/shopify_sync/config.py`: configuracion por variables de entorno.
- `base/shopify_sync/shopify_client.py`: cliente Shopify REST reutilizable.
- `base/shopify_sync/mapping_repository.py`: escritura idempotente en `shopify_mapping`.
- `tools/maintenance/shopify_sync.py`: CLI operativo.

Codigo reutilizado conceptualmente:

- `PyTools/ShopifyBestCash/shopify_mapping.py`: paginacion de productos Shopify y campos de mapping.
- `PyTools/ShopifyBestCash/sync_shopify_stock.py`: uso de `inventory_item_id` y `location_id`.
- `tools/maintenance/daily_shopify_sync.py`: convencion actual de `SHOPIFY_TOKEN`, `shopify_mapping` y stock sync.

## Fase 2

Esta fase porta la sincronizacion de stock al paquete modular. Sigue sin crear productos nuevos.

Componentes creados:

- `base/shopify_sync/inventory_reader.py`: calcula stock real desde `shopify_mapping`, `products_info`, `references_info` e `items_info`.
- `base/shopify_sync/stock_sync.py`: decide acciones de stock/status y ejecuta Shopify solo con `--write`.
- `base/shopify_sync/catalog_repository.py`: guarda una foto local del catalogo actual de Shopify en `shopify_catalog_snapshot`.
- `tools/maintenance/shopify_sync.py sync-stock`: comando operativo con `dry-run` por defecto.

Codigo reutilizado conceptualmente:

- `tools/maintenance/daily_shopify_sync.py`: SQL de stock real, comparacion con `stock_sync_log` y actualizacion `active`/`draft`.
- `PyTools/ShopifyBestCash/sync_shopify_stock.py`: uso de `inventory_levels/set.json` y status por `product_id`.

Decision actualizada:

- si `stock > 0`, el producto objetivo en Shopify es `active`;
- si `stock = 0`, el producto objetivo en Shopify es `archived`;
- `sync-stock` puede usar `shopify_catalog_snapshot.status` para archivar aunque el stock no haya cambiado desde la ultima sincronizacion.

## Fase 3

Esta fase prepara la publicacion de Wallapop sin crear productos todavia.

Fuente canonica para Wallapop:

- `items_info.shop_id = 5`;
- `items_info.ok_online = 1`;
- ASIN desde `references_info -> products_info`;
- ficha final desde `amazon_scraped_products`;
- duplicados y productos existentes controlados con `shopify_mapping`;
- estado actual auxiliar desde `shopify_catalog_snapshot`.

Componentes creados:

- `base/shopify_sync/wallapop_products.py`: lector de candidatos Wallapop y constructor de payload Shopify.
- `tools/maintenance/shopify_sync.py wallapop-candidates`: reporte dry-run de candidatos faltantes/listos.
- `tools/maintenance/shopify_sync.py wallapop-payload`: genera el JSON Shopify para un ASIN sin publicarlo.

Reglas actuales:

- no publicar sin ASIN;
- no publicar ASIN duplicado en `products_info`;
- no publicar si ya existe en `shopify_mapping`;
- no publicar sin stock;
- no publicar sin titulo, precio, peso o imagen;
- no publicar si hay varios precios para el mismo ASIN hasta definir politica;
- exigir imagen oficial S3 de `bestcashproductimages`, admitiendo `image_not_found.jpg` si aparece como imagen.

## Variables De Entorno

No guardar secretos en el repositorio. Configurar estos valores en `.env` local o en el servidor:

```bash
DB_HOST=
DB_PORT=3306
DB_USER=
DB_PASSWORD=
DB_NAME=

SHOPIFY_SHOP_DOMAIN=bestcash-outlet.myshopify.com
SHOPIFY_API_VERSION=2024-10
SHOPIFY_TOKEN=
SHOPIFY_LOCATION_NAME=Lanzarote
SHOPIFY_REQUEST_TIMEOUT=30
SHOPIFY_RATE_LIMIT_SLEEP=0.4
```

## Comandos

Dry-run de mapping, limitado a 10 variantes:

```bash
python3 tools/maintenance/shopify_sync.py refresh-mapping --limit 10
```

Escritura real en `shopify_mapping`:

```bash
python3 tools/maintenance/shopify_sync.py refresh-mapping --write
```

Dry-run de catalogo Shopify publicado actualmente:

```bash
python3 tools/maintenance/shopify_sync.py refresh-catalog --status active --limit 10
```

Escritura real de la foto local del catalogo publicado:

```bash
python3 tools/maintenance/shopify_sync.py refresh-catalog --status active --write
```

Dry-run de sincronizacion de stock, limitado a 10 SKUs con cambios:

```bash
python3 tools/maintenance/shopify_sync.py sync-stock --limit 10
```

Dry-run de sincronizacion de stock para un ASIN:

```bash
python3 tools/maintenance/shopify_sync.py sync-stock --asin B012345678
```

Escritura real de stock y status en Shopify:

```bash
python3 tools/maintenance/shopify_sync.py sync-stock --write
```

Reporte de candidatos Wallapop faltantes en Shopify:

```bash
python3 tools/maintenance/shopify_sync.py wallapop-candidates --limit 20
```

Primeros candidatos Wallapop listos para publicar:

```bash
python3 tools/maintenance/shopify_sync.py wallapop-candidates --ready-only --limit 20
```

Payload Shopify para el primer ASIN Wallapop listo, sin publicar:

```bash
python3 tools/maintenance/shopify_sync.py wallapop-payload
```

Payload Shopify para un ASIN concreto, sin publicar:

```bash
python3 tools/maintenance/shopify_sync.py wallapop-payload --asin B012345678
```

Dry-run de creacion de un producto Wallapop concreto:

```bash
python3 tools/maintenance/shopify_sync.py wallapop-create --asin B012345678
```

Crear un producto Wallapop real en Shopify como `active`, guardar mapping y snapshot:

```bash
python3 tools/maintenance/shopify_sync.py wallapop-create --asin B012345678 --write
```

Dry-run de publicacion batch de todos los Wallapop listos:

```bash
python3 tools/maintenance/shopify_sync.py wallapop-publish-ready
```

Publicar todos los Wallapop listos como `active`:

```bash
python3 tools/maintenance/shopify_sync.py wallapop-publish-ready --write
```

Aplicar precios aceptados desde un CSV con columnas `asin` y `sheet_price` a `items_info.bestcash_price`
para Wallapop (`shop_id = 5`):

```bash
python3 tools/maintenance/shopify_sync.py wallapop-apply-sheet-prices --csv tools/data/shopify_wallapop_reports/wallapop_sheet_price_publishable_32.csv --write
```

Regla operativa: cualquier producto mapeado con stock mayor que 0 debe estar `active`.
Si el stock llega a 0, `sync-stock --write` debe pasarlo a `archived`.

## Despliegue Previsto

Servidor de aplicaciones previsto: `root@212.227.90.202`.

La base de datos vive fuera del servidor de aplicaciones. El despliegue debe consistir en copiar el repo, configurar `.env` en el servidor y ejecutar primero comandos con limite bajo antes de cualquier sincronizacion completa.

## Siguiente Fase

Crear la primera publicacion real controlada de productos nuevos:

- crear productos Wallapop en `active` cuando tengan stock;
- guardar mapping resultante;
- refrescar `shopify_mapping` y `shopify_catalog_snapshot`;
- probar primero con `--asin` y limite bajo.
