# Tools

Utilidades operativas de `BestCashOps`.

## Estructura

- `tools/data/`
  - archivos de entrada CSV/TXT usados por utilidades.
- `tools/maintenance/`
  - scripts de mantenimiento de datos/catalogo.
- `tools/media/`
  - utilidades de subida/descarga de imagenes.
- `tools/wholesale/`
  - utilidades puntuales relacionadas con lotes.
- `tools/printing/`
  - generacion de CSV/PDF para impresion de etiquetas.
- `tools/export/`
  - exportaciones puntuales (por ejemplo, XLSX de pallets).

## Nota

- Flujo mayorista oficial: `wholesale/`.
- Flujo retail de importacion/sync: `base/amazon_import/` + `base/bestcash_modules/`.

## Proceso: vendidos Wallapop -> Shopify

Este proceso carga un CSV de vendidos de Wallapop y actualiza stock/estado en Shopify.

1) Subir CSV desde local al VPS:

```bash
scp tools/data/wallapop_input/wallapop_2026_04_14.csv root@212.227.90.202:/root/BestCashOps/tools/data/wallapop_input/
```

2) (Opcional) Verificar en VPS:

```bash
ls -la /root/BestCashOps/tools/data/wallapop_input/
```

3) Ejecutar sync manual (sin esperar al cron):

```bash
bash /root/run_shopify_sync.sh
```

4) Ver resultado en log:

```bash
tail -n 120 /root/BestCashOps/logs/shopify_sync.log
```

5) Confirmar que el CSV se movio a procesados:

```bash
ls -la /root/BestCashOps/tools/data/wallapop_input/processed/
```
