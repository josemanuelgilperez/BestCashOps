# BestCashOps

Repositorio unificado de operaciones BestCash: flujo retail, flujo mayorista y utilidades.

## Estructura actual

```text
BestCashOps/
├── base/
│   ├── amazon_import/         # importacion y sync retail desde Amazon
│   └── bestcash_modules/      # modulos compartidos para retail
├── wholesale/
│   ├── data/                  # update_status.csv (deploy incremental); opcional si no usas tools/data
│   ├── pipeline/              # ingest, enrich, finance (lotes)
│   ├── scripts/               # wrappers de ejecucion/deploy mayorista
│   └── web/                   # build html, categories, templates, assets
├── tools/                     # utilidades puntuales (mantenimiento, media, export, printing)
├── tools/data/                # Excel lotes + names.csv (ingest los usa por defecto)
├── config/
├── logs/
├── db.py
└── requirements.txt
```

## Flujos principales

- **Retail (Amazon -> catalogo):**
  - `base/amazon_import/import_manifest.py`
  - `base/amazon_import/sync_delivery_to_scraped_products.py`
  - `base/amazon_import/force_rescrape_asins.py`

- **Mayorista (lotes):**
  - `wholesale/scripts/run_pipeline.py`
  - `wholesale/scripts/update_status_and_deploy.py`

## Comandos de referencia

### Pipeline mayorista completo

```bash
python3 wholesale/scripts/run_pipeline.py
```

### Actualizar estados y publicar

```bash
python3 wholesale/scripts/update_status_and_deploy.py
```

### Utilidades

Ver `tools/README.md`.

## Variables de entorno

Definir en `.env` al menos:

- `DB_USER`
- `DB_PASSWORD`
- `DB_HOST`
- `DB_NAME`
- `FTP_HOST`
- `FTP_USER`
- `FTP_PASS`

Adicionales segun scripts:

- `OPENAI_API_KEY`
- `CRAWLBASE_TOKEN`

## Nota operativa

- El frontend web de tienda (`bestcash` Next.js) se mantiene en repo separado.
- Este repo esta orientado a automatizaciones Python de datos/stock/publicacion.
