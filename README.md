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

## Entorno Python

Crear o activar el **virtualenv** del repo e instalar dependencias **con el pip de ese entorno** (no uses el `pip` global del sistema salvo que sepas lo que haces).

```bash
cd /ruta/a/BestCashOps
python3 -m venv venv          # solo la primera vez
source venv/bin/activate       # macOS / Linux
pip install -r requirements.txt
```

Sin activar, equivalente:

```bash
./venv/bin/pip install -r requirements.txt
```

Los scripts del proyecto deben ejecutarse con el mismo entorno: `python3 ...` tras `source venv/bin/activate`, o `./venv/bin/python3 ...`.

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
