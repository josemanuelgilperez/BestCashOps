# Amazon Vendor import

Run this process when new `Liq_FBA_WeeklyManifest_V3_*_ND72B.txt` files are downloaded from Amazon Vendor.

## Local upload

Download the manifests into `~/Downloads`. Then upload the latest complete DE/ES/FR/IT set:

```bash
python3 tools/amazon_import/upload_vendor_manifests.py
```

To upload a specific manifest date:

```bash
python3 tools/amazon_import/upload_vendor_manifests.py --manifest-date 20260809
```

The script uploads to:

```text
root@212.227.90.202:/root/BestCashOps/base/amazon_import/procesar/
```

Use `--dry-run` to preview the selected files before upload.

## Server import

On the VPS:

```bash
cd /root/BestCashOps
source venv/bin/activate
python3 base/amazon_import/migrate_manifest_metadata.py
python3 base/amazon_import/import_manifest.py
```

The migration is idempotent and ensures `amazon_delivery` can store:

- `manifest_country`
- `manifest_date`
- `source_file`
- `imported_at`

## Required validation

The final report must include all of these fields:

- Manifest files processed.
- Manifest date, country, and source file.
- Rows inserted into `amazon_delivery`.
- Duplicate rows.
- Distinct ASIN count.
- Total units/items: `SUM(CAST(Units AS UNSIGNED))` over the exact imported `ItemId` set.
- `amazon_delivery` row count before and after import.
- `ItemId` validation count, e.g. `20002 / 20002`.
- Confirmation that `base/amazon_import/procesar/` is empty.
- Confirmation that the files moved to `base/amazon_import/procesados/`.

Important: "rows" are manifest lines. "items" or "units" are the sum of the `Units` column in `amazon_delivery`.

## Sync tmux

After a successful import, check whether a usable sync is already running:

```bash
tmux ls 2>/dev/null || true
ps -eo pid,ppid,stat,etime,cmd | grep '[s]ync_delivery_to_scraped_products.py'
```

If no live `sync_delivery_to_scraped_products.py` process exists, create a dated session:

```bash
cd /root/BestCashOps
tmux new -d -s sync_delivery_YYYYMMDD 'cd /root/BestCashOps && source venv/bin/activate && python3 base/amazon_import/sync_delivery_to_scraped_products.py; exec bash'
```

Attach command to give the user:

```bash
ssh root@212.227.90.202
tmux attach -t sync_delivery_YYYYMMDD
```
