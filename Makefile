.PHONY: deploy pipeline html ftp help

PYTHON := python3

help:
	@echo "Comandos disponibles:"
	@echo "  make deploy   - Actualizar estados desde CSV y publicar (incremental)"
	@echo "  make pipeline - Pipeline completo (ingest → enrich → finance → HTML → FTP)"
	@echo "  make html     - Generar HTML (build + categorías)"
	@echo "  make ftp      - Subir web/output por FTP"

deploy:
	$(PYTHON) scripts/update_status_and_deploy.py

pipeline:
	$(PYTHON) scripts/run_pipeline.py

html:
	$(PYTHON) web/build_html.py
	$(PYTHON) web/categories.py

ftp:
	$(PYTHON) scripts/upload_ftp.py
