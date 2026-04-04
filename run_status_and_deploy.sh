#!/usr/bin/env bash
# Edita wholesale/data/update_status.csv y ejecuta update_status_and_deploy.py
#
# En el VPS suele vivir en $HOME junto a run_boxes_pipeline.sh:
#   cp ~/BestCashOps/run_status_and_deploy.sh ~/
#   chmod +x ~/run_status_and_deploy.sh
#   ~/run_status_and_deploy.sh
#
# O enlace: ln -sf ~/BestCashOps/run_status_and_deploy.sh ~/run_status_and_deploy.sh
#
# Si el repo no está en ~/BestCashOps: export BESTCASHOPS_ROOT=/ruta/al/repo
set -euo pipefail

REPO_ROOT="${BESTCASHOPS_ROOT:-${HOME}/BestCashOps}"
CSV="${REPO_ROOT}/wholesale/data/update_status.csv"
EXAMPLE="${REPO_ROOT}/wholesale/data/update_status.csv.example"

if [[ ! -d "$REPO_ROOT" ]]; then
  echo "No existe el repo en: $REPO_ROOT"
  echo "Define BESTCASHOPS_ROOT o coloca BestCashOps en ${HOME}/BestCashOps"
  exit 1
fi

mkdir -p "$(dirname "$CSV")"
if [[ ! -f "$CSV" ]]; then
  if [[ -f "$EXAMPLE" ]]; then
    cp "$EXAMPLE" "$CSV"
  else
    printf 'code,status\n' > "$CSV"
  fi
fi

nano "$CSV"

cd "$REPO_ROOT"
if [[ -f "${REPO_ROOT}/venv/bin/activate" ]]; then
  # shellcheck source=/dev/null
  source "${REPO_ROOT}/venv/bin/activate"
fi

exec python3 "${REPO_ROOT}/wholesale/scripts/update_status_and_deploy.py"
