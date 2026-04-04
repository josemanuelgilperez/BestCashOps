#!/usr/bin/env bash
# Edita wholesale/data/update_status.csv y ejecuta el deploy incremental.
# Uso: ./edit_status_and_deploy.sh   (desde la raíz del repo)
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CSV="${REPO_ROOT}/wholesale/data/update_status.csv"
EXAMPLE="${REPO_ROOT}/wholesale/data/update_status.csv.example"

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
