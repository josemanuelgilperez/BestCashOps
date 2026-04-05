#!/usr/bin/env bash
# Edita wholesale/data/update_status.csv y ejecuta update_status_and_deploy.py
#
# La raíz del repo se deduce de ESTE fichero (resolviendo symlinks), así el mismo
# script vale en Mac (p. ej. ~/Documents/GitHub/BestCashOps) y en VPS (~/BestCashOps).
#
# Recomendado: enlace desde $HOME al script dentro del repo:
#   ln -sf /ruta/completa/a/BestCashOps/run_status_and_deploy.sh ~/run_status_and_deploy.sh
#   chmod +x ~/run_status_and_deploy.sh
#   ~/run_status_and_deploy.sh
#   Desde $HOME también: ./run_status_and_deploy.sh (equivalente si el .sh está ahí)
#
# También puedes ejecutarlo por ruta explícita:
#   bash ~/Documents/GitHub/BestCashOps/run_status_and_deploy.sh
#
# Override opcional: export BESTCASHOPS_ROOT=/otra/ruta/al/repo
set -euo pipefail

# Raíz del repo = directorio donde está este .sh (tras seguir enlaces simbólicos).
_resolve_repo_root() {
  local src="${BASH_SOURCE[0]}"
  while [[ -L "$src" ]]; do
    local dir
    dir="$(cd -P "$(dirname "$src")" && pwd)"
    local target
    target="$(readlink "$src")"
    [[ "$target" != /* ]] && target="${dir}/${target}"
    src="$target"
  done
  cd -P "$(dirname "$src")" && pwd
}

if [[ -n "${BESTCASHOPS_ROOT:-}" ]]; then
  REPO_ROOT="$(cd "$BESTCASHOPS_ROOT" && pwd)"
else
  REPO_ROOT="$(_resolve_repo_root)"
fi

PY="${REPO_ROOT}/wholesale/scripts/update_status_and_deploy.py"
if [[ ! -f "$PY" ]]; then
  echo "No encuentro el repo BestCashOps en: $REPO_ROOT"
  echo "  (falta $PY)"
  echo "Ejecuta este script desde la raíz del repo, o enlázalo desde ahí, o define BESTCASHOPS_ROOT."
  exit 1
fi

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

exec python3 "$PY"
