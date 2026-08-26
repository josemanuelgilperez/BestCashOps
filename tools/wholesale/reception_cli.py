#!/usr/bin/env python3
import argparse
import os
import sys


REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from tools.wholesale.reception_common import (
    ensure_client,
    ensure_reception_units,
    get_or_create_client_pallet,
    read_pallet_codes,
    run_schema,
)


DEFAULT_PALLETS_TXT = os.path.join(REPO_ROOT, "tools", "printing", "pallets.txt")


def parse_args():
    parser = argparse.ArgumentParser(
        description="Da de alta un cliente de recepcion y asigna pallets."
    )
    parser.add_argument("--client", help="Nombre del cliente.")
    parser.add_argument(
        "--pallets-txt",
        default=DEFAULT_PALLETS_TXT,
        help="TXT con codigos de pallet, uno por linea.",
    )
    parser.add_argument(
        "--token",
        help="Token privado opcional. Si no se indica, se genera uno.",
    )
    parser.add_argument(
        "--create-schema",
        action="store_true",
        help="Crea/actualiza las tablas de recepcion antes del alta.",
    )
    parser.add_argument(
        "--schema-only",
        action="store_true",
        help="Solo crea/actualiza las tablas de recepcion y termina.",
    )
    parser.add_argument(
        "--base-url",
        default=os.getenv("RECEPTION_BASE_URL", "http://127.0.0.1:8092"),
        help="Base URL para imprimir el enlace final.",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    if args.create_schema or args.schema_only:
        run_schema()
        print("Tablas de recepcion comprobadas.")
        if args.schema_only:
            return

    if not args.client:
        raise SystemExit("Indica --client, o usa --schema-only para crear solo las tablas.")

    codes = read_pallet_codes(args.pallets_txt)
    if not codes:
        raise SystemExit(f"No hay pallets en {args.pallets_txt}")

    client = ensure_client(args.client, token=args.token)
    total_created_units = 0
    assigned = 0

    for code in codes:
        assignment = get_or_create_client_pallet(client["id"], code)
        created_units = ensure_reception_units(assignment["id"], code)
        total_created_units += created_units
        assigned += 1
        print(f"{code}: asignado | unidades nuevas {created_units}")

    base_url = args.base_url.rstrip("/")
    print("")
    print(f"Cliente: {client['name']}")
    print(f"Token: {client['token']}")
    print(f"Pallets asignados: {assigned}")
    print(f"Unidades nuevas creadas: {total_created_units}")
    print(f"URL cliente: {base_url}/recepcion/{client['token']}/")


if __name__ == "__main__":
    main()
