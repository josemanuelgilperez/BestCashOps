#!/usr/bin/env python3
import argparse
import os
import subprocess
import sys


REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))


def run(cmd, skip=False):
    if skip:
        print(f"⏭️  Saltando: {' '.join(cmd)}", flush=True)
        return
    print(f"\n▶ {' '.join(cmd)}", flush=True)
    subprocess.run(cmd, cwd=REPO_ROOT, check=True)


def parse_args():
    parser = argparse.ArgumentParser(description="Publicación mayorista: finanzas, HTML, categorías y FTP.")
    parser.add_argument("--boxes", action="append", help="Códigos MP separados por coma. Puede repetirse.")
    parser.add_argument("--from-asins", help="TXT de ASINs para resolver pallets afectados.")
    parser.add_argument("--new-pallets", action="store_true", help="Usa wholesale/data/processed/*.xlsx.")
    parser.add_argument("--full-finance", action="store_true", help="Recalcula todos los pallets Disponible/Reservado.")
    parser.add_argument("--skip-quality", action="store_true")
    parser.add_argument("--skip-finance", action="store_true")
    parser.add_argument("--skip-build", action="store_true")
    parser.add_argument("--no-ftp", action="store_true")
    return parser.parse_args()


def scope_args(args):
    out = []
    for box_arg in args.boxes or []:
        out.extend(["--boxes", box_arg])
    if args.from_asins:
        out.extend(["--from-asins", args.from_asins])
    if args.new_pallets:
        out.append("--new-pallets")
    return out


def main():
    args = parse_args()
    py = sys.executable
    scoped = scope_args(args)

    quality_scope = scoped or ["--all-available"]
    finance_scope = ["--full"] if args.full_finance else scoped

    run(
        [
            py,
            "tools/wholesale/quality_report.py",
            *quality_scope,
            "--output-prefix",
            "tools/data/quality_pre_publish",
        ],
        skip=args.skip_quality,
    )
    run(
        [py, "wholesale/pipeline/finance.py", *finance_scope],
        skip=args.skip_finance,
    )
    run([py, "wholesale/web/build_html.py"], skip=args.skip_build)
    run([py, "wholesale/web/categories.py"], skip=args.skip_build)
    run(
        [
            "node",
            "wholesale/scripts/ventadelotes_add_new_filters.js",
            "--site",
            "wholesale/web/output",
        ],
        skip=args.skip_build,
    )
    run(
        [
            py,
            "tools/wholesale/quality_report.py",
            *quality_scope,
            "--output-prefix",
            "tools/data/quality_post_publish",
        ],
        skip=args.skip_quality,
    )
    run([py, "wholesale/scripts/upload_ftp.py"], skip=args.no_ftp)

    print("\n🎉 Publicación mayorista completada", flush=True)


if __name__ == "__main__":
    main()
