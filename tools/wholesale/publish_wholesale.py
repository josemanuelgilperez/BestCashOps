#!/usr/bin/env python3
import argparse
import os
from pathlib import Path
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


def write_new_codes_file(args):
    if not (args.new_pallets or args.boxes or args.from_asins):
        return
    sys.path.insert(0, REPO_ROOT)
    from wholesale.pipeline.finance import resolve_scope_codes

    codes = resolve_scope_codes(
        boxes=args.boxes,
        from_asins=args.from_asins,
        new_pallets=args.new_pallets,
    )
    if not codes:
        return
    path = Path(REPO_ROOT) / "wholesale" / "data" / "new_published_pallets.txt"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(codes) + "\n", encoding="utf-8")
    print(f"\n▶ nuevos publicados: {len(codes)} codigos en {path.relative_to(REPO_ROOT)}", flush=True)


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
    if not args.skip_build:
        write_new_codes_file(args)
    run(
        [
            py,
            "tools/wholesale/mark_new_lots.py",
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
