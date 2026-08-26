#!/usr/bin/env python3
"""Upload Amazon Vendor weekly manifests to the VPS import inbox."""

from __future__ import annotations

import argparse
import os
import re
import subprocess
import sys
from collections import defaultdict
from pathlib import Path


MANIFEST_RE = re.compile(
    r"^Liq_FBA_WeeklyManifest_V3_(DE|ES|FR|IT)_(\d{8})_ND72B\.txt$"
)
DEFAULT_REMOTE = "root@212.227.90.202"
DEFAULT_REMOTE_DIR = "/root/BestCashOps/base/amazon_import/procesar/"
EXPECTED_COUNTRIES = {"DE", "ES", "FR", "IT"}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Upload Amazon Vendor manifests from a local folder to the BestCashOps VPS."
    )
    parser.add_argument(
        "--source-dir",
        default=str(Path.home() / "Downloads"),
        help="Local folder containing downloaded manifest TXT files. Default: ~/Downloads",
    )
    parser.add_argument(
        "--manifest-date",
        help="Manifest date to upload, e.g. 20260809. Defaults to the latest complete date.",
    )
    parser.add_argument(
        "--remote",
        default=DEFAULT_REMOTE,
        help=f"SSH remote. Default: {DEFAULT_REMOTE}",
    )
    parser.add_argument(
        "--remote-dir",
        default=DEFAULT_REMOTE_DIR,
        help=f"Remote inbox. Default: {DEFAULT_REMOTE_DIR}",
    )
    parser.add_argument(
        "--allow-partial",
        action="store_true",
        help="Allow uploading a date that does not have all DE/ES/FR/IT manifests.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print what would be uploaded without running scp.",
    )
    return parser.parse_args()


def discover_manifests(source_dir: Path) -> dict[str, dict[str, Path]]:
    manifests: dict[str, dict[str, Path]] = defaultdict(dict)
    for path in sorted(source_dir.iterdir()):
        if not path.is_file():
            continue
        match = MANIFEST_RE.match(path.name)
        if not match:
            continue
        country, manifest_date = match.groups()
        manifests[manifest_date][country] = path
    return manifests


def choose_date(
    manifests: dict[str, dict[str, Path]], requested_date: str | None, allow_partial: bool
) -> str:
    if requested_date:
        if requested_date not in manifests:
            raise SystemExit(f"No manifest files found for date {requested_date}.")
        return requested_date

    dates = sorted(manifests)
    if not dates:
        raise SystemExit("No Amazon Vendor manifest TXT files found.")

    if allow_partial:
        return dates[-1]

    complete_dates = [
        date for date in dates if EXPECTED_COUNTRIES.issubset(set(manifests[date]))
    ]
    if not complete_dates:
        available = ", ".join(
            f"{date}({','.join(sorted(countries))})"
            for date, countries in manifests.items()
        )
        raise SystemExit(f"No complete DE/ES/FR/IT date found. Available: {available}")
    return complete_dates[-1]


def validate_selection(files_by_country: dict[str, Path], allow_partial: bool) -> list[Path]:
    missing = EXPECTED_COUNTRIES - set(files_by_country)
    if missing and not allow_partial:
        raise SystemExit(
            "Missing manifests for countries: "
            + ", ".join(sorted(missing))
            + ". Use --allow-partial to upload an incomplete set."
        )
    return [files_by_country[country] for country in sorted(files_by_country)]


def main() -> int:
    args = parse_args()
    source_dir = Path(os.path.expanduser(args.source_dir)).resolve()
    if not source_dir.exists():
        raise SystemExit(f"Source folder does not exist: {source_dir}")

    manifests = discover_manifests(source_dir)
    manifest_date = choose_date(manifests, args.manifest_date, args.allow_partial)
    files = validate_selection(manifests[manifest_date], args.allow_partial)

    print(f"Manifest date: {manifest_date}")
    print(f"Source folder: {source_dir}")
    print(f"Remote inbox: {args.remote}:{args.remote_dir}")
    print("Files:")
    for path in files:
        print(f"  - {path.name}")

    if args.dry_run:
        print("Dry run: no upload performed.")
        return 0

    for path in files:
        destination = f"{args.remote}:{args.remote_dir}"
        print(f"Uploading {path.name} ...", flush=True)
        subprocess.run(["scp", str(path), destination], check=True)

    print(f"Uploaded {len(files)} files.")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except subprocess.CalledProcessError as exc:
        raise SystemExit(exc.returncode)
