#!/usr/bin/env python3
"""Build aggregated data for Amazon delivery manifest reports."""

from __future__ import annotations

import argparse
import csv
import json
import os
import re
from collections import defaultdict
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from pathlib import Path


MANIFEST_RE = re.compile(
    r"^Liq_FBA_WeeklyManifest_V3_(DE|ES|FR|IT)_(\d{8})_ND72B\.txt(?:_\d+)?$"
)

GL_TRANSLATIONS = {
    "gl_apparel": "Ropa",
    "gl_automotive": "Automoción",
    "gl_baby_product": "Bebé",
    "gl_beauty": "Belleza",
    "gl_biss": "Industrial y empresa",
    "gl_book": "Libros",
    "gl_camera": "Cámaras y fotografía",
    "gl_digital_accessories": "Accesorios digitales",
    "gl_drugstore": "Droguería y salud",
    "gl_dvd": "DVD",
    "gl_electronics": "Electrónica",
    "gl_furniture": "Muebles",
    "gl_grocery": "Alimentación",
    "gl_home": "Hogar",
    "gl_home_entertainment": "Entretenimiento hogar",
    "gl_home_improvement": "Bricolaje",
    "gl_jewelry": "Joyería",
    "gl_kitchen": "Cocina",
    "gl_lawn_and_garden": "Jardín",
    "gl_luggage": "Equipaje",
    "gl_major_appliances": "Grandes electrodomésticos",
    "gl_musical_instruments": "Instrumentos musicales",
    "gl_office_product": "Oficina",
    "gl_outdoors": "Aire libre",
    "gl_pc": "Informática",
    "gl_personal_care_appliances": "Cuidado personal",
    "gl_pet_products": "Mascotas",
    "gl_sdp_misc": "Miscelánea",
    "gl_shoes": "Calzado",
    "gl_softlines_private_label": "Marca privada textil",
    "gl_software": "Software",
    "gl_sports": "Deporte",
    "gl_tires": "Neumáticos",
    "gl_tools": "Herramientas",
    "gl_toy": "Juguetes",
    "gl_video_games": "Videojuegos",
    "gl_watch": "Relojes",
    "gl_wine": "Vino",
    "gl_wireless": "Telefonía y accesorios",
    "(Sin GLDesc)": "Sin GL",
}

COST_BUCKETS = [
    (0, "0 - 1 EUR", Decimal("0"), Decimal("1")),
    (1, "1 - 2 EUR", Decimal("1"), Decimal("2")),
    (2, "2 - 5 EUR", Decimal("2"), Decimal("5")),
    (3, "5 - 10 EUR", Decimal("5"), Decimal("10")),
    (4, "10 - 20 EUR", Decimal("10"), Decimal("20")),
    (5, "20 - 50 EUR", Decimal("20"), Decimal("50")),
    (6, "> 50 EUR", Decimal("50"), None),
]


def translate_gl(gl_desc: str) -> str:
    if gl_desc in GL_TRANSLATIONS:
        return GL_TRANSLATIONS[gl_desc]
    return gl_desc.removeprefix("gl_").replace("_", " ").title()


def cost_bucket(unitcost: Decimal) -> tuple[int, str]:
    for order, label, lower, upper in COST_BUCKETS:
        if upper is None and unitcost > lower:
            return order, label
        if unitcost >= lower and unitcost <= upper:
            return order, label
    return COST_BUCKETS[0][0], COST_BUCKETS[0][1]


def parse_decimal(value: str | None) -> Decimal:
    if value is None or value == "":
        return Decimal("0")
    try:
        return Decimal(str(value).replace(",", "."))
    except InvalidOperation:
        return Decimal("0")


def parse_int(value: str | None) -> int:
    if value is None or value == "":
        return 0
    return int(Decimal(str(value).replace(",", ".")))


def money(value: Decimal) -> float:
    return float(value.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP))


def ratio(value: Decimal) -> float:
    return float(value.quantize(Decimal("0.0001"), rounding=ROUND_HALF_UP))


def make_bucket() -> dict:
    return {
        "rows": 0,
        "units": 0,
        "asins": set(),
        "unitcost_sum": Decimal("0"),
        "unitrecovery_sum": Decimal("0"),
        "amazonprice_sum": Decimal("0"),
        "unitcost_weighted": Decimal("0"),
        "unitrecovery_weighted": Decimal("0"),
        "amazonprice_weighted": Decimal("0"),
        "total_cost": Decimal("0"),
        "total_recovery": Decimal("0"),
        "total_amazonprice": Decimal("0"),
        "categories": set(),
        "source_files": set(),
        "shipment_min": None,
        "shipment_max": None,
        "missing_category_rows": 0,
        "missing_category_units": 0,
        "item_desc": "",
    }


def add_to_bucket(bucket: dict, row: dict, units: int, unitcost: Decimal, unitrecovery: Decimal, amazonprice: Decimal) -> None:
    bucket["rows"] += 1
    bucket["units"] += units
    if row["Asin"]:
        bucket["asins"].add(row["Asin"])
    if not bucket["item_desc"] and row.get("ItemDesc"):
        bucket["item_desc"] = row["ItemDesc"]
    if row["CategoryDesc"]:
        bucket["categories"].add(row["CategoryDesc"])
    if row.get("category_missing"):
        bucket["missing_category_rows"] += 1
        bucket["missing_category_units"] += units
    bucket["source_files"].add(row["source_file"])
    bucket["unitcost_sum"] += unitcost
    bucket["unitrecovery_sum"] += unitrecovery
    bucket["amazonprice_sum"] += amazonprice
    bucket["unitcost_weighted"] += unitcost * units
    bucket["unitrecovery_weighted"] += unitrecovery * units
    bucket["amazonprice_weighted"] += amazonprice * units
    bucket["total_cost"] += unitcost * units
    bucket["total_recovery"] += unitrecovery * units
    bucket["total_amazonprice"] += amazonprice * units

    shipment_closed = row["ShipmentClosed"]
    if shipment_closed:
        if bucket["shipment_min"] is None or shipment_closed < bucket["shipment_min"]:
            bucket["shipment_min"] = shipment_closed
        if bucket["shipment_max"] is None or shipment_closed > bucket["shipment_max"]:
            bucket["shipment_max"] = shipment_closed


def finalize_bucket(key, bucket: dict, extra: dict | None = None) -> dict:
    units = bucket["units"]
    rows = bucket["rows"]
    total_recovery = bucket["total_recovery"]
    total_cost = bucket["total_cost"]
    total_amazonprice = bucket["total_amazonprice"]

    out = {
        "key": key,
        "rows": rows,
        "units": units,
        "distinct_asins": len(bucket["asins"]),
        "avg_unitcost": money(bucket["unitcost_sum"] / rows) if rows else 0,
        "avg_unitcost_weighted": money(bucket["unitcost_weighted"] / units) if units else 0,
        "avg_unitrecovery": money(bucket["unitrecovery_sum"] / rows) if rows else 0,
        "avg_unitrecovery_weighted": money(bucket["unitrecovery_weighted"] / units) if units else 0,
        "avg_amazonprice": money(bucket["amazonprice_sum"] / rows) if rows else 0,
        "avg_amazonprice_weighted": money(bucket["amazonprice_weighted"] / units) if units else 0,
        "total_unitcost_value": money(total_cost),
        "total_recovery_value": money(total_recovery),
        "total_amazonprice_value": money(total_amazonprice),
        "recovery_vs_unitcost_pct": ratio(total_recovery / total_cost) if total_cost else 0,
        "recovery_vs_amazonprice_pct": ratio(total_recovery / total_amazonprice) if total_amazonprice else 0,
        "source_files": len(bucket["source_files"]),
        "shipment_min": bucket["shipment_min"],
        "shipment_max": bucket["shipment_max"],
        "missing_category_rows": bucket["missing_category_rows"],
        "missing_category_units": bucket["missing_category_units"],
        "missing_category_unit_share": ratio(Decimal(bucket["missing_category_units"]) / Decimal(units))
        if units
        else 0,
    }
    if extra:
        out.update(extra)
    return out


def manifest_info(path: Path) -> tuple[str, str]:
    match = MANIFEST_RE.match(path.name)
    if not match:
        raise ValueError(f"Archivo no reconocido: {path.name}")
    country, raw_date = match.groups()
    return country, f"{raw_date[:4]}-{raw_date[4:6]}-{raw_date[6:]}"


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source-dir", required=True)
    parser.add_argument("--output-json", required=True)
    args = parser.parse_args()

    source_dir = Path(args.source_dir)
    files = sorted(source_dir.glob("Liq_FBA_WeeklyManifest_V3_*_ND72B.txt*"))
    if not files:
        raise SystemExit(f"No manifest files found in {source_dir}")

    by_category = defaultdict(make_bucket)
    by_report_category = defaultdict(make_bucket)
    by_report_category_week = defaultdict(make_bucket)
    by_report_category_week_country = defaultdict(make_bucket)
    by_week = defaultdict(make_bucket)
    by_category_date_country = defaultdict(make_bucket)
    by_file = defaultdict(make_bucket)
    by_gl = defaultdict(make_bucket)
    by_gl_week = defaultdict(make_bucket)
    by_cost_bucket = defaultdict(make_bucket)
    by_cost_bucket_week = defaultdict(make_bucket)
    by_subcategory = defaultdict(make_bucket)
    by_asin = defaultdict(make_bucket)
    base_rows = []
    all_bucket = make_bucket()

    for path in files:
        manifest_country, manifest_date = manifest_info(path)
        with path.open("r", encoding="utf-8", errors="ignore", newline="") as handle:
            reader = csv.DictReader(handle, delimiter="\t", quotechar='"', escapechar="\\")
            for raw in reader:
                raw_category = raw.get("CategoryDesc") or ""
                category_missing = not raw_category
                category = raw_category or "(Sin categoria)"
                subcategory = raw.get("SubcatDesc") or "(Sin subcategoria)"
                gl_desc = raw.get("GLDesc") or "(Sin GLDesc)"
                gl_es = translate_gl(gl_desc)
                report_category = raw_category or f"GL: {gl_desc}"
                asin = raw.get("Asin") or ""
                units = parse_int(raw.get("Units"))
                unitcost = parse_decimal(raw.get("UnitCost"))
                unitrecovery = parse_decimal(raw.get("UnitRecovery"))
                amazonprice = parse_decimal(raw.get("AmazonPrice"))
                bucket_order, bucket_label = cost_bucket(unitcost)

                row = {
                    **raw,
                    "CategoryDesc": category,
                    "category_raw": raw_category,
                    "category_missing": category_missing,
                    "report_category": report_category,
                    "SubcatDesc": subcategory,
                    "GLDesc": gl_desc,
                    "gl_es": gl_es,
                    "Asin": asin,
                    "manifest_country": manifest_country,
                    "manifest_date": manifest_date,
                    "source_file": path.name,
                }

                for bucket in (
                    all_bucket,
                    by_category[category],
                    by_report_category[report_category],
                    by_report_category_week[(manifest_date, report_category)],
                    by_report_category_week_country[(manifest_date, manifest_country, report_category)],
                    by_week[manifest_date],
                    by_category_date_country[(manifest_date, manifest_country, category)],
                    by_file[(manifest_date, manifest_country, path.name)],
                    by_gl[gl_desc],
                    by_gl_week[(manifest_date, gl_desc, gl_es)],
                    by_cost_bucket[(bucket_order, bucket_label)],
                    by_cost_bucket_week[(manifest_date, bucket_order, bucket_label)],
                    by_subcategory[(category, subcategory)],
                    by_asin[(asin, gl_desc, gl_es)],
                ):
                    add_to_bucket(bucket, row, units, unitcost, unitrecovery, amazonprice)

                base_rows.append(
                    {
                        "manifest_date": manifest_date,
                        "manifest_country": manifest_country,
                        "source_file": path.name,
                        "ShipmentClosed": raw.get("ShipmentClosed") or "",
                        "Categoria_informe": report_category,
                        "CategoryDesc": category,
                        "CategoryDesc_original_vacio": category_missing,
                        "SubcatDesc": subcategory,
                        "GLDesc": gl_desc,
                        "GL_ES": gl_es,
                        "Asin": asin,
                        "ItemDesc": raw.get("ItemDesc") or "",
                        "Units": units,
                        "UnitCost": money(unitcost),
                        "CostBucket": bucket_label,
                        "AmazonPrice": money(amazonprice),
                        "UnitRecovery": money(unitrecovery),
                        "UnitCostValue": money(unitcost * units),
                        "AmazonPriceValue": money(amazonprice * units),
                        "RecoveryValue": money(unitrecovery * units),
                    }
                )

    categories = [
        finalize_bucket(
            category,
            bucket,
            {
                "category": category,
                "unit_share": ratio(Decimal(bucket["units"]) / Decimal(all_bucket["units"]))
                if all_bucket["units"]
                else 0,
                "unitcost_value_share": ratio(bucket["total_cost"] / all_bucket["total_cost"])
                if all_bucket["total_cost"]
                else 0,
                "recovery_value_share": ratio(bucket["total_recovery"] / all_bucket["total_recovery"])
                if all_bucket["total_recovery"]
                else 0,
            },
        )
        for category, bucket in by_category.items()
    ]
    categories.sort(key=lambda row: (-row["units"], -row["total_unitcost_value"], row["category"]))

    weekly_summary = [
        finalize_bucket(
            manifest_date,
            bucket,
            {
                "manifest_date": manifest_date,
                "files": len(bucket["source_files"]),
                "gl_categories": len(
                    {
                        gl_desc
                        for (week, gl_desc, _gl_es), gl_bucket in by_gl_week.items()
                        if week == manifest_date and gl_bucket["units"] > 0
                    }
                ),
                "report_categories": len(
                    {
                        report_category
                        for (week, report_category), report_bucket in by_report_category_week.items()
                        if week == manifest_date and report_bucket["units"] > 0
                    }
                ),
            },
        )
        for manifest_date, bucket in by_week.items()
    ]
    weekly_summary.sort(key=lambda row: row["manifest_date"])

    report_categories = [
        finalize_bucket(
            report_category,
            bucket,
            {
                "report_category": report_category,
                "unit_share": ratio(Decimal(bucket["units"]) / Decimal(all_bucket["units"]))
                if all_bucket["units"]
                else 0,
                "unitcost_value_share": ratio(bucket["total_cost"] / all_bucket["total_cost"])
                if all_bucket["total_cost"]
                else 0,
                "recovery_value_share": ratio(bucket["total_recovery"] / all_bucket["total_recovery"])
                if all_bucket["total_recovery"]
                else 0,
            },
        )
        for report_category, bucket in by_report_category.items()
    ]
    report_categories.sort(
        key=lambda row: (-row["units"], -row["total_unitcost_value"], row["report_category"])
    )

    report_category_week = [
        finalize_bucket(
            "|".join(key),
            bucket,
            {
                "manifest_date": key[0],
                "report_category": key[1],
                "unit_share": ratio(
                    Decimal(bucket["units"])
                    / Decimal(sum(b["units"] for k, b in by_report_category_week.items() if k[0] == key[0]))
                )
                if bucket["units"]
                else 0,
            },
        )
        for key, bucket in by_report_category_week.items()
    ]
    report_category_week.sort(
        key=lambda row: (row["manifest_date"], -row["units"], -row["total_unitcost_value"], row["report_category"])
    )

    report_category_week_country = [
        finalize_bucket(
            "|".join(key),
            bucket,
            {
                "manifest_date": key[0],
                "manifest_country": key[1],
                "report_category": key[2],
            },
        )
        for key, bucket in by_report_category_week_country.items()
    ]
    report_category_week_country.sort(
        key=lambda row: (
            row["manifest_date"],
            row["manifest_country"],
            -row["units"],
            -row["total_unitcost_value"],
            row["report_category"],
        )
    )

    category_date_country = [
        finalize_bucket(
            "|".join(key),
            bucket,
            {
                "manifest_date": key[0],
                "manifest_country": key[1],
                "category": key[2],
            },
        )
        for key, bucket in by_category_date_country.items()
    ]
    category_date_country.sort(
        key=lambda row: (row["manifest_date"], row["manifest_country"], -row["units"], row["category"])
    )

    files_report = [
        finalize_bucket(
            "|".join(key),
            bucket,
            {
                "manifest_date": key[0],
                "manifest_country": key[1],
                "source_file": key[2],
            },
        )
        for key, bucket in by_file.items()
    ]
    files_report.sort(key=lambda row: (row["manifest_date"], row["manifest_country"], row["source_file"]))

    gl_report = [
        finalize_bucket(
            gl_desc,
            bucket,
            {
                "gl_desc": gl_desc,
                "gl_es": translate_gl(gl_desc),
                "unit_share": ratio(Decimal(bucket["units"]) / Decimal(all_bucket["units"]))
                if all_bucket["units"]
                else 0,
            },
        )
        for gl_desc, bucket in by_gl.items()
    ]
    gl_report.sort(key=lambda row: (row["gl_es"], row["gl_desc"]))

    gl_week_report = [
        finalize_bucket(
            "|".join(key),
            bucket,
            {
                "manifest_date": key[0],
                "gl_desc": key[1],
                "gl_es": key[2],
                "unit_share": ratio(
                    Decimal(bucket["units"])
                    / Decimal(sum(b["units"] for k, b in by_gl_week.items() if k[0] == key[0]))
                )
                if bucket["units"]
                else 0,
            },
        )
        for key, bucket in by_gl_week.items()
    ]
    gl_week_report.sort(key=lambda row: (row["manifest_date"], row["gl_es"], row["gl_desc"]))

    cost_bucket_report = [
        finalize_bucket(
            bucket_label,
            bucket,
            {
                "bucket_order": bucket_order,
                "cost_bucket": bucket_label,
                "unit_share": ratio(Decimal(bucket["units"]) / Decimal(all_bucket["units"]))
                if all_bucket["units"]
                else 0,
                "unitcost_value_share": ratio(bucket["total_cost"] / all_bucket["total_cost"])
                if all_bucket["total_cost"]
                else 0,
            },
        )
        for (bucket_order, bucket_label), bucket in by_cost_bucket.items()
    ]
    cost_bucket_report.sort(key=lambda row: row["bucket_order"])

    cost_bucket_week_report = [
        finalize_bucket(
            "|".join(map(str, key)),
            bucket,
            {
                "manifest_date": key[0],
                "bucket_order": key[1],
                "cost_bucket": key[2],
                "unit_share": ratio(
                    Decimal(bucket["units"])
                    / Decimal(sum(b["units"] for k, b in by_cost_bucket_week.items() if k[0] == key[0]))
                )
                if bucket["units"]
                else 0,
                "unitcost_value_share": ratio(
                    bucket["total_cost"]
                    / sum(
                        (b["total_cost"] for k, b in by_cost_bucket_week.items() if k[0] == key[0]),
                        Decimal("0"),
                    )
                )
                if bucket["total_cost"]
                else 0,
            },
        )
        for key, bucket in by_cost_bucket_week.items()
    ]
    cost_bucket_week_report.sort(key=lambda row: (row["manifest_date"], row["bucket_order"]))

    subcategory_report = [
        finalize_bucket(
            "|".join(key),
            bucket,
            {"category": key[0], "subcategory": key[1]},
        )
        for key, bucket in by_subcategory.items()
    ]
    subcategory_report.sort(key=lambda row: (-row["units"], row["category"], row["subcategory"]))

    asin_report = [
        finalize_bucket(
            "|".join(key),
            bucket,
            {
                "asin": key[0],
                "gl_desc": key[1],
                "gl_es": key[2],
                "item_desc": bucket["item_desc"],
            },
        )
        for key, bucket in by_asin.items()
        if key[0]
    ]
    asin_report.sort(key=lambda row: (-row["units"], -row["total_unitcost_value"], row["asin"]))

    summary = finalize_bucket("TOTAL", all_bucket)
    summary.update(
        {
            "files": len(files),
            "categories": len(categories),
            "report_categories": len(report_categories),
            "source_dir": str(source_dir),
        }
    )

    out = {
        "summary": summary,
        "weekly_summary": weekly_summary,
        "files": files_report,
        "categories": categories,
        "report_categories": report_categories,
        "report_category_week": report_category_week,
        "report_category_week_country": report_category_week_country,
        "category_date_country": category_date_country,
        "gl": gl_report,
        "gl_week": gl_week_report,
        "cost_buckets": cost_bucket_report,
        "cost_buckets_week": cost_bucket_week_report,
        "subcategories": subcategory_report,
        "top_asins": asin_report[:500],
        "base_rows": base_rows,
    }

    output_path = Path(args.output_json)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")

    print(f"Files: {summary['files']}")
    print(f"Rows: {summary['rows']}")
    print(f"Units: {summary['units']}")
    print(f"Distinct ASINs: {summary['distinct_asins']}")
    print(f"Categories: {summary['categories']}")
    print(f"Output: {output_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
