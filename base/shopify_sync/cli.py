import argparse
import json
import logging

from .catalog_repository import CatalogSnapshotRepository
from .config import ShopifySyncConfig
from .db import get_connection
from .mapping_repository import MappingRepository
from .sheet_price_importer import WallapopSheetPriceImporter, load_sheet_price_rows
from .shopify_client import ShopifyClient
from .stock_sync import StockSynchronizer
from .wallapop_products import ShopifyProductPayloadBuilder, WallapopProductReader
from .wallapop_publisher import WallapopPublisher


def build_parser():
    parser = argparse.ArgumentParser(description="BestCash Shopify synchronization tools.")
    subparsers = parser.add_subparsers(dest="command", required=True)

    refresh_mapping = subparsers.add_parser(
        "refresh-mapping",
        help="Refresh local SKU to Shopify product/variant/inventory mapping.",
    )
    refresh_mapping.add_argument(
        "--write",
        action="store_true",
        help="Write changes to shopify_mapping. Without this flag the command is dry-run.",
    )
    refresh_mapping.add_argument(
        "--location-name",
        help="Shopify location name. Defaults to SHOPIFY_LOCATION_NAME or Lanzarote.",
    )
    refresh_mapping.add_argument(
        "--limit",
        type=int,
        help="Stop after this number of variants. Useful for first verification runs.",
    )

    sync_stock = subparsers.add_parser(
        "sync-stock",
        help="Synchronize real BestCash stock to Shopify inventory and status.",
    )
    sync_stock.add_argument(
        "--write",
        action="store_true",
        help="Write changes to Shopify and stock_sync_log. Without this flag the command is dry-run.",
    )
    sync_stock.add_argument("--asin", help="Synchronize only one ASIN/SKU.")
    sync_stock.add_argument(
        "--limit",
        type=int,
        help="Stop after this number of changed SKUs. Useful for first verification runs.",
    )

    refresh_catalog = subparsers.add_parser(
        "refresh-catalog",
        help="Refresh a local snapshot of Shopify products by SKU.",
    )
    refresh_catalog.add_argument(
        "--write",
        action="store_true",
        help="Write changes to shopify_catalog_snapshot. Without this flag the command is dry-run.",
    )
    refresh_catalog.add_argument(
        "--status",
        choices=("active", "draft", "archived", "any"),
        default="active",
        help="Shopify product status to fetch. Defaults to active, i.e. currently published products.",
    )
    refresh_catalog.add_argument(
        "--limit",
        type=int,
        help="Stop after this number of variants. Useful for first verification runs.",
    )

    wallapop_candidates = subparsers.add_parser(
        "wallapop-candidates",
        help="List Wallapop products from items_info.shop_id=5 that are missing or pending for Shopify.",
    )
    wallapop_candidates.add_argument("--asin", help="Inspect only one ASIN.")
    wallapop_candidates.add_argument(
        "--all",
        action="store_true",
        help="Include ASINs already present in shopify_mapping. Defaults to missing-only.",
    )
    wallapop_candidates.add_argument(
        "--ready-only",
        action="store_true",
        help="Show only candidates ready to publish under the current rules.",
    )
    wallapop_candidates.add_argument(
        "--limit",
        type=int,
        help="Stop after this number of ASIN candidates.",
    )

    wallapop_payload = subparsers.add_parser(
        "wallapop-payload",
        help="Build a Shopify product payload for one Wallapop ASIN without publishing it.",
    )
    wallapop_payload.add_argument("--asin", help="ASIN to build. If omitted, uses the first ready missing ASIN.")
    wallapop_payload.add_argument(
        "--status",
        choices=("draft", "active"),
        default="draft",
        help="Product status in the generated payload. Defaults to draft.",
    )

    wallapop_create = subparsers.add_parser(
        "wallapop-create",
        help="Create one missing Wallapop product in Shopify and store mapping. Dry-run unless --write is used.",
    )
    wallapop_create.add_argument("--asin", required=True, help="ASIN to create in Shopify.")
    wallapop_create.add_argument(
        "--write",
        action="store_true",
        help="Create the product in Shopify and write shopify_mapping/shopify_catalog_snapshot.",
    )
    wallapop_create.add_argument(
        "--status",
        choices=("draft", "active"),
        default="active",
        help="Product status to create. Defaults to active.",
    )
    wallapop_create.add_argument(
        "--location-name",
        help="Shopify location name. Defaults to SHOPIFY_LOCATION_NAME or Lanzarote.",
    )

    wallapop_publish_ready = subparsers.add_parser(
        "wallapop-publish-ready",
        help="Publish all ready Wallapop products from items_info.shop_id=5. Dry-run unless --write is used.",
    )
    wallapop_publish_ready.add_argument(
        "--write",
        action="store_true",
        help="Create products in Shopify and write shopify_mapping/shopify_catalog_snapshot.",
    )
    wallapop_publish_ready.add_argument(
        "--limit",
        type=int,
        help="Stop after this number of ready products.",
    )
    wallapop_publish_ready.add_argument(
        "--status",
        choices=("active", "draft"),
        default="active",
        help="Product status to create. Defaults to active.",
    )
    wallapop_publish_ready.add_argument(
        "--location-name",
        help="Shopify location name. Defaults to SHOPIFY_LOCATION_NAME or Lanzarote.",
    )

    wallapop_apply_sheet_prices = subparsers.add_parser(
        "wallapop-apply-sheet-prices",
        help="Apply accepted Google Sheet prices to Wallapop items_info.shop_id=5. Dry-run unless --write is used.",
    )
    wallapop_apply_sheet_prices.add_argument("--csv", required=True, help="CSV with asin and sheet_price columns.")
    wallapop_apply_sheet_prices.add_argument(
        "--write",
        action="store_true",
        help="Update items_info.bestcash_price for shop_id=5 rows.",
    )
    return parser


def main(argv=None):
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    args = build_parser().parse_args(argv)
    config = ShopifySyncConfig.from_env()

    if args.command == "refresh-mapping":
        refresh_mapping(config, args)
        return 0
    if args.command == "sync-stock":
        sync_stock(config, args)
        return 0
    if args.command == "refresh-catalog":
        refresh_catalog(config, args)
        return 0
    if args.command == "wallapop-candidates":
        wallapop_candidates(config, args)
        return 0
    if args.command == "wallapop-payload":
        wallapop_payload(config, args)
        return 0
    if args.command == "wallapop-create":
        wallapop_create(config, args)
        return 0
    if args.command == "wallapop-publish-ready":
        wallapop_publish_ready(config, args)
        return 0
    if args.command == "wallapop-apply-sheet-prices":
        wallapop_apply_sheet_prices(config, args)
        return 0

    raise SystemExit(f"Unsupported command: {args.command}")


def refresh_mapping(config: ShopifySyncConfig, args):
    dry_run = not args.write
    if dry_run:
        logging.info("Running in dry-run mode. Use --write to update shopify_mapping.")

    client = ShopifyClient(config)
    location_id = client.get_location_id(args.location_name)
    mappings = client.iter_variant_mappings(location_id=location_id)
    if args.limit:
        mappings = _limited(mappings, args.limit)

    conn = get_connection(config)
    try:
        result = MappingRepository(conn).upsert_many(mappings, dry_run=dry_run)
    finally:
        conn.close()

    logging.info("Variants seen: %s", result.total_seen)
    logging.info("Rows written: %s", result.total_written)
    if result.sample_skus:
        logging.info("Sample SKUs: %s", ", ".join(result.sample_skus))


def sync_stock(config: ShopifySyncConfig, args):
    dry_run = not args.write
    if dry_run:
        logging.info("Running in dry-run mode. Use --write to update Shopify and stock_sync_log.")

    client = ShopifyClient(config) if not dry_run else None
    conn = get_connection(config)
    try:
        result = StockSynchronizer(
            conn,
            client,
            rate_limit_sleep=config.rate_limit_sleep,
        ).sync(asin=args.asin, limit=args.limit, dry_run=dry_run)
    finally:
        conn.close()

    logging.info("Changed SKUs evaluated: %s", result.total_candidates)
    logging.info("Rows updated: %s", result.total_updated)
    logging.info("Errors: %s", result.total_errors)
    for action in result.actions[:50]:
        previous = "NULL" if action.previous_stock is None else action.previous_stock
        current_status = "NULL" if action.current_shopify_status is None else action.current_shopify_status
        status = "ERROR" if action.error else ("DRY-RUN" if action.dry_run else "UPDATED")
        logging.info(
            "%s sku=%s previous=%s stock=%s current_status=%s target_status=%s%s",
            status,
            action.sku,
            previous,
            action.stock,
            current_status,
            action.target_status,
            f" error={action.error}" if action.error else "",
        )
    if len(result.actions) > 50:
        logging.info("Output truncated to 50 actions out of %s.", len(result.actions))


def refresh_catalog(config: ShopifySyncConfig, args):
    dry_run = not args.write
    if dry_run:
        logging.info("Running in dry-run mode. Use --write to update shopify_catalog_snapshot.")

    client = ShopifyClient(config)
    snapshots = client.iter_catalog_snapshots(status=args.status)
    if args.limit:
        snapshots = _limited(snapshots, args.limit)

    if dry_run:
        total_seen = 0
        sample_skus = []
        for snapshot in snapshots:
            total_seen += 1
            if len(sample_skus) < 10:
                sample_skus.append(snapshot.sku)
        logging.info("Variants seen: %s", total_seen)
        logging.info("Rows written: 0")
        if sample_skus:
            logging.info("Sample SKUs: %s", ", ".join(sample_skus))
        return

    conn = get_connection(config)
    try:
        result = CatalogSnapshotRepository(conn).upsert_many(snapshots, dry_run=False)
    finally:
        conn.close()

    logging.info("Variants seen: %s", result.total_seen)
    logging.info("Rows written: %s", result.total_written)
    if result.sample_skus:
        logging.info("Sample SKUs: %s", ", ".join(result.sample_skus))


def wallapop_candidates(config: ShopifySyncConfig, args):
    conn = get_connection(config)
    try:
        candidates = WallapopProductReader(conn).load_candidates(
            asin=args.asin,
            missing_only=not args.all,
            ready_only=args.ready_only,
            limit=args.limit,
        )
    finally:
        conn.close()

    issue_counts = {}
    for candidate in candidates:
        for issue in candidate.issues:
            issue_counts[issue] = issue_counts.get(issue, 0) + 1

    logging.info("Wallapop ASIN candidates evaluated: %s", len(candidates))
    logging.info("Ready to publish: %s", sum(1 for candidate in candidates if candidate.is_ready_to_publish))
    for issue, count in sorted(issue_counts.items()):
        logging.info("Issue %s: %s", issue, count)

    for candidate in candidates[:50]:
        issues = ",".join(candidate.issues) or "ready"
        logging.info(
            "asin=%s stock=%s price=%s images=%s status=%s issues=%s title=%s",
            candidate.asin,
            candidate.stock,
            candidate.price or "",
            len(candidate.images),
            candidate.shopify_status or "",
            issues,
            candidate.title[:80],
        )
    if len(candidates) > 50:
        logging.info("Output truncated to 50 candidates out of %s.", len(candidates))


def wallapop_payload(config: ShopifySyncConfig, args):
    conn = get_connection(config)
    try:
        if args.asin:
            candidates = WallapopProductReader(conn).load_candidates(
                asin=args.asin,
                missing_only=True,
                ready_only=False,
            )
        else:
            candidates = WallapopProductReader(conn).load_candidates(
                missing_only=True,
                ready_only=True,
                limit=1,
            )
    finally:
        conn.close()

    if not candidates:
        raise SystemExit("No Wallapop candidate found for the requested criteria.")

    candidate = candidates[0]
    payload = ShopifyProductPayloadBuilder().build_product_payload(candidate, status=args.status)
    print(json.dumps(payload, indent=2, ensure_ascii=False, default=str))


def wallapop_create(config: ShopifySyncConfig, args):
    dry_run = not args.write
    conn = get_connection(config)
    try:
        candidates = WallapopProductReader(conn).load_candidates(
            asin=args.asin,
            missing_only=True,
            ready_only=False,
        )
        if not candidates:
            raise SystemExit(f"No missing Wallapop candidate found for ASIN {args.asin}.")

        candidate = candidates[0]
        payload = ShopifyProductPayloadBuilder().build_product_payload(candidate, status=args.status)

        if dry_run:
            logging.info("Running in dry-run mode. Use --write to create this Shopify product.")
            print(json.dumps(payload, indent=2, ensure_ascii=False, default=str))
            return

        candidates = WallapopProductReader(conn).load_candidates(
            asin=args.asin,
            missing_only=True,
            ready_only=False,
        )
        if not candidates:
            raise SystemExit(f"ASIN {args.asin} is no longer missing from shopify_mapping.")
        candidate = candidates[0]
        payload = ShopifyProductPayloadBuilder().build_product_payload(candidate, status=args.status)

        client = ShopifyClient(config)
        location_id = client.get_location_id(args.location_name)
        product = client.create_product(payload)
        mapping = client.mapping_from_product(product, sku=candidate.asin, location_id=location_id)
        snapshot = client.snapshot_from_product(product, sku=candidate.asin)

        client.set_inventory_level(
            inventory_item_id=mapping.inventory_item_id,
            location_id=mapping.location_id,
            available=candidate.stock,
        )

        MappingRepository(conn).upsert_many([mapping], dry_run=False)
        CatalogSnapshotRepository(conn).upsert_many([snapshot], dry_run=False)

        logging.info(
            "CREATED asin=%s product_id=%s variant_id=%s inventory_item_id=%s stock=%s status=%s",
            candidate.asin,
            mapping.shopify_product_id,
            mapping.shopify_variant_id,
            mapping.inventory_item_id,
            candidate.stock,
            snapshot.status,
        )
    finally:
        conn.close()


def wallapop_publish_ready(config: ShopifySyncConfig, args):
    dry_run = not args.write
    if dry_run:
        logging.info("Running in dry-run mode. Use --write to publish ready Wallapop products.")

    client = ShopifyClient(config) if not dry_run else None
    location_id = client.get_location_id(args.location_name) if client else _location_id_from_mapping(config)

    conn = get_connection(config)
    try:
        result = WallapopPublisher(
            conn,
            client,
            location_id=location_id,
            rate_limit_sleep=config.rate_limit_sleep,
        ).publish_ready(limit=args.limit, dry_run=dry_run, status=args.status)
    finally:
        conn.close()

    logging.info("Ready Wallapop candidates evaluated: %s", result.total_candidates)
    logging.info("Products created: %s", result.total_created)
    logging.info("Errors: %s", result.total_errors)
    for action in result.actions[:100]:
        state = "ERROR" if action.error else ("DRY-RUN" if action.dry_run else "CREATED")
        logging.info(
            "%s asin=%s stock=%s status=%s product_id=%s%s",
            state,
            action.asin,
            action.stock,
            action.status,
            action.shopify_product_id or "",
            f" error={action.error}" if action.error else "",
        )
    if len(result.actions) > 100:
        logging.info("Output truncated to 100 actions out of %s.", len(result.actions))


def wallapop_apply_sheet_prices(config: ShopifySyncConfig, args):
    dry_run = not args.write
    if dry_run:
        logging.info("Running in dry-run mode. Use --write to update Wallapop item prices.")

    rows = load_sheet_price_rows(args.csv)
    conn = get_connection(config)
    try:
        result = WallapopSheetPriceImporter(conn).apply_prices(rows, dry_run=dry_run)
    finally:
        conn.close()

    logging.info("Sheet price rows seen: %s", result.total_seen)
    logging.info("Rows updated: %s", result.total_updated)
    logging.info("Errors: %s", result.total_errors)
    for action in result.actions[:100]:
        state = "ERROR" if action.error else ("DRY-RUN" if action.dry_run else "UPDATED")
        logging.info(
            "%s asin=%s price=%s matched_items=%s%s",
            state,
            action.asin,
            action.price,
            action.matched_items,
            f" error={action.error}" if action.error else "",
        )
    if len(result.actions) > 100:
        logging.info("Output truncated to 100 actions out of %s.", len(result.actions))


def _limited(items, limit):
    for idx, item in enumerate(items):
        if idx >= limit:
            return
        yield item


def _location_id_from_mapping(config: ShopifySyncConfig) -> int:
    conn = get_connection(config)
    cursor = conn.cursor(dictionary=True)
    try:
        cursor.execute("SELECT DISTINCT location_id FROM shopify_mapping WHERE location_id IS NOT NULL LIMIT 1")
        row = cursor.fetchone()
        if not row:
            raise SystemExit("No location_id found in shopify_mapping.")
        return int(row["location_id"])
    finally:
        cursor.close()
        conn.close()


if __name__ == "__main__":
    raise SystemExit(main())
