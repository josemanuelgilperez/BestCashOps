import time
from dataclasses import dataclass
from datetime import datetime
from typing import Iterable, List, Optional

from .inventory_reader import InventoryReader, StockRecord


@dataclass(frozen=True)
class StockSyncAction:
    sku: str
    stock: int
    previous_stock: Optional[int]
    current_shopify_status: Optional[str]
    target_status: str
    dry_run: bool
    updated: bool
    error: Optional[str] = None


@dataclass(frozen=True)
class StockSyncResult:
    total_candidates: int
    total_updated: int
    total_errors: int
    actions: List[StockSyncAction]


class StockSynchronizer:
    def __init__(self, connection, shopify_client, rate_limit_sleep: float = 0.4):
        self.connection = connection
        self.shopify_client = shopify_client
        self.rate_limit_sleep = rate_limit_sleep

    def sync(self, *, asin: Optional[str] = None, limit: Optional[int] = None, dry_run: bool = True) -> StockSyncResult:
        records = InventoryReader(self.connection).load_stock_records(asin=asin, changed_only=True)
        if limit:
            records = records[:limit]

        actions = []
        for record in records:
            actions.append(self.sync_record(record, dry_run=dry_run))

        return StockSyncResult(
            total_candidates=len(records),
            total_updated=sum(1 for action in actions if action.updated),
            total_errors=sum(1 for action in actions if action.error),
            actions=actions,
        )

    def sync_record(self, record: StockRecord, *, dry_run: bool = True) -> StockSyncAction:
        if dry_run:
            return StockSyncAction(
                sku=record.sku,
                stock=record.stock,
                previous_stock=record.previous_stock,
                current_shopify_status=record.current_shopify_status,
                target_status=record.target_status,
                dry_run=True,
                updated=False,
            )

        if self.shopify_client is None:
            raise ValueError("shopify_client is required when dry_run is false")

        try:
            self.shopify_client.set_inventory_level(
                inventory_item_id=record.inventory_item_id,
                location_id=record.location_id,
                available=record.stock,
            )
            self.shopify_client.update_product_status(
                product_id=record.shopify_product_id,
                status=record.target_status,
            )
            self.record_stock_sync(record)
            time.sleep(self.rate_limit_sleep)
            return StockSyncAction(
                sku=record.sku,
                stock=record.stock,
                previous_stock=record.previous_stock,
                current_shopify_status=record.current_shopify_status,
                target_status=record.target_status,
                dry_run=False,
                updated=True,
            )
        except Exception as exc:
            return StockSyncAction(
                sku=record.sku,
                stock=record.stock,
                previous_stock=record.previous_stock,
                current_shopify_status=record.current_shopify_status,
                target_status=record.target_status,
                dry_run=False,
                updated=False,
                error=str(exc),
            )

    def record_stock_sync(self, record: StockRecord):
        cursor = self.connection.cursor()
        try:
            cursor.execute(
                """
                INSERT INTO stock_sync_log (asin, last_stock, last_synced_at)
                VALUES (%s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    last_stock = VALUES(last_stock),
                    last_synced_at = VALUES(last_synced_at)
                """,
                (record.sku, record.stock, datetime.now()),
            )
            if self.table_exists("shopify_catalog_snapshot"):
                cursor.execute(
                    """
                    UPDATE shopify_catalog_snapshot
                    SET status = %s,
                        shopify_synced_at = %s
                    WHERE sku = %s
                    """,
                    (record.target_status, datetime.now(), record.sku),
                )
        finally:
            cursor.close()

    def table_exists(self, table_name: str) -> bool:
        cursor = self.connection.cursor()
        try:
            cursor.execute("SHOW TABLES LIKE %s", (table_name,))
            return cursor.fetchone() is not None
        finally:
            cursor.close()
