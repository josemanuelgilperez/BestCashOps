from dataclasses import dataclass
from datetime import datetime
from typing import Iterable, List

from .shopify_client import ShopifyCatalogSnapshot


@dataclass(frozen=True)
class CatalogRefreshResult:
    total_seen: int
    total_written: int
    sample_skus: List[str]


class CatalogSnapshotRepository:
    def __init__(self, connection):
        self.connection = connection

    def ensure_table(self):
        cursor = self.connection.cursor()
        try:
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS shopify_catalog_snapshot (
                    sku varchar(50) NOT NULL PRIMARY KEY,
                    shopify_product_id bigint NOT NULL,
                    shopify_variant_id bigint NOT NULL,
                    inventory_item_id bigint NOT NULL,
                    handle varchar(255),
                    title varchar(255),
                    status varchar(50),
                    vendor varchar(255),
                    product_type varchar(255),
                    tags text,
                    price decimal(10,2),
                    compare_at_price decimal(10,2),
                    image_count int NOT NULL DEFAULT 0,
                    shopify_synced_at datetime NOT NULL
                )
                """
            )
        finally:
            cursor.close()

    def upsert_many(
        self,
        snapshots: Iterable[ShopifyCatalogSnapshot],
        dry_run: bool = True,
    ) -> CatalogRefreshResult:
        total_seen = 0
        total_written = 0
        sample_skus = []

        if not dry_run:
            self.ensure_table()

        cursor = self.connection.cursor()
        try:
            for snapshot in snapshots:
                total_seen += 1
                if len(sample_skus) < 10:
                    sample_skus.append(snapshot.sku)

                if dry_run:
                    continue

                cursor.execute(
                    """
                    INSERT INTO shopify_catalog_snapshot (
                        sku,
                        shopify_product_id,
                        shopify_variant_id,
                        inventory_item_id,
                        handle,
                        title,
                        status,
                        vendor,
                        product_type,
                        tags,
                        price,
                        compare_at_price,
                        image_count,
                        shopify_synced_at
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE
                        shopify_product_id = VALUES(shopify_product_id),
                        shopify_variant_id = VALUES(shopify_variant_id),
                        inventory_item_id = VALUES(inventory_item_id),
                        handle = VALUES(handle),
                        title = VALUES(title),
                        status = VALUES(status),
                        vendor = VALUES(vendor),
                        product_type = VALUES(product_type),
                        tags = VALUES(tags),
                        price = VALUES(price),
                        compare_at_price = VALUES(compare_at_price),
                        image_count = VALUES(image_count),
                        shopify_synced_at = VALUES(shopify_synced_at)
                    """,
                    (
                        snapshot.sku,
                        snapshot.shopify_product_id,
                        snapshot.shopify_variant_id,
                        snapshot.inventory_item_id,
                        snapshot.handle,
                        snapshot.title,
                        snapshot.status,
                        snapshot.vendor,
                        snapshot.product_type,
                        snapshot.tags,
                        snapshot.price,
                        snapshot.compare_at_price,
                        snapshot.image_count,
                        datetime.now(),
                    ),
                )
                total_written += 1
        finally:
            cursor.close()

        return CatalogRefreshResult(
            total_seen=total_seen,
            total_written=total_written,
            sample_skus=sample_skus,
        )
