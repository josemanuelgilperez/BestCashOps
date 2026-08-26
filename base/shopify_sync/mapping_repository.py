from dataclasses import dataclass
from typing import Iterable, List

from .shopify_client import ShopifyVariantMapping


@dataclass(frozen=True)
class MappingRefreshResult:
    total_seen: int
    total_written: int
    sample_skus: List[str]


class MappingRepository:
    def __init__(self, connection):
        self.connection = connection

    def upsert_many(self, mappings: Iterable[ShopifyVariantMapping], dry_run: bool = True) -> MappingRefreshResult:
        total_seen = 0
        total_written = 0
        sample_skus = []

        cursor = self.connection.cursor()
        try:
            for mapping in mappings:
                total_seen += 1
                if len(sample_skus) < 10:
                    sample_skus.append(mapping.sku)

                if dry_run:
                    continue

                cursor.execute(
                    """
                    INSERT INTO shopify_mapping
                        (sku, shopify_product_id, shopify_variant_id, inventory_item_id, location_id)
                    VALUES (%s, %s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE
                        shopify_product_id = VALUES(shopify_product_id),
                        shopify_variant_id = VALUES(shopify_variant_id),
                        inventory_item_id = VALUES(inventory_item_id),
                        location_id = VALUES(location_id)
                    """,
                    (
                        mapping.sku,
                        mapping.shopify_product_id,
                        mapping.shopify_variant_id,
                        mapping.inventory_item_id,
                        mapping.location_id,
                    ),
                )
                total_written += 1
        finally:
            cursor.close()

        return MappingRefreshResult(
            total_seen=total_seen,
            total_written=total_written,
            sample_skus=sample_skus,
        )
