import logging
import time
from dataclasses import dataclass
from typing import List, Optional

from .catalog_repository import CatalogSnapshotRepository
from .mapping_repository import MappingRepository
from .wallapop_products import ShopifyProductPayloadBuilder, WallapopProductCandidate, WallapopProductReader


@dataclass(frozen=True)
class PublishAction:
    asin: str
    stock: int
    status: str
    dry_run: bool
    created: bool
    shopify_product_id: Optional[int] = None
    shopify_variant_id: Optional[int] = None
    inventory_item_id: Optional[int] = None
    error: Optional[str] = None


@dataclass(frozen=True)
class PublishResult:
    total_candidates: int
    total_created: int
    total_errors: int
    actions: List[PublishAction]


class WallapopPublisher:
    def __init__(self, connection, shopify_client, *, location_id: int, rate_limit_sleep: float = 0.4):
        self.connection = connection
        self.shopify_client = shopify_client
        self.location_id = location_id
        self.rate_limit_sleep = rate_limit_sleep

    def publish_ready(self, *, limit: Optional[int] = None, dry_run: bool = True, status: str = "active") -> PublishResult:
        candidates = WallapopProductReader(self.connection).load_candidates(
            missing_only=True,
            ready_only=True,
            limit=limit,
        )

        actions = []
        for candidate in candidates:
            action = self.publish_candidate(candidate, dry_run=dry_run, status=status)
            actions.append(action)
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

        return PublishResult(
            total_candidates=len(candidates),
            total_created=sum(1 for action in actions if action.created),
            total_errors=sum(1 for action in actions if action.error),
            actions=actions,
        )

    def publish_candidate(
        self,
        candidate: WallapopProductCandidate,
        *,
        dry_run: bool = True,
        status: str = "active",
    ) -> PublishAction:
        try:
            payload = ShopifyProductPayloadBuilder().build_product_payload(candidate, status=status)
            if dry_run:
                return PublishAction(
                    asin=candidate.asin,
                    stock=candidate.stock,
                    status=status,
                    dry_run=True,
                    created=False,
                )

            product = self.shopify_client.create_product(payload)
            mapping = self.shopify_client.mapping_from_product(
                product,
                sku=candidate.asin,
                location_id=self.location_id,
            )
            snapshot = self.shopify_client.snapshot_from_product(product, sku=candidate.asin)

            self.shopify_client.set_inventory_level(
                inventory_item_id=mapping.inventory_item_id,
                location_id=mapping.location_id,
                available=candidate.stock,
            )

            MappingRepository(self.connection).upsert_many([mapping], dry_run=False)
            CatalogSnapshotRepository(self.connection).upsert_many([snapshot], dry_run=False)
            time.sleep(self.rate_limit_sleep)
            return PublishAction(
                asin=candidate.asin,
                stock=candidate.stock,
                status=snapshot.status,
                dry_run=False,
                created=True,
                shopify_product_id=mapping.shopify_product_id,
                shopify_variant_id=mapping.shopify_variant_id,
                inventory_item_id=mapping.inventory_item_id,
            )
        except Exception as exc:
            return PublishAction(
                asin=candidate.asin,
                stock=candidate.stock,
                status=status,
                dry_run=dry_run,
                created=False,
                error=str(exc),
            )
