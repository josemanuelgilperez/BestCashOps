import time
from dataclasses import dataclass
from typing import Dict, Iterable, Optional

import requests

from .config import ShopifySyncConfig


class ShopifyApiError(RuntimeError):
    pass


@dataclass(frozen=True)
class ShopifyVariantMapping:
    sku: str
    shopify_product_id: int
    shopify_variant_id: int
    inventory_item_id: int
    location_id: int


@dataclass(frozen=True)
class ShopifyCatalogSnapshot:
    sku: str
    shopify_product_id: int
    shopify_variant_id: int
    inventory_item_id: int
    handle: str
    title: str
    status: str
    vendor: str
    product_type: str
    tags: str
    price: Optional[str]
    compare_at_price: Optional[str]
    image_count: int


class ShopifyClient:
    def __init__(self, config: ShopifySyncConfig):
        config.validate_for_shopify()
        self.config = config
        self.session = requests.Session()
        self.session.headers.update(
            {
                "X-Shopify-Access-Token": config.access_token,
                "Content-Type": "application/json",
            }
        )

    def request(self, method, url, **kwargs):
        timeout = kwargs.pop("timeout", self.config.request_timeout)
        for attempt in range(3):
            response = self.session.request(method, url, timeout=timeout, **kwargs)
            if response.status_code != 429:
                return response
            retry_after = _parse_retry_after(response.headers.get("Retry-After"))
            time.sleep(retry_after or 5)
        return response

    def get_shop(self) -> Dict:
        response = self.request("GET", f"{self.config.base_url}/shop.json")
        self._raise_for_status(response, "fetch shop")
        return response.json().get("shop", {})

    def get_location_id(self, location_name: Optional[str] = None) -> int:
        response = self.request("GET", f"{self.config.base_url}/locations.json")
        self._raise_for_status(response, "fetch locations")
        locations = response.json().get("locations", [])
        expected_name = location_name or self.config.location_name
        for location in locations:
            if location.get("name") == expected_name:
                return int(location["id"])
        names = ", ".join(str(location.get("name")) for location in locations)
        raise ShopifyApiError(f"Shopify location not found: {expected_name}. Available: {names}")

    def iter_variant_mappings(self, location_id: Optional[int] = None) -> Iterable[ShopifyVariantMapping]:
        resolved_location_id = location_id if location_id is not None else self.get_location_id()
        next_url = f"{self.config.base_url}/products.json?limit=250"

        while next_url:
            response = self.request("GET", next_url)
            self._raise_for_status(response, "fetch products")
            payload = response.json()

            for product in payload.get("products", []):
                for variant in product.get("variants", []):
                    sku = (variant.get("sku") or "").strip()
                    inventory_item_id = variant.get("inventory_item_id")
                    variant_id = variant.get("id")
                    product_id = product.get("id")
                    if not sku or not inventory_item_id or not variant_id or not product_id:
                        continue
                    yield ShopifyVariantMapping(
                        sku=sku,
                        shopify_product_id=int(product_id),
                        shopify_variant_id=int(variant_id),
                        inventory_item_id=int(inventory_item_id),
                        location_id=int(resolved_location_id),
                    )

            next_url = _next_link(response.headers.get("Link"))
            if next_url:
                time.sleep(self.config.rate_limit_sleep)

    def iter_catalog_snapshots(self, status: str = "active") -> Iterable[ShopifyCatalogSnapshot]:
        next_url = f"{self.config.base_url}/products.json?limit=250"
        if status != "any":
            next_url += f"&status={status}"

        while next_url:
            response = self.request("GET", next_url)
            self._raise_for_status(response, "fetch products")
            payload = response.json()

            for product in payload.get("products", []):
                product_id = product.get("id")
                if not product_id:
                    continue
                images = product.get("images") or []
                for variant in product.get("variants", []):
                    sku = (variant.get("sku") or "").strip()
                    variant_id = variant.get("id")
                    inventory_item_id = variant.get("inventory_item_id")
                    if not sku or not variant_id or not inventory_item_id:
                        continue
                    yield ShopifyCatalogSnapshot(
                        sku=sku,
                        shopify_product_id=int(product_id),
                        shopify_variant_id=int(variant_id),
                        inventory_item_id=int(inventory_item_id),
                        handle=product.get("handle") or "",
                        title=product.get("title") or "",
                        status=product.get("status") or "",
                        vendor=product.get("vendor") or "",
                        product_type=product.get("product_type") or "",
                        tags=product.get("tags") or "",
                        price=variant.get("price"),
                        compare_at_price=variant.get("compare_at_price"),
                        image_count=len(images),
                    )

            next_url = _next_link(response.headers.get("Link"))
            if next_url:
                time.sleep(self.config.rate_limit_sleep)

    def set_inventory_level(self, *, inventory_item_id: int, location_id: int, available: int):
        response = self.request(
            "POST",
            f"{self.config.base_url}/inventory_levels/set.json",
            json={
                "location_id": location_id,
                "inventory_item_id": inventory_item_id,
                "available": available,
            },
        )
        self._raise_for_status(response, "set inventory level")
        return response.json()

    def update_product_status(self, *, product_id: int, status: str):
        response = self.request(
            "PUT",
            f"{self.config.base_url}/products/{product_id}.json",
            json={"product": {"id": product_id, "status": status}},
        )
        self._raise_for_status(response, "update product status")
        return response.json()

    def create_product(self, payload: Dict) -> Dict:
        response = self.request("POST", f"{self.config.base_url}/products.json", json=payload)
        self._raise_for_status(response, "create product")
        product = response.json().get("product")
        if not product:
            raise ShopifyApiError("Could not create product: missing product in Shopify response")
        return product

    def mapping_from_product(self, product: Dict, *, sku: str, location_id: int) -> ShopifyVariantMapping:
        variant = _variant_for_sku(product, sku)
        return ShopifyVariantMapping(
            sku=sku,
            shopify_product_id=int(product["id"]),
            shopify_variant_id=int(variant["id"]),
            inventory_item_id=int(variant["inventory_item_id"]),
            location_id=int(location_id),
        )

    def snapshot_from_product(self, product: Dict, *, sku: str) -> ShopifyCatalogSnapshot:
        variant = _variant_for_sku(product, sku)
        return ShopifyCatalogSnapshot(
            sku=sku,
            shopify_product_id=int(product["id"]),
            shopify_variant_id=int(variant["id"]),
            inventory_item_id=int(variant["inventory_item_id"]),
            handle=product.get("handle") or "",
            title=product.get("title") or "",
            status=product.get("status") or "",
            vendor=product.get("vendor") or "",
            product_type=product.get("product_type") or "",
            tags=product.get("tags") or "",
            price=variant.get("price"),
            compare_at_price=variant.get("compare_at_price"),
            image_count=len(product.get("images") or []),
        )

    @staticmethod
    def _raise_for_status(response, action):
        if 200 <= response.status_code < 300:
            return
        raise ShopifyApiError(f"Could not {action}: HTTP {response.status_code} {response.text[:500]}")


def _parse_retry_after(value):
    if not value:
        return None
    try:
        return max(0, float(value))
    except ValueError:
        return None


def _next_link(link_header):
    if not link_header:
        return None
    for part in link_header.split(","):
        if 'rel="next"' in part:
            return part.split(";")[0].strip().strip("<>").strip()
    return None


def _variant_for_sku(product: Dict, sku: str) -> Dict:
    for variant in product.get("variants", []):
        if (variant.get("sku") or "").strip() == sku:
            return variant
    raise ShopifyApiError(f"Shopify product response does not contain variant for SKU: {sku}")
