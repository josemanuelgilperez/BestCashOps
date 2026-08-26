import unittest
from decimal import Decimal
from tempfile import NamedTemporaryFile

from base.shopify_sync.config import ShopifySyncConfig
from base.shopify_sync.inventory_reader import StockRecord
from base.shopify_sync.sheet_price_importer import load_sheet_price_rows
from base.shopify_sync.shopify_client import ShopifyApiError, ShopifyClient, _next_link
from base.shopify_sync.stock_sync import StockSynchronizer
from base.shopify_sync.wallapop_products import ShopifyProductPayloadBuilder, WallapopProductCandidate
from base.shopify_sync.wallapop_publisher import WallapopPublisher


class ShopifySyncConfigTest(unittest.TestCase):
    def test_base_url_uses_domain_and_version(self):
        config = ShopifySyncConfig(
            shop_domain="example.myshopify.com",
            api_version="2024-10",
            access_token="token",
            db_host="db.example.com",
            db_user="user",
            db_password="password",
            db_name="bestcash",
        )

        self.assertEqual(
            config.base_url,
            "https://example.myshopify.com/admin/api/2024-10",
        )

    def test_shopify_validation_reports_missing_token(self):
        config = ShopifySyncConfig(
            shop_domain="example.myshopify.com",
            api_version="2024-10",
            access_token="",
            db_host="db.example.com",
            db_user="user",
            db_password="password",
            db_name="bestcash",
        )

        with self.assertRaisesRegex(ValueError, "SHOPIFY_TOKEN"):
            config.validate_for_shopify()


class ShopifyPaginationTest(unittest.TestCase):
    def test_next_link_extracts_rel_next_url(self):
        header = (
            '<https://shop/admin/api/2024-10/products.json?page_info=abc&limit=250>; rel="next", '
            '<https://shop/admin/api/2024-10/products.json?page_info=old&limit=250>; rel="previous"'
        )

        self.assertEqual(
            _next_link(header),
            "https://shop/admin/api/2024-10/products.json?page_info=abc&limit=250",
        )

    def test_next_link_returns_none_when_absent(self):
        self.assertIsNone(_next_link(None))
        self.assertIsNone(_next_link('<https://shop/products.json?page_info=old>; rel="previous"'))


class ShopifyCreatedProductTest(unittest.TestCase):
    def test_mapping_from_created_product_uses_matching_sku(self):
        client = _shopify_client_without_init()
        mapping = client.mapping_from_product(_created_product(), sku="B012345678", location_id=321)

        self.assertEqual(mapping.sku, "B012345678")
        self.assertEqual(mapping.shopify_product_id, 123)
        self.assertEqual(mapping.shopify_variant_id, 456)
        self.assertEqual(mapping.inventory_item_id, 789)
        self.assertEqual(mapping.location_id, 321)

    def test_snapshot_from_created_product_uses_matching_sku(self):
        client = _shopify_client_without_init()
        snapshot = client.snapshot_from_product(_created_product(), sku="B012345678")

        self.assertEqual(snapshot.sku, "B012345678")
        self.assertEqual(snapshot.shopify_product_id, 123)
        self.assertEqual(snapshot.status, "draft")
        self.assertEqual(snapshot.image_count, 2)
        self.assertEqual(snapshot.price, "24.95")

    def test_created_product_requires_matching_sku(self):
        client = _shopify_client_without_init()

        with self.assertRaises(ShopifyApiError):
            client.mapping_from_product(_created_product(), sku="MISSING", location_id=321)


class ShopifyCatalogSnapshotTest(unittest.TestCase):
    def test_catalog_snapshot_active_includes_status_filter(self):
        client = FakeCatalogShopifyClient(status_code=200)

        list(client.iter_catalog_snapshots(status="active"))

        self.assertIn("status=active", client.requested_urls[0])

    def test_catalog_snapshot_any_omits_status_filter(self):
        client = FakeCatalogShopifyClient(status_code=200)

        list(client.iter_catalog_snapshots(status="any"))

        self.assertNotIn("status=", client.requested_urls[0])


class SheetPriceImporterTest(unittest.TestCase):
    def test_load_sheet_price_rows_parses_valid_prices(self):
        with NamedTemporaryFile("w+", encoding="utf-8", newline="", suffix=".csv") as handle:
            handle.write('asin,sheet_price\nB012345678,12.95\nB087654321,"9,95"\nEMPTY,\n')
            handle.flush()

            rows = load_sheet_price_rows(handle.name)

        self.assertEqual([row.asin for row in rows], ["B012345678", "B087654321"])
        self.assertEqual([row.price for row in rows], [Decimal("12.95"), Decimal("9.95")])


class StockRecordTest(unittest.TestCase):
    def test_target_status_follows_stock(self):
        active = _stock_record(stock=3, previous_stock=1)
        draft = _stock_record(stock=0, previous_stock=1)

        self.assertEqual(active.target_status, "active")
        self.assertEqual(draft.target_status, "archived")

    def test_needs_update_compares_previous_stock(self):
        self.assertTrue(_stock_record(stock=3, previous_stock=1).needs_update)
        self.assertFalse(_stock_record(stock=3, previous_stock=3).needs_update)

    def test_needs_update_when_shopify_status_differs(self):
        self.assertTrue(
            _stock_record(
                stock=0,
                previous_stock=0,
                current_shopify_status="active",
            ).needs_update
        )
        self.assertFalse(
            _stock_record(
                stock=0,
                previous_stock=0,
                current_shopify_status="archived",
            ).needs_update
        )


class StockSynchronizerTest(unittest.TestCase):
    def test_dry_run_does_not_call_shopify_or_db(self):
        client = FakeShopifyClient()
        connection = FakeConnection()

        action = StockSynchronizer(connection, client).sync_record(
            _stock_record(stock=2, previous_stock=1),
            dry_run=True,
        )

        self.assertTrue(action.dry_run)
        self.assertFalse(action.updated)
        self.assertEqual(client.calls, [])
        self.assertEqual(connection.executed, [])

    def test_write_updates_inventory_status_and_log(self):
        client = FakeShopifyClient()
        connection = FakeConnection()

        action = StockSynchronizer(connection, client, rate_limit_sleep=0).sync_record(
            _stock_record(stock=0, previous_stock=4),
            dry_run=False,
        )

        self.assertFalse(action.dry_run)
        self.assertTrue(action.updated)
        self.assertEqual(
            client.calls,
            [
                ("set_inventory_level", 789, 321, 0),
                ("update_product_status", 123, "archived"),
            ],
        )
        self.assertEqual(len(connection.executed), 3)
        self.assertIn("INSERT INTO stock_sync_log", connection.executed[0][0])
        self.assertIn("SHOW TABLES LIKE", connection.executed[1][0])
        self.assertIn("UPDATE shopify_catalog_snapshot", connection.executed[2][0])


class WallapopProductCandidateTest(unittest.TestCase):
    def test_ready_candidate_uses_shop_id_5_product_data(self):
        candidate = _wallapop_candidate()

        self.assertTrue(candidate.is_ready_to_publish)
        self.assertEqual(candidate.issues, [])
        self.assertEqual(
            candidate.images,
            [
                "https://bestcashproductimages.s3.amazonaws.com/B012345678/B012345678_1.jpg",
                "https://bestcashproductimages.s3.amazonaws.com/B012345678/B012345678_2.jpg",
            ],
        )
        self.assertIn("Wallapop", candidate.tags)
        self.assertIn("Herramientas", candidate.tags)

    def test_candidate_with_multiple_prices_is_not_ready(self):
        candidate = _wallapop_candidate(price=None, min_price=Decimal("10.00"), max_price=Decimal("12.00"))

        self.assertFalse(candidate.is_ready_to_publish)
        self.assertIn("multiple_prices", candidate.issues)
        self.assertIn("missing_price", candidate.issues)

    def test_candidate_requires_official_s3_image(self):
        candidate = _wallapop_candidate(
            main_image="https://images-na.ssl-images-amazon.com/images/I/example.jpg",
            additional_images="",
        )

        self.assertFalse(candidate.is_ready_to_publish)
        self.assertIn("missing_s3_image", candidate.issues)


class ShopifyProductPayloadBuilderTest(unittest.TestCase):
    def test_builds_draft_product_payload(self):
        payload = ShopifyProductPayloadBuilder().build_product_payload(_wallapop_candidate())
        product = payload["product"]

        self.assertEqual(product["title"], "Taladro BestCash")
        self.assertEqual(product["status"], "draft")
        self.assertEqual(product["handle"], "taladro-bestcash-b012345678")
        self.assertEqual(product["variants"][0]["sku"], "B012345678")
        self.assertEqual(product["variants"][0]["price"], "24.95")
        self.assertEqual(product["variants"][0]["inventory_quantity"], 2)
        self.assertEqual(product["variants"][0]["weight"], 500.0)
        self.assertEqual(len(product["images"]), 2)


class WallapopPublisherTest(unittest.TestCase):
    def test_dry_run_does_not_call_shopify_or_db(self):
        client = FakeProductShopifyClient()
        connection = FakeConnection()

        action = WallapopPublisher(
            connection,
            client,
            location_id=321,
            rate_limit_sleep=0,
        ).publish_candidate(_wallapop_candidate(), dry_run=True)

        self.assertTrue(action.dry_run)
        self.assertFalse(action.created)
        self.assertEqual(client.calls, [])
        self.assertEqual(connection.executed, [])

    def test_write_creates_product_inventory_mapping_and_snapshot(self):
        client = FakeProductShopifyClient()
        connection = FakeConnection()

        action = WallapopPublisher(
            connection,
            client,
            location_id=321,
            rate_limit_sleep=0,
        ).publish_candidate(_wallapop_candidate(), dry_run=False)

        self.assertFalse(action.dry_run)
        self.assertTrue(action.created)
        self.assertEqual(action.shopify_product_id, 123)
        self.assertEqual(
            client.calls,
            [
                ("create_product", "B012345678"),
                ("set_inventory_level", 789, 321, 2),
            ],
        )
        self.assertIn("INSERT INTO shopify_mapping", connection.executed[0][0])
        self.assertIn("CREATE TABLE IF NOT EXISTS shopify_catalog_snapshot", connection.executed[1][0])
        self.assertIn("INSERT INTO shopify_catalog_snapshot", connection.executed[2][0])


def _stock_record(stock, previous_stock, current_shopify_status=None):
    return StockRecord(
        sku="B012345678",
        stock=stock,
        previous_stock=previous_stock,
        current_shopify_status=current_shopify_status,
        shopify_product_id=123,
        inventory_item_id=789,
        location_id=321,
    )


def _wallapop_candidate(**overrides):
    data = {
        "asin": "B012345678",
        "stock": 2,
        "price": Decimal("24.95"),
        "min_price": Decimal("24.95"),
        "max_price": Decimal("24.95"),
        "product_rows": 1,
        "title": "Taladro BestCash",
        "description": "Descripcion principal",
        "technical_description": "Detalle tecnico",
        "features": "Funcion A\nFuncion B",
        "vendor": "BestCash",
        "product_type": "Herramientas",
        "handle": "taladro-bestcash-b012345678",
        "seo_title": "Taladro BestCash",
        "seo_description": "Compra Taladro BestCash",
        "weight_grams": Decimal("500"),
        "main_image": "https://bestcashproductimages.s3.amazonaws.com/B012345678/B012345678_1.jpg",
        "additional_images": "https://bestcashproductimages.s3.amazonaws.com/B012345678/B012345678_2.jpg",
        "hashtags": "#Herramientas #Bricolaje",
        "mapped": False,
        "shopify_status": None,
    }
    data.update(overrides)
    return WallapopProductCandidate(**data)


def _shopify_client_without_init():
    return ShopifyClient.__new__(ShopifyClient)


def _created_product():
    return {
        "id": 123,
        "handle": "taladro-bestcash-b012345678",
        "title": "Taladro BestCash",
        "status": "draft",
        "vendor": "BestCash",
        "product_type": "Herramientas",
        "tags": "Wallapop, Herramientas",
        "images": [{"id": 1}, {"id": 2}],
        "variants": [
            {
                "id": 456,
                "sku": "B012345678",
                "inventory_item_id": 789,
                "price": "24.95",
                "compare_at_price": None,
            }
        ],
    }


class FakeShopifyClient:
    def __init__(self):
        self.calls = []

    def set_inventory_level(self, *, inventory_item_id, location_id, available):
        self.calls.append(("set_inventory_level", inventory_item_id, location_id, available))

    def update_product_status(self, *, product_id, status):
        self.calls.append(("update_product_status", product_id, status))


class FakeProductShopifyClient:
    def __init__(self):
        self.calls = []

    def create_product(self, payload):
        self.calls.append(("create_product", payload["product"]["variants"][0]["sku"]))
        return _created_product()

    def mapping_from_product(self, product, *, sku, location_id):
        return _shopify_client_without_init().mapping_from_product(product, sku=sku, location_id=location_id)

    def snapshot_from_product(self, product, *, sku):
        return _shopify_client_without_init().snapshot_from_product(product, sku=sku)

    def set_inventory_level(self, *, inventory_item_id, location_id, available):
        self.calls.append(("set_inventory_level", inventory_item_id, location_id, available))


class FakeCatalogShopifyClient(ShopifyClient):
    def __init__(self, status_code):
        self.config = ShopifySyncConfig(
            shop_domain="example.myshopify.com",
            api_version="2024-10",
            access_token="token",
            db_host="db.example.com",
            db_user="user",
            db_password="password",
            db_name="bestcash",
        )
        self.status_code = status_code
        self.requested_urls = []

    def request(self, method, url, **kwargs):
        self.requested_urls.append(url)
        return FakeResponse(self.status_code, {"products": []})


class FakeResponse:
    def __init__(self, status_code, payload):
        self.status_code = status_code
        self.payload = payload
        self.text = ""
        self.headers = {}

    def json(self):
        return self.payload


class FakeConnection:
    def __init__(self):
        self.executed = []

    def cursor(self):
        return FakeCursor(self.executed)


class FakeCursor:
    def __init__(self, executed):
        self.executed = executed

    def execute(self, query, params=None):
        self.executed.append((query, params))

    def fetchone(self):
        return ("shopify_catalog_snapshot",)

    def close(self):
        pass


if __name__ == "__main__":
    unittest.main()
