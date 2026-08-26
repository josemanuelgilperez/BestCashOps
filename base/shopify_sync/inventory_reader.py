from dataclasses import dataclass
from typing import Dict, Iterable, List, Optional


@dataclass(frozen=True)
class StockRecord:
    sku: str
    stock: int
    previous_stock: Optional[int]
    current_shopify_status: Optional[str]
    shopify_product_id: int
    inventory_item_id: int
    location_id: int

    @property
    def target_status(self):
        return "active" if self.stock > 0 else "archived"

    @property
    def needs_update(self):
        return self.previous_stock != self.stock or (
            self.current_shopify_status is not None
            and self.current_shopify_status != self.target_status
        )


class InventoryReader:
    def __init__(self, connection):
        self.connection = connection

    def load_stock_records(self, asin: Optional[str] = None, changed_only: bool = True) -> List[StockRecord]:
        previous_stock = self.load_previous_stock()
        real_stock = self.load_real_stock(asin=asin)

        records = []
        for row in real_stock:
            record = StockRecord(
                sku=row["sku"],
                stock=int(row["stock"] or 0),
                previous_stock=previous_stock.get(row["sku"]),
                current_shopify_status=row.get("current_shopify_status"),
                shopify_product_id=int(row["shopify_product_id"]),
                inventory_item_id=int(row["inventory_item_id"]),
                location_id=int(row["location_id"]),
            )
            if changed_only and not record.needs_update:
                continue
            records.append(record)
        return records

    def load_previous_stock(self) -> Dict[str, int]:
        cursor = self.connection.cursor(dictionary=True)
        try:
            cursor.execute("SELECT asin, last_stock FROM stock_sync_log")
            return {row["asin"]: int(row["last_stock"]) for row in cursor.fetchall()}
        finally:
            cursor.close()

    def load_real_stock(self, asin: Optional[str] = None) -> Iterable[dict]:
        cursor = self.connection.cursor(dictionary=True)
        try:
            has_snapshot = self._table_exists("shopify_catalog_snapshot")
            snapshot_select = "scs.status AS current_shopify_status," if has_snapshot else "NULL AS current_shopify_status,"
            snapshot_join = (
                "LEFT JOIN shopify_catalog_snapshot scs ON BINARY scs.sku = BINARY sm.sku"
                if has_snapshot
                else ""
            )
            params = []
            asin_filter = ""
            if asin:
                asin_filter = "WHERE sm.sku = %s"
                params.append(asin)

            cursor.execute(
                f"""
                SELECT
                    sm.sku,
                    sm.shopify_product_id,
                    sm.inventory_item_id,
                    sm.location_id,
                    {snapshot_select}
                    COUNT(ii.id) AS stock
                FROM shopify_mapping sm
                LEFT JOIN products_info pi ON BINARY pi.asin = BINARY sm.sku
                LEFT JOIN references_info ri ON ri.product_id = pi.id
                LEFT JOIN items_info ii ON ii.reference_id = ri.id AND ii.ok_online = 1
                {snapshot_join}
                {asin_filter}
                GROUP BY
                    sm.sku,
                    sm.shopify_product_id,
                    sm.inventory_item_id,
                    sm.location_id,
                    current_shopify_status
                ORDER BY sm.sku
                """,
                tuple(params),
            )
            return cursor.fetchall()
        finally:
            cursor.close()

    def _table_exists(self, table_name: str) -> bool:
        cursor = self.connection.cursor()
        try:
            cursor.execute("SHOW TABLES LIKE %s", (table_name,))
            return cursor.fetchone() is not None
        finally:
            cursor.close()
