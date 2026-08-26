import csv
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import Iterable, List, Optional


@dataclass(frozen=True)
class SheetPriceRow:
    asin: str
    price: Decimal


@dataclass(frozen=True)
class SheetPriceUpdate:
    asin: str
    price: Decimal
    matched_items: int
    dry_run: bool
    updated: bool
    error: Optional[str] = None


@dataclass(frozen=True)
class SheetPriceImportResult:
    total_seen: int
    total_updated: int
    total_errors: int
    actions: List[SheetPriceUpdate]


class WallapopSheetPriceImporter:
    def __init__(self, connection):
        self.connection = connection

    def apply_prices(self, rows: Iterable[SheetPriceRow], *, dry_run: bool = True) -> SheetPriceImportResult:
        actions = []
        for row in rows:
            actions.append(self.apply_price(row, dry_run=dry_run))

        return SheetPriceImportResult(
            total_seen=len(actions),
            total_updated=sum(1 for action in actions if action.updated),
            total_errors=sum(1 for action in actions if action.error),
            actions=actions,
        )

    def apply_price(self, row: SheetPriceRow, *, dry_run: bool = True) -> SheetPriceUpdate:
        cursor = self.connection.cursor()
        try:
            cursor.execute(
                """
                SELECT COUNT(*)
                FROM items_info ii
                JOIN references_info ri ON ri.id = ii.reference_id
                JOIN products_info pi ON pi.id = ri.product_id
                WHERE ii.shop_id = 5
                  AND ii.ok_online = 1
                  AND BINARY pi.asin = BINARY %s
                """,
                (row.asin,),
            )
            matched_items = int(cursor.fetchone()[0])
            if dry_run:
                return SheetPriceUpdate(
                    asin=row.asin,
                    price=row.price,
                    matched_items=matched_items,
                    dry_run=True,
                    updated=False,
                )

            cursor.execute(
                """
                UPDATE items_info ii
                JOIN references_info ri ON ri.id = ii.reference_id
                JOIN products_info pi ON pi.id = ri.product_id
                SET ii.bestcash_price = %s
                WHERE ii.shop_id = 5
                  AND ii.ok_online = 1
                  AND BINARY pi.asin = BINARY %s
                """,
                (row.price, row.asin),
            )
            return SheetPriceUpdate(
                asin=row.asin,
                price=row.price,
                matched_items=matched_items,
                dry_run=False,
                updated=True,
            )
        except Exception as exc:
            return SheetPriceUpdate(
                asin=row.asin,
                price=row.price,
                matched_items=0,
                dry_run=dry_run,
                updated=False,
                error=str(exc),
            )
        finally:
            cursor.close()


def load_sheet_price_rows(csv_path: str) -> List[SheetPriceRow]:
    rows = []
    with open(csv_path, newline="", encoding="utf-8-sig") as handle:
        reader = csv.DictReader(handle)
        for raw in reader:
            asin = (raw.get("asin") or "").strip().upper()
            price = _parse_price(raw.get("sheet_price"))
            if not asin or price is None or price <= 0:
                continue
            rows.append(SheetPriceRow(asin=asin, price=price))
    return rows


def _parse_price(value) -> Optional[Decimal]:
    if value is None:
        return None
    text = str(value).strip().replace("€", "").replace(",", ".")
    if not text:
        return None
    try:
        return Decimal(text)
    except InvalidOperation:
        return None
