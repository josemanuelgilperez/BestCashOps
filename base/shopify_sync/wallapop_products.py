import html
import json
import re
from dataclasses import dataclass
from decimal import Decimal
from typing import Iterable, List, Optional


S3_IMAGE_HOST = "bestcashproductimages.s3.amazonaws.com"


@dataclass(frozen=True)
class WallapopProductCandidate:
    asin: str
    stock: int
    price: Optional[Decimal]
    min_price: Optional[Decimal]
    max_price: Optional[Decimal]
    product_rows: int
    title: str
    description: str
    technical_description: str
    features: str
    vendor: str
    product_type: str
    handle: str
    seo_title: str
    seo_description: str
    weight_grams: Optional[Decimal]
    main_image: str
    additional_images: str
    hashtags: str
    mapped: bool
    shopify_status: Optional[str]

    @property
    def images(self) -> List[str]:
        return _parse_image_urls([self.main_image, self.additional_images])

    @property
    def tags(self) -> List[str]:
        tags = ["Wallapop"]
        tags.extend(_parse_hashtags(self.hashtags))
        if self.product_type:
            tags.append(self.product_type)
        return _unique([tag for tag in tags if tag])

    @property
    def issues(self) -> List[str]:
        issues = []
        if not self.asin:
            issues.append("missing_asin")
        if self.product_rows > 1:
            issues.append("duplicated_asin")
        if self.mapped:
            issues.append("already_mapped")
        if self.stock <= 0:
            issues.append("no_stock")
        if not self.title:
            issues.append("missing_title")
        if not self.price or self.price <= 0:
            issues.append("missing_price")
        if self.min_price and self.max_price and self.min_price != self.max_price:
            issues.append("multiple_prices")
        if not self.weight_grams or self.weight_grams <= 0:
            issues.append("missing_weight")
        if not self.images:
            issues.append("missing_image")
        elif not _has_official_image(self.images):
            issues.append("missing_s3_image")
        return issues

    @property
    def is_ready_to_publish(self) -> bool:
        return not self.issues


class WallapopProductReader:
    def __init__(self, connection):
        self.connection = connection

    def load_candidates(
        self,
        *,
        asin: Optional[str] = None,
        missing_only: bool = True,
        ready_only: bool = False,
        limit: Optional[int] = None,
    ) -> List[WallapopProductCandidate]:
        cursor = self.connection.cursor(dictionary=True)
        try:
            params = []
            filters = [
                "ii.shop_id = 5",
                "ii.ok_online = 1",
                "pi.asin IS NOT NULL",
                "pi.asin <> ''",
            ]
            if asin:
                filters.append("pi.asin = %s")
                params.append(asin)
            if missing_only:
                filters.append("sm.sku IS NULL")

            cursor.execute(
                f"""
                SELECT
                    pi.asin,
                    COUNT(ii.id) AS stock,
                    MIN(ii.bestcash_price) AS min_price,
                    MAX(ii.bestcash_price) AS max_price,
                    COUNT(DISTINCT pi.id) AS product_rows,
                    COALESCE(MAX(asp.titulo_amazon), MAX(pi.title), '') AS title,
                    COALESCE(MAX(asp.descripcion), '') AS description,
                    COALESCE(MAX(asp.descripcion_tecnica), '') AS technical_description,
                    COALESCE(MAX(asp.caracteristicas), '') AS features,
                    COALESCE(NULLIF(MAX(asp.vendor), ''), NULLIF(MAX(asp.marca), ''), 'BestCash') AS vendor,
                    COALESCE(MAX(asp.categoria), '') AS product_type,
                    COALESCE(MAX(asp.handle), '') AS handle,
                    COALESCE(MAX(asp.seo_title), '') AS seo_title,
                    COALESCE(MAX(asp.seo_description), '') AS seo_description,
                    MAX(asp.peso) AS weight_grams,
                    COALESCE(MAX(asp.imagen_principal), '') AS main_image,
                    COALESCE(MAX(asp.imagenes_adicionales), '') AS additional_images,
                    COALESCE(MAX(asp.hashtags), '') AS hashtags,
                    MAX(sm.sku) AS mapped_sku,
                    MAX(scs.status) AS shopify_status
                FROM items_info ii
                JOIN references_info ri ON ri.id = ii.reference_id
                JOIN products_info pi ON pi.id = ri.product_id
                LEFT JOIN amazon_scraped_products asp ON BINARY asp.asin = BINARY pi.asin
                LEFT JOIN shopify_mapping sm ON BINARY sm.sku = BINARY pi.asin
                LEFT JOIN shopify_catalog_snapshot scs ON BINARY scs.sku = BINARY pi.asin
                WHERE {" AND ".join(filters)}
                GROUP BY pi.asin
                ORDER BY pi.asin
                """,
                tuple(params),
            )
            candidates = [_candidate_from_row(row) for row in cursor.fetchall()]
            if ready_only:
                candidates = [candidate for candidate in candidates if candidate.is_ready_to_publish]
            if limit:
                candidates = candidates[: int(limit)]
            return candidates
        finally:
            cursor.close()


class ShopifyProductPayloadBuilder:
    def build_product_payload(self, candidate: WallapopProductCandidate, *, status: str = "draft") -> dict:
        if not candidate.is_ready_to_publish:
            raise ValueError(
                f"Candidate {candidate.asin} is not ready to publish: {', '.join(candidate.issues)}"
            )

        return {
            "product": {
                "title": candidate.title,
                "body_html": _build_body_html(candidate),
                "vendor": candidate.vendor or "BestCash",
                "product_type": candidate.product_type,
                "handle": _normalize_handle(candidate.handle) or _fallback_handle(candidate.title, candidate.asin),
                "status": status,
                "tags": ", ".join(candidate.tags),
                "metafields_global_title_tag": candidate.seo_title or candidate.title,
                "metafields_global_description_tag": candidate.seo_description or _plain_excerpt(candidate.description),
                "variants": [
                    {
                        "sku": candidate.asin,
                        "price": _decimal_to_price(candidate.price),
                        "inventory_management": "shopify",
                        "inventory_policy": "deny",
                        "inventory_quantity": candidate.stock,
                        "requires_shipping": True,
                        "taxable": True,
                        "weight": float(candidate.weight_grams),
                        "weight_unit": "g",
                    }
                ],
                "images": [{"src": image_url} for image_url in candidate.images],
            }
        }


def _candidate_from_row(row: dict) -> WallapopProductCandidate:
    min_price = row.get("min_price")
    max_price = row.get("max_price")
    return WallapopProductCandidate(
        asin=(row.get("asin") or "").strip(),
        stock=int(row.get("stock") or 0),
        price=min_price if min_price == max_price else None,
        min_price=min_price,
        max_price=max_price,
        product_rows=int(row.get("product_rows") or 0),
        title=(row.get("title") or "").strip(),
        description=(row.get("description") or "").strip(),
        technical_description=(row.get("technical_description") or "").strip(),
        features=(row.get("features") or "").strip(),
        vendor=(row.get("vendor") or "").strip(),
        product_type=(row.get("product_type") or "").strip(),
        handle=(row.get("handle") or "").strip(),
        seo_title=(row.get("seo_title") or "").strip(),
        seo_description=(row.get("seo_description") or "").strip(),
        weight_grams=row.get("weight_grams"),
        main_image=(row.get("main_image") or "").strip(),
        additional_images=(row.get("additional_images") or "").strip(),
        hashtags=(row.get("hashtags") or "").strip(),
        mapped=bool(row.get("mapped_sku")),
        shopify_status=row.get("shopify_status"),
    )


def _build_body_html(candidate: WallapopProductCandidate) -> str:
    blocks = []
    if candidate.description:
        blocks.append(_paragraphs(candidate.description))
    if candidate.features and candidate.features != candidate.description:
        blocks.append(f"<h3>Caracteristicas</h3>{_paragraphs(candidate.features)}")
    if (
        candidate.technical_description
        and candidate.technical_description != candidate.description
        and candidate.technical_description != candidate.features
    ):
        blocks.append(f"<h3>Detalles tecnicos</h3>{_paragraphs(candidate.technical_description)}")
    if f"Ref. BestCash {candidate.asin}" not in candidate.description:
        blocks.append(f"<p>Ref. BestCash {html.escape(candidate.asin)}</p>")
    return "\n".join(blocks)


def _paragraphs(value: str) -> str:
    paragraphs = [line.strip() for line in re.split(r"\n{2,}", value.strip()) if line.strip()]
    return "".join(f"<p>{html.escape(paragraph).replace(chr(10), '<br>')}</p>" for paragraph in paragraphs)


def _parse_image_urls(values: Iterable[str]) -> List[str]:
    urls = []
    for value in values:
        if not value:
            continue
        parsed = _try_json_list(value)
        if parsed is not None:
            urls.extend(str(item).strip() for item in parsed)
            continue
        urls.extend(part.strip() for part in value.split(","))
    return _unique([url for url in urls if url.startswith(("http://", "https://"))])


def _try_json_list(value: str):
    text = value.strip()
    if not text.startswith("["):
        return None
    try:
        parsed = json.loads(text)
    except json.JSONDecodeError:
        return None
    if isinstance(parsed, list):
        return parsed
    return None


def _parse_hashtags(value: str) -> List[str]:
    if not value:
        return []
    tags = []
    for part in re.split(r"[\s,]+", value):
        tag = part.strip().lstrip("#")
        if tag:
            tags.append(tag)
    return _unique(tags)


def _has_official_image(images: List[str]) -> bool:
    return any(S3_IMAGE_HOST in image or image.endswith("image_not_found.jpg") for image in images)


def _unique(values: Iterable[str]) -> List[str]:
    seen = set()
    unique_values = []
    for value in values:
        if value in seen:
            continue
        seen.add(value)
        unique_values.append(value)
    return unique_values


def _decimal_to_price(value: Decimal) -> str:
    return f"{value:.2f}"


def _plain_excerpt(value: str, limit: int = 320) -> str:
    text = re.sub(r"\s+", " ", value or "").strip()
    return text[:limit]


def _fallback_handle(title: str, asin: str) -> str:
    raw = f"{title}-{asin}".lower()
    return _normalize_handle(raw)


def _normalize_handle(value: str) -> str:
    raw = (value or "").lower()
    raw = re.sub(r"[^a-z0-9]+", "-", raw)
    return raw.strip("-")
