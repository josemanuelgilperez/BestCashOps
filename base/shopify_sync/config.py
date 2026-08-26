import os
from dataclasses import dataclass

try:
    from dotenv import find_dotenv, load_dotenv
except ModuleNotFoundError:
    find_dotenv = None
    load_dotenv = None


if load_dotenv and find_dotenv:
    load_dotenv(find_dotenv())


@dataclass(frozen=True)
class ShopifySyncConfig:
    shop_domain: str
    api_version: str
    access_token: str
    db_host: str
    db_user: str
    db_password: str
    db_name: str
    db_port: int = 3306
    location_name: str = "Lanzarote"
    request_timeout: int = 30
    rate_limit_sleep: float = 0.4

    @classmethod
    def from_env(cls):
        return cls(
            shop_domain=_env("SHOPIFY_SHOP_DOMAIN", "bestcash-outlet.myshopify.com"),
            api_version=_env("SHOPIFY_API_VERSION", "2024-10"),
            access_token=_env("SHOPIFY_TOKEN", ""),
            db_host=_env("DB_HOST", "82.223.203.117"),
            db_user=_env("DB_USER", "bestcash_app"),
            db_password=_env("DB_PASSWORD", ""),
            db_name=_env("DB_NAME", "bestcash"),
            db_port=int(_env("DB_PORT", "3306")),
            location_name=_env("SHOPIFY_LOCATION_NAME", "Lanzarote"),
            request_timeout=int(_env("SHOPIFY_REQUEST_TIMEOUT", "30")),
            rate_limit_sleep=float(_env("SHOPIFY_RATE_LIMIT_SLEEP", "0.4")),
        )

    @property
    def base_url(self):
        return f"https://{self.shop_domain}/admin/api/{self.api_version}"

    def validate_for_shopify(self):
        missing = []
        if not self.shop_domain:
            missing.append("SHOPIFY_SHOP_DOMAIN")
        if not self.api_version:
            missing.append("SHOPIFY_API_VERSION")
        if not self.access_token:
            missing.append("SHOPIFY_TOKEN")
        if missing:
            raise ValueError("Missing Shopify configuration: " + ", ".join(missing))

    def validate_for_db(self):
        missing = []
        if not self.db_host:
            missing.append("DB_HOST")
        if not self.db_user:
            missing.append("DB_USER")
        if not self.db_password:
            missing.append("DB_PASSWORD")
        if not self.db_name:
            missing.append("DB_NAME")
        if missing:
            raise ValueError("Missing database configuration: " + ", ".join(missing))


def _env(name, default):
    value = os.getenv(name)
    if value is None or value == "":
        return default
    return value
