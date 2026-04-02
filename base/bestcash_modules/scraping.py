import json
import logging
from urllib.request import urlopen
from urllib.parse import quote_plus

from .config import CRAWLBASE_TOKEN


def intentar_scraping_con_dominios(asin):
    dominios = ["es", "de", "fr", "it", "com", "com.be", "co.uk", "ca", "nl", "pl", "se"]
    for dominio in dominios:
        try:
            url = f"https://www.amazon.{dominio}/dp/{asin}"
            api = (
                f"https://api.crawlbase.com/?token={CRAWLBASE_TOKEN}"
                f"&scraper=amazon-product-details&url={quote_plus(url)}"
            )
            data = json.loads(urlopen(api, timeout=20).read().decode("utf-8"))
            product = data.get("body", {})
            if (
                product.get("name")
                and (product.get("highResolutionImages") or product.get("images"))
                and (product.get("rawPrice") or product.get("description"))
            ):
                domain_from_url = None
                if product.get("url"):
                    try:
                        domain_from_url = product["url"].split("/")[2]
                    except Exception:
                        domain_from_url = None
                scraping_domain = domain_from_url or f"amazon.{dominio}"
                return product, scraping_domain
        except Exception as exc:
            logging.info("Scraping fallido %s para %s: %s", dominio, asin, exc)
    return None, None
