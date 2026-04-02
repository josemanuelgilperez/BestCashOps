import re
import html
import json
from unidecode import unidecode


def sanitize_text(text):
    return html.unescape(text).strip() if text else ""


def parse_price(value):
    if value is None:
        return None

    try:
        raw = str(value).strip()
        cleaned = re.sub(r"[^\d,.\-]", "", raw)
        if not cleaned:
            return None

        if "," in cleaned and "." in cleaned:
            if cleaned.rfind(",") > cleaned.rfind("."):
                cleaned = cleaned.replace(".", "").replace(",", ".")
            else:
                cleaned = cleaned.replace(",", "")
        elif "," in cleaned:
            cleaned = cleaned.replace(".", "").replace(",", ".")
        elif cleaned.count(".") > 1:
            parts = cleaned.split(".")
            cleaned = "".join(parts[:-1]) + "." + parts[-1]

        return float(cleaned)
    except (TypeError, ValueError):
        return None


def clean_price(price):
    return price if price is not None and 0 < price < 10000 else None


def get_product_price(product):
    if not product:
        return None
    candidates = [
        product.get("rawPrice"),
        (product.get("price") or {}).get("amount") if isinstance(product.get("price"), dict) else None,
        (product.get("price") or {}).get("value") if isinstance(product.get("price"), dict) else None,
    ]
    for candidate in candidates:
        parsed = parse_price(candidate)
        if parsed is not None:
            return parsed
    return None


def generate_shopify_handle(title, asin):
    text = unidecode(title)
    text = re.sub(r"[^a-zA-Z0-9\s]", "", text).lower()
    return f"{'-'.join(text.split())}-{asin}"


def traducir_categoria(gl_key):
    gl_mapeo = {
        "gl_apparel": "Ropa",
        "gl_baby_product": "Productos para bebe",
        "gl_beauty": "Belleza",
        "gl_electronics": "Electronica",
        "gl_furniture": "Muebles",
        "gl_home": "Hogar",
        "gl_home_improvement": "Mejoras del hogar",
        "gl_kitchen": "Cocina",
        "gl_lawn_and_garden": "Jardin y exteriores",
        "gl_luggage": "Equipaje",
        "gl_musical_instruments": "Instrumentos musicales",
        "gl_pet_products": "Productos para mascotas",
        "gl_shoes": "Calzado",
        "gl_sports": "Deportes y aire libre",
        "gl_tools": "Herramientas",
        "gl_wine": "Vinos",
        "gl_wireless": "Dispositivos inalambricos / moviles",
    }
    return gl_mapeo.get((gl_key or "").strip(), None)


def extraer_dimensiones_y_peso(product):
    product_information = product.get("productInformation", []) if product else []
    if not product_information:
        return None, None

    dim_keys = [
        "dimensiones del producto",
        "dimensiones del paquete",
        "dimensiones articulo",
        "dimensiones del articulo (profundidad x ancho x alto)",
        "dimensiones del producto: largo x ancho x alto",
        "product dimensions",
        "package dimensions",
    ]
    weight_keys = [
        "peso del producto",
        "recomendacion de peso maximo",
        "peso del articulo",
        "item weight",
        "shipping weight",
    ]

    dimensions = None
    weight_text = None

    normalized_dim_keys = [re.sub(r"\s|\u200f", "", k).lower() for k in dim_keys]
    normalized_weight_keys = [re.sub(r"\s|\u200f", "", k).lower() for k in weight_keys]

    for item in product_information:
        name = re.sub(r"\s|\u200f", "", str(item.get("name", ""))).lower()
        value = str(item.get("value", "")).strip()
        if not value:
            continue

        if name in normalized_dim_keys or "dimension" in name or "dimensiones" in name:
            if ";" in value:
                parts = [p.strip() for p in value.split(";")]
                if len(parts) >= 2:
                    dimensions = parts[0] or dimensions
                    weight_text = parts[1] or weight_text
                    continue
            dimensions = value
        elif name in normalized_weight_keys or "weight" in name or "peso" in name:
            weight_text = value

    peso = None
    if weight_text:
        try:
            weight_clean = re.sub(r"[^\d.,]", "", weight_text).replace(",", ".")
            weight_val = float(weight_clean)
            text_lower = weight_text.lower()
            if "kg" in text_lower or "kilogram" in text_lower:
                peso = weight_val * 1000
            elif "lb" in text_lower or "pound" in text_lower or "libr" in text_lower:
                peso = weight_val * 453.592
            elif "oz" in text_lower or "onza" in text_lower:
                peso = weight_val * 28.3495
            else:
                peso = weight_val
        except (TypeError, ValueError):
            peso = None

    return dimensions, peso


def construir_texto_dimensiones_peso(product):
    if not product:
        return ""
    bloques = []
    for item in product.get("productInformation", []) or []:
        name = (item.get("name") or "").strip()
        value = (item.get("value") or "").strip()
        if name or value:
            bloques.append(f"{name}: {value}".strip(": "))
    return "\n".join(bloques).strip()


def normalize_for_mysql_value(value):
    if isinstance(value, list):
        return " | ".join(str(v).strip() for v in value if v is not None)
    if isinstance(value, dict):
        return json.dumps(value, ensure_ascii=False)
    if isinstance(value, str):
        return value.strip()
    return value


def normalize_payload_dict(payload):
    return {key: normalize_for_mysql_value(val) for key, val in payload.items()}
