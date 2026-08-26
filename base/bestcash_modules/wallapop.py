import re


WALLAPOP_TITLE_MAX_CHARS = 50
WALLAPOP_DESCRIPTION_MAX_CHARS = 640


def compact_whitespace(value):
    return re.sub(r"\s+", " ", str(value or "")).strip()


def truncate_at_word(value, max_chars):
    text = compact_whitespace(value)
    if len(text) <= max_chars:
        return text

    clipped = text[:max_chars].rstrip()
    if " " in clipped:
        clipped = clipped.rsplit(" ", 1)[0].rstrip(" ,.;:-")
    return clipped or text[:max_chars].rstrip()


def build_wallapop_title(title, fallback_title=None, asin=None):
    source = title or fallback_title or (f"Producto {asin}" if asin else "Producto BestCash")
    return truncate_at_word(source, WALLAPOP_TITLE_MAX_CHARS)


def _clean_description_line(value):
    text = compact_whitespace(value)
    text = re.sub(r"\bREF\.?\s+BESTCASH\s+[A-Z0-9]{10}\b", "", text, flags=re.IGNORECASE)
    return text.strip(" -;")


def build_wallapop_description(
    *,
    asin,
    title=None,
    description=None,
    features=None,
    brand=None,
    size=None,
    color=None,
):
    lines = []

    headline = _clean_description_line(description) or _clean_description_line(title)
    if headline:
        lines.append(headline)

    if features:
        if isinstance(features, str):
            raw_features = re.split(r"[\n|]+", features)
        else:
            raw_features = features
        for feature in raw_features:
            cleaned = _clean_description_line(feature)
            if cleaned and cleaned.lower() not in {line.lower() for line in lines}:
                lines.append(f"- {cleaned}")
            if len(lines) >= 5:
                break

    attrs = []
    if brand:
        attrs.append(f"Marca: {compact_whitespace(brand)}")
    if size:
        attrs.append(f"Talla: {compact_whitespace(size)}")
    if color:
        attrs.append(f"Color: {compact_whitespace(color)}")
    if attrs:
        lines.append(" | ".join(attrs))

    ref_line = f"REF. BESTCASH {asin}"
    reserved = len(ref_line) + 2
    body = "\n".join(lines).strip()
    body = truncate_at_word(body, WALLAPOP_DESCRIPTION_MAX_CHARS - reserved)

    if body:
        return f"{body}\n\n{ref_line}"[:WALLAPOP_DESCRIPTION_MAX_CHARS]
    return ref_line[:WALLAPOP_DESCRIPTION_MAX_CHARS]
