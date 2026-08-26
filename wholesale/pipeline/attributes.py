import re


NO_RELIABLE_ASINS = {
    # Packs or descriptions without a concrete variant.
    "B07Q8KDHH8",
    "B0B4CWVHCF",
    "B0BPHK41DQ",
    "B0CYP2SBRC",
    "B0DBLHFHZL",
    "B0DBLLCYSP",
    "B0DNB1925F",
    "B0F13NJTGH",
    "B0FC2PSJGT",
}


MANUAL_VARIANTS = {
    "B00ZPT9O3A": ("40", "gris"),
    "B01N0QTHHO": ("40", "negro"),
    "B07DRQT38B": ("41", "dorado"),
    "B084MJ3TR7": ("37", "negro"),
    "B08R9Q6RT3": ("40", "rojo"),
    "B0932MYJ1C": ("41", "gris"),
    "B0967TD1WQ": ("31", "dorado"),
    "B0967VPG45": ("33.5", "negro charol"),
    "B09HHP92NB": ("40", "camel"),
    "B09YY2NBJY": ("35", "negro PU"),
    "B0C8TDR6S5": ("40", "caqui/blanco"),
    "B0CCFBXJD3": ("36", "blanco"),
    "B0CKWSQ5RT": ("39", "negro"),
    "B0CKWTC5C5": ("41", "negro"),
    "B0CVXHXD32": ("43", "negro"),
    "B0CYWR14G2": ("42", "negro/caqui"),
    "B0D4QY4HM9": ("39", "camel"),
    "B0D7Q4L881": ("40", "blanco"),
    "B0DBPK9JPT": ("39", "negro/caqui"),
    "B0DQSKSYXS": ("41", "negro"),
    "B0DRR8KKS8": ("43-44", "negro"),
    "B0DWJT7LC2": ("38.5", "gris"),
    "B0F485646V": ("38", "negro"),
}


COLOR_RULES = sorted(
    [
        ("schwarz/pat", "negro charol"),
        ("black patent", "negro charol"),
        ("patent black", "negro charol"),
        ("schwarz/pu", "negro PU"),
        ("schwarz-pu", "negro PU"),
        ("schwarz suede", "negro ante"),
        ("schwarz-suede", "negro ante"),
        ("todo negro", "negro"),
        ("all black", "negro"),
        ("mattschwarz", "negro mate"),
        ("black", "negro"),
        ("schwarz", "negro"),
        ("negro", "negro"),
        ("nero", "negro"),
        ("noir", "negro"),
        ("preto", "negro"),
        ("zwart", "negro"),
        ("negra", "negro"),
        ("negros", "negro"),
        ("negras", "negro"),
        ("khakiweiß", "caqui/blanco"),
        ("white/silver", "blanco/plata"),
        ("weiss/pu", "blanco PU"),
        ("weiß/pu", "blanco PU"),
        ("weiss", "blanco"),
        ("weiß", "blanco"),
        ("white", "blanco"),
        ("blanco", "blanco"),
        ("blanc", "blanco"),
        ("bianco", "blanco"),
        ("branco", "blanco"),
        ("gold/glitzer", "dorado glitter"),
        ("gold-o", "dorado"),
        ("gold glitter", "dorado glitter"),
        ("gold", "dorado"),
        ("golden", "dorado"),
        ("dorado", "dorado"),
        ("silber-glitzer", "plata glitter"),
        ("silber glitzer", "plata glitter"),
        ("silver glitter", "plata glitter"),
        ("silber", "plata"),
        ("silver", "plata"),
        ("plata", "plata"),
        ("argento", "plata"),
        ("gris plata", "gris/plata"),
        ("marrón", "marron"),
        ("marron", "marron"),
        ("brown", "marron"),
        ("braun", "marron"),
        ("bräunlich-gelb", "marron amarillento"),
        ("bräunlichgelb", "marron amarillento"),
        ("tan", "marron claro"),
        ("cognac", "cognac"),
        ("camel", "camel"),
        ("taupe", "taupe"),
        ("caqui", "caqui"),
        ("khaki", "caqui"),
        ("beige", "beige"),
        ("nackt/wildleder", "nude ante"),
        ("nude suede", "nude ante"),
        ("nackt", "nude"),
        ("nude", "nude"),
        ("schwarz khaki", "negro/caqui"),
        ("navy-wildleder", "azul marino ante"),
        ("navy suede", "azul marino ante"),
        ("navy", "azul marino"),
        ("azul zafiro", "azul zafiro"),
        ("sapphire blue", "azul zafiro"),
        ("himmelblau", "azul claro"),
        ("sky blue", "azul claro"),
        ("dark blue", "azul oscuro"),
        ("dunkelblau grün", "azul oscuro/verde"),
        ("dunkelblau", "azul oscuro"),
        ("blau", "azul"),
        ("blue", "azul"),
        ("azul", "azul"),
        ("bleu", "azul"),
        ("blu", "azul"),
        ("grau-blau", "gris/azul"),
        ("grau-braun", "gris/marron"),
        ("grigio-marrone", "gris/marron"),
        ("grey", "gris"),
        ("gray", "gris"),
        ("grau", "gris"),
        ("gris", "gris"),
        ("grigio", "gris"),
        ("rosa/pu", "rosa PU"),
        ("rosa", "rosa"),
        ("pink", "rosa"),
        ("rose", "rosa"),
        ("fucsia", "fucsia"),
        ("fuchsia", "fucsia"),
        ("viola", "violeta"),
        ("violet", "violeta"),
        ("violeta", "violeta"),
        ("purple", "morado"),
        ("morado", "morado"),
        ("lila", "lila"),
        ("lavender", "lavanda"),
        ("burdeos", "burdeos"),
        ("bordeaux", "burdeos"),
        ("burgundy", "burdeos"),
        ("weinrot", "burdeos"),
        ("green", "verde"),
        ("grün", "verde"),
        ("verde", "verde"),
        ("vert", "verde"),
        ("gelb", "amarillo"),
        ("yellow", "amarillo"),
        ("amarillo", "amarillo"),
        ("orange", "naranja"),
        ("naranja", "naranja"),
        ("rot", "rojo"),
        ("rosso", "rojo"),
        ("red", "rojo"),
        ("rojo", "rojo"),
        ("rouge", "rojo"),
        ("floral blanc", "floral blanco"),
        ("multicolor", "multicolor"),
        ("mehrfarbig", "multicolor"),
        ("süße donuts", "donuts"),
    ],
    key=lambda item: len(item[0]),
    reverse=True,
)


SIZE_PATTERNS = [
    re.compile(
        r"(?:Size|Taille|Talla|Größe|Groesse|Gr\.?|Numeric_|Taglia|Tamanho|Pointure)"
        r"[\s:_-]*(\d{2}(?:[.]\d)?(?:\s*/\s*\d{2})?)(?:\s*[-/]\s*(\d{2}))?\s*(?:EU|EUR)?",
        re.IGNORECASE,
    ),
    re.compile(r"(?:EU|EUR)\s*(\d{2}(?:[.]\d)?(?:\s*/\s*\d{2})?)(?:\s*[-/]\s*(\d{2}))?", re.IGNORECASE),
    re.compile(r"(\d{2}(?:[.]\d)?(?:\s*/\s*\d{2})?)(?:\s*[-/]\s*(\d{2}))?\s*(?:EU|EUR)\b", re.IGNORECASE),
    re.compile(r"\b(\d{2}(?:[.]\d)?(?:\s*/\s*\d{2})?)\s*\(EUR\)", re.IGNORECASE),
    re.compile(r"(?:^|[\s,(_-])(\d{2}(?:[.]\d)?(?:\s*/\s*\d{2})?)(?:\s*[-/]\s*(\d{2}))?(?=[\s).,-]*$)", re.IGNORECASE),
]


def _normalize_size(value):
    return str(value or "").replace(",", ".").replace(" ", "")


def extract_size(itemdesc):
    text = str(itemdesc or "").replace(",", " ")
    found = []
    for pattern in SIZE_PATTERNS:
        for match in pattern.finditer(text):
            value = _normalize_size(match.group(1))
            if match.group(2):
                value = f"{value}-{_normalize_size(match.group(2))}"
            first_number = re.search(r"\d{2}(?:\.\d)?", value)
            if not first_number:
                continue
            numeric = float(first_number.group(0))
            if 20 <= numeric <= 50 and value not in found:
                found.append(value)
    return found[-1] if found else ""


def extract_color(itemdesc):
    text = str(itemdesc or "").lower()
    for needle, label in COLOR_RULES:
        if needle in text:
            return label
    return ""


def extract_size_color(asin, itemdesc):
    asin = (asin or "").strip().upper()
    if asin in MANUAL_VARIANTS:
        return MANUAL_VARIANTS[asin]
    if asin in NO_RELIABLE_ASINS:
        return "", ""

    size = extract_size(itemdesc)
    color = extract_color(itemdesc)
    if not size or not color:
        return "", ""
    return size, color


def preload_delivery_descriptions(cursor, asins):
    asins = sorted({str(asin).strip().upper() for asin in asins if asin})
    if not asins:
        return {}

    descriptions = {}
    for offset in range(0, len(asins), 500):
        batch = asins[offset : offset + 500]
        placeholders = ",".join(["%s"] * len(batch))
        cursor.execute(
            f"""
            SELECT Asin, MAX(ItemDesc)
            FROM amazon_delivery
            WHERE Asin IN ({placeholders})
              AND ItemDesc IS NOT NULL
            GROUP BY Asin
            """,
            tuple(batch),
        )
        for asin, itemdesc in cursor.fetchall():
            key = str(asin or "").strip().upper()
            if key and key not in descriptions:
                descriptions[key] = itemdesc or ""

    return descriptions
