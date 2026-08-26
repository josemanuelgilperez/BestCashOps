const TITLE_MAX = 50;
const DESCRIPTION_MAX = 640;

function cleanText(value) {
  return String(value || "").replace(/\s+/g, " ").trim();
}

function truncateAtWord(value, maxChars) {
  const text = cleanText(value);
  if (text.length <= maxChars) return text;
  const clipped = text.slice(0, maxChars).trim().replace(/[ ,.;:-]+$/g, "");
  const lastSpace = clipped.lastIndexOf(" ");
  return lastSpace > 0 ? clipped.slice(0, lastSpace).trim() : clipped;
}

function parsePrice(value) {
  if (value === null || value === undefined || value === "") return null;
  let normalized = String(value).replace(/[^\d,.-]/g, "");
  if (normalized.includes(",") && normalized.includes(".")) {
    normalized =
      normalized.lastIndexOf(",") > normalized.lastIndexOf(".")
        ? normalized.replace(/\./g, "").replace(",", ".")
        : normalized.replace(/,/g, "");
  } else if (normalized.includes(",")) {
    normalized = normalized.replace(/\./g, "").replace(",", ".");
  }
  const parsed = Number.parseFloat(normalized);
  return Number.isFinite(parsed) && parsed > 0 ? parsed : null;
}

function pickWallapopPrice(item) {
  const dbPrice = parsePrice(item.bestcash_price);
  return dbPrice ? Math.round(dbPrice * 100) / 100 : "";
}

function hasAmazonContent(item) {
  return Boolean(
    cleanText(item.titulo_breve) ||
    cleanText(item.titulo_amazon) ||
    cleanText(item.descripcion) ||
    cleanText(item.caracteristicas) ||
    cleanText(item.descripcion_tecnica)
  );
}

function buildDescription(item) {
  const asin = cleanText(item.asin);
  const refLine = `Ref. BestCash ${asin}`;
  const parts = [];

  let baseDescription = cleanText(item.descripcion || item.descripcion_tecnica || item.caracteristicas || item.titulo_amazon);
  baseDescription = baseDescription
    .replace(/ref\.?\s*best\s*cash:?\s*[A-Z0-9]{10}/gi, "")
    .replace(/ref\.?\s*bestcash:?\s*[A-Z0-9]{10}/gi, "")
    .replace(/\s+/g, " ")
    .trim();
  if (baseDescription && baseDescription !== asin) parts.push(baseDescription);

  const brand = cleanText(item.marca);
  if (brand && !baseDescription.toLowerCase().includes(`marca: ${brand.toLowerCase()}`)) {
    parts.push(`Marca: ${brand}`);
  }

  const reserved = refLine.length + 1;
  const body = truncateAtWord(parts.join(" "), DESCRIPTION_MAX - reserved);
  return body ? `${body} ${refLine}` : refLine;
}

return items.filter(({ json }) => hasAmazonContent(json)).map(({ json }) => {
  const asin = cleanText(json.asin);
  return {
    json: {
      asin,
      titulo_breve: truncateAtWord(json.titulo_breve || json.titulo_amazon || `Producto ${asin}`, TITLE_MAX),
      descripcion: buildDescription(json),
      "UD.": json.num_codes_distintos || "",
      precio: pickWallapopPrice(json),
      peso: json.peso || "",
      peso_amazon: json.peso_amazon || "",
      dimensiones: json.dimensiones || "",
      "REF.": json.codes ? `'${json.codes}` : "",
      marca: json.marca || "",
      titulo_amazon: json.titulo_amazon || "",
      caracteristicas: json.caracteristicas || "",
      descripcion_tecnica: json.descripcion_tecnica || "",
      estado: "NUEVO",
    },
  };
});
