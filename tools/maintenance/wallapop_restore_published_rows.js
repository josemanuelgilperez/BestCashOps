function cleanText(value) {
  return String(value || "").replace(/\s+/g, " ").trim();
}

const publishedAsins = new Set(
  $("PUBLICAR | Build Publish Rows")
    .all()
    .map((item) => cleanText(item.json.asin).toUpperCase())
    .filter(Boolean)
);

return $("TPV | Prepare CSV Rows")
  .all()
  .filter((item) => publishedAsins.has(cleanText(item.json.asin).toUpperCase()))
  .map((item) => ({
    json: {
      item: item.json.item,
      asin: item.json.asin,
      precio: item.json.precio,
      tienda: item.json.tienda,
      ok_online: item.json.ok_online,
    },
  }));
