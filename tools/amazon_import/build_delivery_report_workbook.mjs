import fs from "node:fs/promises";
import path from "node:path";
import { SpreadsheetFile, Workbook } from "@oai/artifact-tool";

const inputJson = process.argv[2];
const outputXlsx = process.argv[3];

if (!inputJson || !outputXlsx) {
  throw new Error("Usage: node build_delivery_report_workbook.mjs <report_data.json> <output.xlsx>");
}

const data = JSON.parse(await fs.readFile(inputJson, "utf8"));
const workbook = Workbook.create();

const COLORS = {
  navy: "#1F4E79",
  teal: "#0F766E",
  blue: "#D9EAF7",
  gray: "#F3F6F8",
  darkGray: "#44546A",
  border: "#D9E2EC",
  white: "#FFFFFF",
};

const moneyFmt = "€#,##0.00";
const intFmt = "#,##0";
const pctFmt = "0.0%";

function ws(name) {
  const sheet = workbook.worksheets.add(name);
  sheet.showGridLines = false;
  return sheet;
}

function cellToIndexes(cell) {
  const match = /^([A-Z]+)(\d+)$/.exec(cell);
  if (!match) throw new Error(`Invalid cell: ${cell}`);
  const [, letters, rowText] = match;
  let col = 0;
  for (const ch of letters) col = col * 26 + ch.charCodeAt(0) - 64;
  return { row: Number(rowText) - 1, col: col - 1 };
}

function setTitle(sheet, title, subtitle, span = "A1:K1") {
  sheet.getRange(span).merge();
  sheet.getRange("A1").values = [[title]];
  sheet.getRange("A1").format = {
    fill: COLORS.navy,
    font: { bold: true, color: COLORS.white, size: 16 },
  };
  sheet.getRange("A1").format.rowHeightPx = 34;
  sheet.getRange("A2:K2").merge();
  sheet.getRange("A2").values = [[subtitle]];
  sheet.getRange("A2").format = {
    fill: COLORS.gray,
    font: { color: COLORS.darkGray },
    wrapText: true,
  };
}

function metricCards(sheet, cards) {
  const headers = cards.map((card) => card.label);
  const values = cards.map((card) => card.value);
  sheet.getRangeByIndexes(3, 0, 1, headers.length).values = [headers];
  sheet.getRangeByIndexes(4, 0, 1, values.length).values = [values];
  sheet.getRangeByIndexes(3, 0, 1, headers.length).format = {
    fill: COLORS.blue,
    font: { bold: true, color: COLORS.navy },
    wrapText: true,
  };
  sheet.getRangeByIndexes(4, 0, 1, values.length).format = {
    fill: COLORS.white,
    font: { bold: true, size: 13 },
  };
  cards.forEach((card, index) => {
    const block = sheet.getRangeByIndexes(3, index, 2, 1);
    block.format.borders = { preset: "outside", style: "thin", color: COLORS.border };
    block.format.columnWidthPx = card.widthPx ?? 120;
    if (card.format) sheet.getRangeByIndexes(4, index, 1, 1).format.numberFormat = card.format;
  });
}

function writeTable(sheet, startCell, headers, rows, tableName, formats = {}) {
  const matrix = [headers, ...rows.map((row) => headers.map((header) => row[header] ?? null))];
  const start = cellToIndexes(startCell);
  const range = sheet.getRangeByIndexes(start.row, start.col, matrix.length, headers.length);
  range.values = matrix;
  const headerRange = sheet.getRangeByIndexes(start.row, start.col, 1, headers.length);
  headerRange.format = {
    fill: COLORS.navy,
    font: { bold: true, color: COLORS.white },
    wrapText: true,
  };
  headerRange.format.rowHeightPx = 34;
  range.format.borders = { preset: "outside", style: "thin", color: COLORS.border };
  if (rows.length > 0) {
    const table = sheet.tables.add(range.address, true, tableName);
    table.style = "TableStyleMedium2";
    table.showFilterButton = true;
  }
  Object.entries(formats).forEach(([header, numberFormat]) => {
    const idx = headers.indexOf(header);
    if (idx >= 0 && rows.length > 0) {
      sheet.getRangeByIndexes(start.row + 1, start.col + idx, rows.length, 1).format.numberFormat = numberFormat;
    }
  });
  range.format.autofitColumns();
  range.format.autofitRows();
  return range;
}

function weeklyRows() {
  return data.weekly_summary.map((row) => ({
    "Fecha manifiesto": row.manifest_date,
    Archivos: row.files,
    Líneas: row.rows,
    Unidades: row.units,
    "ASIN distintos": row.distinct_asins,
    "Categorías GL": row.gl_categories,
    "Coste medio": row.avg_unitcost_weighted,
    "Recovery medio": row.avg_unitrecovery_weighted,
    "Valor UnitCost": row.total_unitcost_value,
    "Valor Recovery": row.total_recovery_value,
  }));
}

function glRows(rows) {
  return rows.map((row) => ({
    "Categoría ES": row.gl_es,
    GL: row.gl_desc,
    Líneas: row.rows,
    Unidades: row.units,
    "ASIN distintos": row.distinct_asins,
    "Coste medio": row.avg_unitcost_weighted,
    "Recovery medio": row.avg_unitrecovery_weighted,
    "Amazon medio": row.avg_amazonprice_weighted,
    "Valor UnitCost": row.total_unitcost_value,
    "Valor Recovery": row.total_recovery_value,
    "Recovery / UnitCost": row.recovery_vs_unitcost_pct,
    "% unidades": row.unit_share,
  }));
}

function costRows(rows, includeWeek = false) {
  return rows.map((row) => ({
    ...(includeWeek ? { "Fecha manifiesto": row.manifest_date } : {}),
    "Tramo coste": row.cost_bucket,
    Líneas: row.rows,
    Unidades: row.units,
    "ASIN distintos": row.distinct_asins,
    "Coste medio": row.avg_unitcost_weighted,
    "Recovery medio": row.avg_unitrecovery_weighted,
    "Valor UnitCost": row.total_unitcost_value,
    "Valor Recovery": row.total_recovery_value,
    "% unidades": row.unit_share,
    "% valor": row.unitcost_value_share,
  }));
}

function topAsinRows(rows) {
  return rows.map((row) => ({
    ASIN: row.asin,
    "Categoría ES": row.gl_es,
    GL: row.gl_desc,
    Descripción: row.item_desc,
    Líneas: row.rows,
    Unidades: row.units,
    "Coste medio": row.avg_unitcost_weighted,
    "Recovery medio": row.avg_unitrecovery_weighted,
    "Valor UnitCost": row.total_unitcost_value,
    "Valor Recovery": row.total_recovery_value,
    "Recovery / UnitCost": row.recovery_vs_unitcost_pct,
    "Ficheros origen": row.source_files,
  }));
}

const tableHeaders = [
  "Categoría ES",
  "GL",
  "Líneas",
  "Unidades",
  "ASIN distintos",
  "Coste medio",
  "Recovery medio",
  "Amazon medio",
  "Valor UnitCost",
  "Valor Recovery",
  "Recovery / UnitCost",
  "% unidades",
];

const tableFormats = {
  Líneas: intFmt,
  Unidades: intFmt,
  "ASIN distintos": intFmt,
  "Coste medio": moneyFmt,
  "Recovery medio": moneyFmt,
  "Amazon medio": moneyFmt,
  "Valor UnitCost": moneyFmt,
  "Valor Recovery": moneyFmt,
  "Recovery / UnitCost": pctFmt,
  "% unidades": pctFmt,
};

const costHeaders = [
  "Tramo coste",
  "Líneas",
  "Unidades",
  "ASIN distintos",
  "Coste medio",
  "Recovery medio",
  "Valor UnitCost",
  "Valor Recovery",
  "% unidades",
  "% valor",
];

const costFormats = {
  Líneas: intFmt,
  Unidades: intFmt,
  "ASIN distintos": intFmt,
  "Coste medio": moneyFmt,
  "Recovery medio": moneyFmt,
  "Valor UnitCost": moneyFmt,
  "Valor Recovery": moneyFmt,
  "% unidades": pctFmt,
  "% valor": pctFmt,
};

const summary = data.summary;
const resumen = ws("Resumen");
setTitle(
  resumen,
  "Informe entregas Amazon - Agosto 2026",
  `Fuente: ${summary.source_dir}. Informe por fecha de manifiesto, agrupando DE/ES/FR/IT y usando siempre GLDesc como categoría uniforme.`
);
metricCards(resumen, [
  { label: "Archivos", value: summary.files, format: intFmt },
  { label: "Líneas", value: summary.rows, format: intFmt },
  { label: "Unidades", value: summary.units, format: intFmt },
  { label: "ASIN distintos", value: summary.distinct_asins, format: intFmt },
  { label: "Categorías GL", value: data.gl.length, format: intFmt },
  { label: "Valor UnitCost", value: summary.total_unitcost_value, format: moneyFmt, widthPx: 135 },
  { label: "Valor Recovery", value: summary.total_recovery_value, format: moneyFmt, widthPx: 135 },
]);

resumen.getRange("A7").values = [["Resumen semanal"]];
resumen.getRange("A7").format = { font: { bold: true, color: COLORS.navy, size: 13 } };
writeTable(
  resumen,
  "A8",
  [
    "Fecha manifiesto",
    "Archivos",
    "Líneas",
    "Unidades",
    "ASIN distintos",
    "Categorías GL",
    "Coste medio",
    "Recovery medio",
    "Valor UnitCost",
    "Valor Recovery",
  ],
  weeklyRows(),
  "ResumenSemanalTabla",
  {
    Archivos: intFmt,
    Líneas: intFmt,
    Unidades: intFmt,
    "ASIN distintos": intFmt,
    "Categorías GL": intFmt,
    "Coste medio": moneyFmt,
    "Recovery medio": moneyFmt,
    "Valor UnitCost": moneyFmt,
    "Valor Recovery": moneyFmt,
  }
);

resumen.getRange("A14").values = [["Tramos de coste del periodo"]];
resumen.getRange("A14").format = { font: { bold: true, color: COLORS.navy, size: 13 } };
writeTable(resumen, "A15", costHeaders, costRows(data.cost_buckets), "TramosCostePeriodoResumen", costFormats);

resumen.getRange("A24").values = [["Categorías GL del periodo"]];
resumen.getRange("A24").format = { font: { bold: true, color: COLORS.navy, size: 13 } };
writeTable(resumen, "A25", tableHeaders, glRows(data.gl), "CategoriasGLPeriodoTabla", tableFormats);
resumen.getRange("A:A").format.columnWidthPx = 180;
resumen.getRange("B:B").format.columnWidthPx = 210;
resumen.getRange("F:J").format.columnWidthPx = 126;
resumen.freezePanes.freezeRows(7);

for (const week of data.weekly_summary) {
  const compactDate = week.manifest_date.replaceAll("-", "");
  const sheet = ws(`Semana_${compactDate}`);
  setTitle(
    sheet,
    `Semana manifiesto ${week.manifest_date}`,
    "Agrupado por GLDesc de los 4 manifiestos DE/ES/FR/IT. Ordenado por la traducción española de la categoría."
  );
  metricCards(sheet, [
    { label: "Archivos", value: week.files, format: intFmt },
    { label: "Líneas", value: week.rows, format: intFmt },
    { label: "Unidades", value: week.units, format: intFmt },
    { label: "ASIN distintos", value: week.distinct_asins, format: intFmt },
    { label: "Categorías GL", value: week.gl_categories, format: intFmt },
    { label: "Valor UnitCost", value: week.total_unitcost_value, format: moneyFmt, widthPx: 135 },
    { label: "Valor Recovery", value: week.total_recovery_value, format: moneyFmt, widthPx: 135 },
  ]);
  sheet.getRange("A7").values = [["Categorías GL"]];
  sheet.getRange("A7").format = { font: { bold: true, color: COLORS.navy, size: 13 } };
  const weekGlRows = data.gl_week.filter((row) => row.manifest_date === week.manifest_date);
  writeTable(sheet, "A8", tableHeaders, glRows(weekGlRows), `Semana${compactDate}GLTabla`, tableFormats);
  sheet.getRange("A:A").format.columnWidthPx = 180;
  sheet.getRange("B:B").format.columnWidthPx = 210;
  sheet.getRange("F:J").format.columnWidthPx = 126;
  sheet.freezePanes.freezeRows(7);
}

const tramos = ws("Tramos_coste");
setTitle(
  tramos,
  "Tramos de coste Amazon - Agosto 2026",
  "Distribución de unidades por tramos de UnitCost, ponderada por Units. Sirve para detectar concentración de producto barato."
);
metricCards(tramos, [
  { label: "Unidades <= 5 EUR", value: data.cost_buckets.filter((row) => row.bucket_order <= 2).reduce((sum, row) => sum + row.units, 0), format: intFmt, widthPx: 135 },
  { label: "% unidades <= 5 EUR", value: data.cost_buckets.filter((row) => row.bucket_order <= 2).reduce((sum, row) => sum + row.unit_share, 0), format: pctFmt, widthPx: 145 },
  { label: "Valor <= 5 EUR", value: data.cost_buckets.filter((row) => row.bucket_order <= 2).reduce((sum, row) => sum + row.total_unitcost_value, 0), format: moneyFmt, widthPx: 135 },
  { label: "% valor <= 5 EUR", value: data.cost_buckets.filter((row) => row.bucket_order <= 2).reduce((sum, row) => sum + row.unitcost_value_share, 0), format: pctFmt, widthPx: 135 },
]);
tramos.getRange("A7").values = [["Periodo completo"]];
tramos.getRange("A7").format = { font: { bold: true, color: COLORS.navy, size: 13 } };
writeTable(tramos, "A8", costHeaders, costRows(data.cost_buckets), "TramosCostePeriodoTabla", costFormats);
tramos.getRange("A18").values = [["Por fecha de manifiesto"]];
tramos.getRange("A18").format = { font: { bold: true, color: COLORS.navy, size: 13 } };
writeTable(
  tramos,
  "A19",
  ["Fecha manifiesto", ...costHeaders],
  costRows(data.cost_buckets_week, true),
  "TramosCosteSemanaTabla",
  {
    "Fecha manifiesto": "yyyy-mm-dd",
    ...costFormats,
  }
);
tramos.getRange("A:A").format.columnWidthPx = 125;
tramos.getRange("B:B").format.columnWidthPx = 82;
tramos.getRange("C:C").format.columnWidthPx = 95;
tramos.getRange("D:D").format.columnWidthPx = 115;
tramos.getRange("E:E").format.columnWidthPx = 110;
tramos.getRange("F:F").format.columnWidthPx = 125;
tramos.getRange("G:H").format.columnWidthPx = 130;
tramos.getRange("I:J").format.columnWidthPx = 120;
tramos.freezePanes.freezeRows(7);

const topAsin = ws("Top_ASIN");
setTitle(
  topAsin,
  "Top ASIN - Agosto 2026",
  "Top 500 ASIN agregados por ASIN real, ordenados por unidades descendentes. Incluye GL y traducción española."
);
writeTable(
  topAsin,
  "A4",
  [
    "ASIN",
    "Categoría ES",
    "GL",
    "Descripción",
    "Líneas",
    "Unidades",
    "Coste medio",
    "Recovery medio",
    "Valor UnitCost",
    "Valor Recovery",
    "Recovery / UnitCost",
    "Ficheros origen",
  ],
  topAsinRows(data.top_asins),
  "TopAsinTabla",
  {
    Líneas: intFmt,
    Unidades: intFmt,
    "Coste medio": moneyFmt,
    "Recovery medio": moneyFmt,
    "Valor UnitCost": moneyFmt,
    "Valor Recovery": moneyFmt,
    "Recovery / UnitCost": pctFmt,
    "Ficheros origen": intFmt,
  }
);
topAsin.getRange("A:A").format.columnWidthPx = 110;
topAsin.getRange("B:B").format.columnWidthPx = 150;
topAsin.getRange("C:C").format.columnWidthPx = 210;
topAsin.getRange("D:D").format.columnWidthPx = 420;
topAsin.getRange("G:J").format.columnWidthPx = 118;
topAsin.freezePanes.freezeRows(4);

await fs.mkdir(path.dirname(outputXlsx), { recursive: true });
const output = await SpreadsheetFile.exportXlsx(workbook);
await output.save(outputXlsx);

const inspectSummary = await workbook.inspect({
  kind: "sheet,table",
  maxChars: 6000,
  tableMaxRows: 5,
  tableMaxCols: 8,
});
console.log(inspectSummary.ndjson);

const errors = await workbook.inspect({
  kind: "match",
  searchTerm: "#REF!|#DIV/0!|#VALUE!|#NAME\\?|#N/A",
  options: { useRegex: true, maxResults: 300 },
  summary: "final formula error scan",
});
console.log(errors.ndjson);

const previewRanges = [
  ["Resumen", "A1:L50"],
  ["Semana_20260802", "A1:L40"],
  ["Semana_20260809", "A1:L40"],
  ["Semana_20260816", "A1:L40"],
  ["Tramos_coste", "A1:K35"],
  ["Top_ASIN", "A1:L35"],
];

for (const [sheetName, range] of previewRanges) {
  const preview = await workbook.render({ sheetName, range, scale: 1, format: "png" });
  await fs.writeFile(
    path.join(path.dirname(outputXlsx), `${sheetName}.png`),
    new Uint8Array(await preview.arrayBuffer())
  );
}

console.log(`Saved ${outputXlsx}`);
