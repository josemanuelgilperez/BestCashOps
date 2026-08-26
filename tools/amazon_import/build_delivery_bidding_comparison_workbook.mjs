import fs from "node:fs/promises";
import path from "node:path";
import { SpreadsheetFile, Workbook } from "@oai/artifact-tool";

const inputJson = process.argv[2];
const outputXlsx = process.argv[3];

if (!inputJson || !outputXlsx) {
  throw new Error("Usage: node build_delivery_bidding_comparison_workbook.mjs <comparison_data.json> <output.xlsx>");
}

const data = JSON.parse(await fs.readFile(inputJson, "utf8"));
const workbook = Workbook.create();

const COLORS = {
  navy: "#1F4E79",
  blue: "#D9EAF7",
  paleBlue: "#EAF4FB",
  gray: "#F3F6F8",
  darkGray: "#44546A",
  border: "#D9E2EC",
  white: "#FFFFFF",
  warn: "#FFF2CC",
  bad: "#FCE4D6",
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

function setTitle(sheet, title, subtitle, span = "A1:L1") {
  sheet.getRange(span).merge();
  sheet.getRange("A1").values = [[title]];
  sheet.getRange("A1").format = {
    fill: COLORS.navy,
    font: { bold: true, color: COLORS.white, size: 16 },
  };
  sheet.getRange("A1").format.rowHeightPx = 34;
  sheet.getRange("A2:L2").merge();
  sheet.getRange("A2").values = [[subtitle]];
  sheet.getRange("A2").format = {
    fill: COLORS.gray,
    font: { color: COLORS.darkGray },
    wrapText: true,
  };
}

function metricCards(sheet, cards, startRow = 3) {
  const headers = cards.map((card) => card.label);
  const values = cards.map((card) => card.value);
  sheet.getRangeByIndexes(startRow, 0, 1, headers.length).values = [headers];
  sheet.getRangeByIndexes(startRow + 1, 0, 1, values.length).values = [values];
  sheet.getRangeByIndexes(startRow, 0, 1, headers.length).format = {
    fill: COLORS.blue,
    font: { bold: true, color: COLORS.navy },
    wrapText: true,
  };
  sheet.getRangeByIndexes(startRow + 1, 0, 1, values.length).format = {
    fill: COLORS.white,
    font: { bold: true, size: 12 },
  };
  cards.forEach((card, index) => {
    const block = sheet.getRangeByIndexes(startRow, index, 2, 1);
    block.format.borders = { preset: "outside", style: "thin", color: COLORS.border };
    block.format.columnWidthPx = card.widthPx ?? 125;
    if (card.format) sheet.getRangeByIndexes(startRow + 1, index, 1, 1).format.numberFormat = card.format;
  });
}

function writeSectionLabel(sheet, cell, label) {
  sheet.getRange(cell).values = [[label]];
  sheet.getRange(cell).format = { font: { bold: true, color: COLORS.navy, size: 13 } };
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

function glRows(rows) {
  return rows.map((row) => ({
    "Bid Category": row.bid_category,
    "Categoría ES": row.gl_es,
    "GL#": row.gl_number,
    GL: row.gl_desc,
    Estado: row.status,
    "Cap. Sellable": row.capacity_sellable,
    "Recib. Sellable": row.sellable_units,
    "Uso Sellable": row.sellable_usage,
    "Cap. Unsellable": row.capacity_unsellable,
    "Recib. Unsellable": row.unsellable_units,
    "Uso Unsellable": row.unsellable_usage,
    "Cap. Total": row.capacity_total,
    "Unidades": row.units,
    "Uso Total": row.total_usage,
    "ASIN distintos": row.distinct_asins,
    "UnitCost medio": row.avg_unitcost,
    "Valor UnitCost": row.unitcost_value,
    "Recovery real": row.actual_recovery,
    "Recovery esperado": row.expected_recovery,
    "Delta Recovery": row.recovery_delta,
    "Unid. <5 EUR": row.cheap_units_lt5,
    "% unid. <5": row.cheap_unit_share,
  }));
}

function rateRows(rows) {
  return rows.map((row) => ({
    "Bid Category": row.bid_category,
    "Categoría ES": row.gl_es,
    "GL#": row.gl_number,
    "Tramo precio": row.price_band,
    Condición: row.condition,
    "Rate bidding": row.bid_rate,
    Líneas: row.rows,
    Unidades: row.units,
    "ASIN distintos": row.distinct_asins,
    "UnitCost medio": row.avg_unitcost,
    "Valor UnitCost": row.unitcost_value,
    "Recovery real": row.actual_recovery,
    "Recovery esperado": row.expected_recovery,
    "Delta Recovery": row.recovery_delta,
    "Rate real": row.actual_recovery_rate,
  }));
}

function makeAccumulator(seed = {}) {
  return {
    lines: 0,
    units: 0,
    unitcostValue: 0,
    actualRecovery: 0,
    expectedRecovery: 0,
    ...seed,
  };
}

function addToAccumulator(acc, row) {
  acc.lines += row.rows;
  acc.units += row.units;
  acc.unitcostValue += row.unitcost_value;
  acc.actualRecovery += row.actual_recovery;
  acc.expectedRecovery += row.expected_recovery;
}

function priceMixRows(rows, byGl = false) {
  const totalUnits = rows.reduce((sum, row) => sum + row.units, 0);
  const totalValue = rows.reduce((sum, row) => sum + row.unitcost_value, 0);
  const grouped = new Map();

  for (const row of rows) {
    const key = byGl ? `${row.gl_number}|${row.price_band}` : row.price_band;
    if (!grouped.has(key)) {
      grouped.set(
        key,
        makeAccumulator({
          bidCategory: row.bid_category,
          glEs: row.gl_es,
          glNumber: row.gl_number,
          priceBand: row.price_band,
          priceBandOrder: row.price_band_order,
        })
      );
    }
    addToAccumulator(grouped.get(key), row);
  }

  return [...grouped.values()]
    .sort((a, b) =>
      byGl
        ? (a.bidCategory || "").localeCompare(b.bidCategory || "") || a.priceBandOrder - b.priceBandOrder
        : a.priceBandOrder - b.priceBandOrder
    )
    .map((row) => ({
      ...(byGl
        ? {
            "Bid Category": row.bidCategory,
            "Categoría ES": row.glEs,
            "GL#": row.glNumber,
          }
        : {}),
      "Tramo precio": row.priceBand,
      Líneas: row.lines,
      Unidades: row.units,
      "% unidades": totalUnits ? row.units / totalUnits : 0,
      "UnitCost medio": row.units ? row.unitcostValue / row.units : 0,
      "Valor UnitCost": row.unitcostValue,
      "% valor": totalValue ? row.unitcostValue / totalValue : 0,
      "Recovery real": row.actualRecovery,
      "Recovery esperado": row.expectedRecovery,
      "Rate real": row.unitcostValue ? row.actualRecovery / row.unitcostValue : 0,
      "Rate bidding ponderado": row.unitcostValue ? row.expectedRecovery / row.unitcostValue : 0,
    }));
}

function strategyRows(rows) {
  return rows.map((row) => ({
    "Bid Category": row.bid_category,
    "Categoría ES": row.gl_es,
    "GL#": row.gl_number,
    Unidades: row.units,
    "UnitCost medio": row.avg_unitcost,
    "Valor UnitCost": row.unitcost_value,
    "Rate <€5": row.rate_lt5_sellable,
    "Rate >€45": row.rate_gt45_sellable,
    "Prima rate": row.rate_premium_gt45_vs_lt5,
    "Unid. <€15": row.units_lt15,
    "% unid. <€15": row.unit_share_lt15,
    "Unid. >=€35": row.units_ge35,
    "% unid. >=€35": row.unit_share_ge35,
    "Valor >=€35": row.value_ge35,
    "% valor >=€35": row.value_share_ge35,
    "Indicador desajuste": row.mismatch_score,
  }));
}

function biddingCurveRows(rows) {
  return rows.map((row) => ({
    "Bid Category": row.bid_category,
    "Categoría ES": row.gl_es,
    "GL#": row.gl_number,
    "Tramo precio": row.price_band,
    "Rate Sellable": row.rate_sellable,
    "Rate Unsellable": row.rate_unsellable,
    Unidades: row.units,
    "% mix recibido": summary.units ? row.units / summary.units : 0,
    "UnitCost medio": row.avg_unitcost,
    "Valor UnitCost": row.unitcost_value,
    "Recovery real": row.actual_recovery,
    "Recovery esperado": row.expected_recovery,
  }));
}

const commonFormats = {
  "Cap. Sellable": intFmt,
  "Recib. Sellable": intFmt,
  "Uso Sellable": pctFmt,
  "Cap. Unsellable": intFmt,
  "Recib. Unsellable": intFmt,
  "Uso Unsellable": pctFmt,
  "Cap. Total": intFmt,
  Unidades: intFmt,
  "Uso Total": pctFmt,
  "ASIN distintos": intFmt,
  "UnitCost medio": moneyFmt,
  "Valor UnitCost": moneyFmt,
  "Recovery real": moneyFmt,
  "Recovery esperado": moneyFmt,
  "Delta Recovery": moneyFmt,
  "Unid. <5 EUR": intFmt,
  "% unid. <5": pctFmt,
  "Rate bidding": pctFmt,
  "Rate bidding ponderado": pctFmt,
  "Rate real": pctFmt,
  "% unidades": pctFmt,
  "% valor": pctFmt,
  "Rate <€5": pctFmt,
  "Rate >€45": pctFmt,
  "Prima rate": pctFmt,
  "Unid. <€15": intFmt,
  "% unid. <€15": pctFmt,
  "Unid. >=€35": intFmt,
  "% unid. >=€35": pctFmt,
  "Valor >=€35": moneyFmt,
  "% valor >=€35": pctFmt,
  "Indicador desajuste": "0.0%",
  "Rate Sellable": pctFmt,
  "Rate Unsellable": pctFmt,
  "% mix recibido": pctFmt,
  Líneas: intFmt,
  Sellable: intFmt,
  Unsellable: intFmt,
};

const summary = data.summary;
const mixPrecio = priceMixRows(data.rate_comparison);
const mixPrecioGl = priceMixRows(data.rate_comparison, true);
const unitsLt15 = mixPrecio
  .filter((row) => row["Tramo precio"] === "<€5" || row["Tramo precio"] === "€5-15")
  .reduce((sum, row) => sum + row.Unidades, 0);
const valueLt15 = mixPrecio
  .filter((row) => row["Tramo precio"] === "<€5" || row["Tramo precio"] === "€5-15")
  .reduce((sum, row) => sum + row["Valor UnitCost"], 0);
const apparelStrategy = data.bid_strategy.find((row) => row.gl_number === 193);

const resumen = ws("Resumen");
setTitle(
  resumen,
  "Comparativa Amazon vs Bidding - Agosto 2026",
  `Bidding: ${summary.bidding_file}. Manifiestos: ${summary.source_dir}. ${summary.comparison_note}`
);
metricCards(resumen, [
  { label: "Unidades recibidas", value: summary.units, format: intFmt },
  { label: "Capacidad bidding", value: summary.capacity_total, format: intFmt },
  { label: "Uso total", value: summary.total_usage, format: pctFmt },
  { label: "Sellable recibidas", value: summary.sellable_units, format: intFmt },
  { label: "Uso sellable", value: summary.sellable_usage, format: pctFmt },
  { label: "Unsellable recibidas", value: summary.unsellable_units, format: intFmt },
  { label: "Uso unsellable", value: summary.unsellable_usage, format: pctFmt },
  { label: "Recovery real", value: summary.actual_recovery, format: moneyFmt },
  { label: "Recovery esperado", value: summary.expected_recovery, format: moneyFmt },
  { label: "Delta", value: summary.recovery_delta, format: moneyFmt },
  { label: "Unid. <15 EUR", value: unitsLt15, format: intFmt },
  { label: "% unid. <15", value: summary.units ? unitsLt15 / summary.units : 0, format: pctFmt },
]);

writeSectionLabel(resumen, "A7", "Lectura de la bidding");
writeTable(
  resumen,
  "A8",
  ["Pregunta", "Respuesta", "Evidencia en la hoja"],
  [
    {
      Pregunta: "¿Apostamos más por producto caro?",
      Respuesta: "Sí, vía rate por tramo",
      "Evidencia en la hoja":
        "Ejemplo Apparel: 3% hasta €35, 5% en €35-45 y 7% en >€45. La apuesta mejora en los tramos caros.",
    },
    {
      Pregunta: "¿Qué está mandando Amazon?",
      Respuesta: "Mucho volumen barato",
      "Evidencia en la hoja":
        `El ${((unitsLt15 / summary.units) * 100).toFixed(1)}% de las unidades están por debajo de €15. En Apparel: ${(
          (apparelStrategy?.unit_share_lt15 ?? 0) * 100
        ).toFixed(1)}% <€15 y ${((apparelStrategy?.unit_share_ge35 ?? 0) * 100).toFixed(1)}% >=€35.`,
    },
    {
      Pregunta: "¿Dónde está el problema?",
      Respuesta: "En el mix recibido, no en el cálculo del recovery",
      "Evidencia en la hoja":
        "La bidding incentiva los caros con mejor rate, pero no obliga a Amazon a entregar volumen en esos tramos.",
    },
  ],
  "LecturaBidding",
  {}
);

writeSectionLabel(resumen, "A14", "Mix recibido por tramo de precio");
writeTable(
  resumen,
  "A15",
  ["Tramo precio", "Líneas", "Unidades", "% unidades", "UnitCost medio", "Valor UnitCost", "% valor", "Recovery real", "Recovery esperado", "Rate real", "Rate bidding ponderado"],
  mixPrecio,
  "MixPrecioResumen",
  commonFormats
);

resumen.getRange("A23").values = [["Unidades <€15"]];
resumen.getRange("B23").values = [[unitsLt15]];
resumen.getRange("C23").values = [[summary.units ? unitsLt15 / summary.units : 0]];
resumen.getRange("D23").values = [["Valor <€15"]];
resumen.getRange("E23").values = [[valueLt15]];
resumen.getRange("F23").values = [[summary.unitcost_value ? valueLt15 / summary.unitcost_value : 0]];
resumen.getRange("A23:F23").format = { fill: COLORS.warn, font: { bold: true } };
resumen.getRange("B23").format.numberFormat = intFmt;
resumen.getRange("C23").format.numberFormat = pctFmt;
resumen.getRange("E23").format.numberFormat = moneyFmt;
resumen.getRange("F23").format.numberFormat = pctFmt;

writeSectionLabel(resumen, "A26", "GL con mayor concentración de producto <5 EUR");
writeTable(
  resumen,
  "A27",
  ["Bid Category", "Categoría ES", "GL#", "Unidades", "UnitCost medio", "Unid. <5 EUR", "% unid. <5", "Valor UnitCost", "Recovery real"],
  glRows([...data.gl_comparison].filter((row) => row.units >= 500).sort((a, b) => b.cheap_unit_share - a.cheap_unit_share).slice(0, 12)),
  "CheapGLResumen",
  commonFormats
);
resumen.freezePanes.freezeRows(5);

const comparacion = ws("Comparacion_GL");
setTitle(comparacion, "Comparación por GL", "Recibido por GL frente a capacidades mensuales de la bidding sheet.", "A1:V1");
writeTable(
  comparacion,
  "A4",
  [
    "Bid Category",
    "Categoría ES",
    "GL#",
    "GL",
    "Estado",
    "Cap. Sellable",
    "Recib. Sellable",
    "Uso Sellable",
    "Cap. Unsellable",
    "Recib. Unsellable",
    "Uso Unsellable",
    "Cap. Total",
    "Unidades",
    "Uso Total",
    "ASIN distintos",
    "UnitCost medio",
    "Valor UnitCost",
    "Recovery real",
    "Recovery esperado",
    "Delta Recovery",
    "Unid. <5 EUR",
    "% unid. <5",
  ],
  glRows(data.gl_comparison),
  "ComparacionGL",
  commonFormats
);
comparacion.freezePanes.freezeRows(4);

const mix = ws("Mix_precios");
setTitle(
  mix,
  "Mix de precio recibido",
  "Distribución real de lo recibido por los tramos de precio que aparecen en la bidding sheet. La bidding no contiene volumen objetivo por tramo.",
  "A1:K1"
);
writeTable(
  mix,
  "A4",
  ["Tramo precio", "Líneas", "Unidades", "% unidades", "UnitCost medio", "Valor UnitCost", "% valor", "Recovery real", "Recovery esperado", "Rate real", "Rate bidding ponderado"],
  mixPrecio,
  "MixPrecioTotal",
  commonFormats
);
writeSectionLabel(mix, "A14", "Mix de precio por GL");
writeTable(
  mix,
  "A15",
  ["Bid Category", "Categoría ES", "GL#", "Tramo precio", "Líneas", "Unidades", "% unidades", "UnitCost medio", "Valor UnitCost", "% valor", "Recovery real", "Recovery esperado", "Rate real", "Rate bidding ponderado"],
  mixPrecioGl,
  "MixPrecioGL",
  commonFormats
);
mix.freezePanes.freezeRows(4);

const apuesta = ws("Apuesta_vs_recibido");
setTitle(
  apuesta,
  "Apuesta por precio vs recibido",
  "Compara la curva de rates ofertados con el mix real recibido. Si la prima de rate está en tramos caros pero el volumen llega barato, hay desajuste de mix.",
  "A1:P1"
);
writeTable(
  apuesta,
  "A4",
  [
    "Bid Category",
    "Categoría ES",
    "GL#",
    "Unidades",
    "UnitCost medio",
    "Valor UnitCost",
    "Rate <€5",
    "Rate >€45",
    "Prima rate",
    "Unid. <€15",
    "% unid. <€15",
    "Unid. >=€35",
    "% unid. >=€35",
    "Valor >=€35",
    "% valor >=€35",
    "Indicador desajuste",
  ],
  strategyRows(data.bid_strategy.filter((row) => row.units > 0)),
  "ApuestaGL",
  commonFormats
);
writeSectionLabel(apuesta, "A46", "Curva completa de bidding y unidades recibidas por tramo");
writeTable(
  apuesta,
  "A47",
  [
    "Bid Category",
    "Categoría ES",
    "GL#",
    "Tramo precio",
    "Rate Sellable",
    "Rate Unsellable",
    "Unidades",
    "% mix recibido",
    "UnitCost medio",
    "Valor UnitCost",
    "Recovery real",
    "Recovery esperado",
  ],
  biddingCurveRows(data.bidding_rates),
  "CurvaBiddingRecibido",
  commonFormats
);
apuesta.freezePanes.freezeRows(4);

const rates = ws("Rates_tramos");
setTitle(rates, "Recovery por tramo de precio", "Comparación entre rate de bidding y recovery real por GL, tramo de UnitCost y condición.", "A1:O1");
writeTable(
  rates,
  "A4",
  [
    "Bid Category",
    "Categoría ES",
    "GL#",
    "Tramo precio",
    "Condición",
    "Rate bidding",
    "Líneas",
    "Unidades",
    "ASIN distintos",
    "UnitCost medio",
    "Valor UnitCost",
    "Recovery real",
    "Recovery esperado",
    "Delta Recovery",
    "Rate real",
  ],
  rateRows(data.rate_comparison),
  "RatesTramos",
  commonFormats
);
rates.freezePanes.freezeRows(4);

const alertas = ws("Alertas");
setTitle(alertas, "Alertas", "Categorías con exceso por condición, GL sin bidding o filas sin rate aplicable.", "A1:L1");
const alertRows = glRows(
  data.gl_comparison.filter(
    (row) => !row.in_bid || row.sellable_usage > 1 || row.unsellable_usage > 1 || row.total_usage > 1
  )
);
writeTable(
  alertas,
  "A4",
  [
    "Bid Category",
    "Categoría ES",
    "GL#",
    "Estado",
    "Cap. Sellable",
    "Recib. Sellable",
    "Uso Sellable",
    "Cap. Unsellable",
    "Recib. Unsellable",
    "Uso Unsellable",
    "Cap. Total",
    "Unidades",
  ],
  alertRows,
  "AlertasCapacidad",
  commonFormats
);
alertas.getRange("A12").values = [["Unidades sin rate bidding"]];
alertas.getRange("B12").values = [[summary.missing_rate_units]];
alertas.getRange("A12:B12").format = { fill: summary.missing_rate_units ? COLORS.bad : COLORS.paleBlue, font: { bold: true } };
alertas.getRange("B12").format.numberFormat = intFmt;

for (const sheet of [resumen, comparacion, mix, apuesta, rates, alertas]) {
  const used = sheet.getUsedRange();
  used.format.autofitColumns();
  used.format.autofitRows();
}

const outDir = path.dirname(outputXlsx);
await fs.mkdir(outDir, { recursive: true });
const output = await SpreadsheetFile.exportXlsx(workbook);
await output.save(outputXlsx);

for (const [sheetName, range] of [
  ["Resumen", "A1:L40"],
  ["Comparacion_GL", "A1:V35"],
  ["Mix_precios", "A1:N35"],
  ["Apuesta_vs_recibido", "A1:P35"],
  ["Rates_tramos", "A1:O35"],
  ["Alertas", "A1:L15"],
]) {
  const preview = await workbook.render({ sheetName, range, scale: 1, format: "png" });
  const bytes = new Uint8Array(await preview.arrayBuffer());
  await fs.writeFile(path.join(outDir, `comparativa_${sheetName}.png`), bytes);
}

console.log(`Saved ${outputXlsx}`);
