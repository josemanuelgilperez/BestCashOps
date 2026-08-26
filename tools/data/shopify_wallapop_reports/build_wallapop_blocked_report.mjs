import fs from "node:fs/promises";
import path from "node:path";
import { SpreadsheetFile, Workbook } from "@oai/artifact-tool";

const reportDir = process.argv[2];
const rows = JSON.parse(await fs.readFile(path.join(reportDir, "wallapop_blocked_candidates_with_sheet.json"), "utf8"));
const summary = JSON.parse(await fs.readFile(path.join(reportDir, "wallapop_blocked_summary.json"), "utf8"));

const workbook = Workbook.create();
const summarySheet = workbook.worksheets.add("Resumen");
const detailSheet = workbook.worksheets.add("ASIN bloqueados");
summarySheet.showGridLines = false;
detailSheet.showGridLines = false;

const generatedAt = new Date().toISOString().slice(0, 19).replace("T", " ");
const summaryRows = [
  ["Informe Wallapop bloqueados Shopify", ""],
  ["Generado", generatedAt],
  ["ASIN bloqueados", summary.blocked_rows],
  ["ASIN del Google Sheet", summary.sheet_rows_by_asin],
  ["ASIN bloqueados encontrados en Sheet", summary.rows_with_sheet_match],
  ["Bloqueados por precio", summary.missing_price_rows],
  ["Bloqueados por precio con precio en Sheet", summary.missing_price_with_sheet_price],
  ["Bloqueados solo por precio y publicables si se acepta Sheet", summary.recommended_actions.usar_precio_sheet_y_publicar || 0],
  ["Pendientes por peso", summary.issue_counts.missing_weight],
  ["Pendientes sin imagen", summary.issue_counts.missing_image],
  ["Pendientes con imagen no S3", summary.issue_counts.missing_s3_image],
  ["Pendientes por precio multiple", summary.issue_counts.multiple_prices],
  ["", ""],
  ["Accion recomendada", "ASIN"],
  ...Object.entries(summary.recommended_actions).sort((a, b) => b[1] - a[1]),
];

summarySheet.getRangeByIndexes(0, 0, summaryRows.length, 2).values = summaryRows;
summarySheet.getRange("A1:B1").format.fill.color = "#17324D";
summarySheet.getRange("A1:B1").format.font.color = "#FFFFFF";
summarySheet.getRange("A1:B1").format.font.bold = true;
summarySheet.getRange("A14:B14").format.fill.color = "#EAF2F8";
summarySheet.getRange("A14:B14").format.font.bold = true;
summarySheet.getRange("A1:B30").format.borders = { preset: "inside", style: "thin", color: "#D9E2EC" };
summarySheet.getRange("A1:B30").format.autofitColumns();
summarySheet.freezePanes.freezeRows(1);

const detailHeaders = [
  "asin",
  "stock_wallapop",
  "issues",
  "recommended_action",
  "db_price",
  "db_min_price",
  "db_max_price",
  "sheet_price",
  "sheet_price_can_fix",
  "sheet_status",
  "sheet_stock",
  "title_db",
  "sheet_title",
  "weight_grams",
  "image_count",
  "has_s3_image",
  "missing_price",
  "multiple_prices",
  "missing_weight",
  "missing_image",
  "missing_s3_image",
  "vendor",
  "product_type",
  "main_image",
  "first_image",
  "sheet_date",
  "sheet_items",
];

const detailValues = [
  detailHeaders,
  ...rows.map((row) => detailHeaders.map((header) => {
    const value = row[header];
    if (["stock_wallapop", "sheet_stock", "image_count"].includes(header) && value !== "") {
      const parsed = Number(value);
      return Number.isFinite(parsed) ? parsed : value;
    }
    if (["db_price", "db_min_price", "db_max_price", "sheet_price", "weight_grams"].includes(header) && value !== "") {
      const parsed = Number(value);
      return Number.isFinite(parsed) ? parsed : value;
    }
    return value ?? "";
  })),
];

detailSheet.getRangeByIndexes(0, 0, detailValues.length, detailHeaders.length).values = detailValues;
const headerRange = detailSheet.getRangeByIndexes(0, 0, 1, detailHeaders.length);
headerRange.format.fill.color = "#17324D";
headerRange.format.font.color = "#FFFFFF";
headerRange.format.font.bold = true;
headerRange.format.wrapText = true;
detailSheet.freezePanes.freezeRows(1);
detailSheet.freezePanes.freezeColumns(1);
detailSheet.getRangeByIndexes(0, 0, detailValues.length, detailHeaders.length).format.borders = {
  insideHorizontal: { style: "thin", color: "#E6ECF1" },
  insideVertical: { style: "thin", color: "#E6ECF1" },
};
detailSheet.getRange("B:B").setNumberFormat("#,##0");
detailSheet.getRange("E:H").setNumberFormat("0.00");
detailSheet.getRange("N:O").setNumberFormat("#,##0");
detailSheet.getRange("A:AA").format.autofitColumns();
detailSheet.getRange("C:D").format.columnWidth = 34;
detailSheet.getRange("L:M").format.columnWidth = 48;
detailSheet.getRange("X:Y").format.columnWidth = 60;
detailSheet.getRange("A:AA").format.wrapText = false;

const outputPath = path.join(reportDir, "wallapop_bloqueados_shopify.xlsx");
const output = await SpreadsheetFile.exportXlsx(workbook);
await output.save(outputPath);

const inspect = await workbook.inspect({
  kind: "sheet,table",
  tableMaxRows: 5,
  tableMaxCols: 8,
  maxChars: 4000,
});
console.log(inspect.ndjson);

const errors = await workbook.inspect({
  kind: "match",
  searchTerm: "#REF!|#DIV/0!|#VALUE!|#NAME\\?|#N/A",
  options: { useRegex: true, maxResults: 50 },
  maxChars: 1000,
});
console.log(errors.ndjson);

const preview = await workbook.render({ sheetName: "Resumen", range: "A1:B25", scale: 1, format: "png" });
await fs.writeFile(path.join(reportDir, "wallapop_bloqueados_shopify_resumen.png"), new Uint8Array(await preview.arrayBuffer()));

console.log(outputPath);
