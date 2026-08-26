#!/usr/bin/env node
'use strict';

const crypto = require('crypto');
const fs = require('fs');
const path = require('path');

const DEFAULT_NEW_DAYS = 14;
const DEFAULT_INITIAL_NEW_LIMIT = 95;
const DEFAULT_NEW_CODES_FILE = path.resolve(__dirname, '..', 'data', 'new_published_pallets.txt');
const STATE_RELATIVE_PATH = path.join('assets', 'publication-metadata.json');
const CSS_RELATIVE_PATH = path.join('assets', 'new-lots.css');
const JS_RELATIVE_PATH = path.join('assets', 'new-lots.js');

function parseArgs(argv) {
  const args = {};

  for (let index = 2; index < argv.length; index += 1) {
    const token = argv[index];
    if (!token.startsWith('--')) continue;

    const eqIndex = token.indexOf('=');
    if (eqIndex !== -1) {
      args[token.slice(2, eqIndex)] = token.slice(eqIndex + 1);
      continue;
    }

    const key = token.slice(2);
    const next = argv[index + 1];
    if (next && !next.startsWith('--')) {
      args[key] = next;
      index += 1;
    } else {
      args[key] = '1';
    }
  }

  return args;
}

function bool(value, fallback = false) {
  if (value == null || value === '') return fallback;
  return ['1', 'true', 'yes', 'on', 'si'].includes(String(value).toLowerCase());
}

function buildConfig() {
  const args = parseArgs(process.argv);
  const siteDir = args.site || args.source || process.env.VDL_SITE_DIR;

  if (!siteDir) {
    throw new Error('Missing site directory. Use --site /ruta/a/ventadelotes-generado or VDL_SITE_DIR.');
  }

  return {
    siteDir: path.resolve(siteDir),
    newCodesFile: path.resolve(args['new-codes-file'] || process.env.VDL_NEW_CODES_FILE || DEFAULT_NEW_CODES_FILE),
    newDays: Number(args['new-days'] || process.env.VDL_NEW_DAYS || DEFAULT_NEW_DAYS),
    initialNewLimit: Number(
      args['initial-new-limit'] ||
      process.env.VDL_INITIAL_NEW_LIMIT ||
      DEFAULT_INITIAL_NEW_LIMIT,
    ),
    minimumNewCount: Number(
      args['minimum-new-count'] ||
      process.env.VDL_MINIMUM_NEW_COUNT ||
      args['initial-new-limit'] ||
      process.env.VDL_INITIAL_NEW_LIMIT ||
      DEFAULT_INITIAL_NEW_LIMIT,
    ),
    markAllCurrentNew: bool(args['mark-all-current-new'] || process.env.VDL_MARK_ALL_CURRENT_NEW, false),
  };
}

function ensureDir(dir) {
  fs.mkdirSync(dir, { recursive: true });
}

function readText(filePath) {
  return fs.readFileSync(filePath, 'utf8');
}

function writeText(filePath, content) {
  fs.writeFileSync(filePath, content, 'utf8');
}

function escapeHtml(value) {
  return String(value ?? '')
    .replaceAll('&', '&amp;')
    .replaceAll('<', '&lt;')
    .replaceAll('>', '&gt;')
    .replaceAll('"', '&quot;')
    .replaceAll("'", '&#039;');
}

function stripTags(html) {
  return String(html || '')
    .replace(/<script[\s\S]*?<\/script>/gi, '')
    .replace(/<style[\s\S]*?<\/style>/gi, '')
    .replace(/<[^>]+>/g, ' ')
    .replace(/&nbsp;/gi, ' ')
    .replace(/&euro;/gi, '€')
    .replace(/&amp;/gi, '&')
    .replace(/\s+/g, ' ')
    .trim();
}

function walkFiles(dir, predicate) {
  const files = [];
  if (!fs.existsSync(dir)) return files;

  for (const entry of fs.readdirSync(dir)) {
    const filePath = path.join(dir, entry);
    const stats = fs.statSync(filePath);
    if (stats.isDirectory()) {
      files.push(...walkFiles(filePath, predicate));
    } else if (!predicate || predicate(filePath)) {
      files.push(filePath);
    }
  }

  return files;
}

function palletCodeFromFile(filePath) {
  const base = path.basename(filePath, '.html');
  return /^[A-Z]{2}\d{4}$/i.test(base) ? base.toUpperCase() : null;
}

function firstMatch(html, pattern) {
  return html.match(pattern)?.[1]?.trim() || '';
}

function getCategoryFromLotPage(html) {
  const linkText = firstMatch(html, /<a[^>]+href=["'][^"']*categorias\/([^"']+)\.html["'][^>]*>[\s\S]*?<\/a>/i);
  if (linkText) return linkText;
  return '';
}

function extractPalletInfo(filePath, siteDir) {
  const code = palletCodeFromFile(filePath);
  if (!code) return null;

  const html = readText(filePath);
  const unitsMatch = html.match(/<div[^>]*class=["'][^"']*title-meta[^"']*["'][^>]*>\s*([0-9.]+)\s+unidades/i);
  const units = unitsMatch ? Number(unitsMatch[1].replace(/\./g, '')) : 0;
  const title = stripTags(firstMatch(html, /<title>([\s\S]*?)<\/title>/i));
  const titleMain = stripTags(firstMatch(html, /<span[^>]+data-pallet-title[^>]*>([\s\S]*?)<\/span>/i));
  const name = stripTags(firstMatch(html, /<span[^>]+data-pallet-name[^>]*>([\s\S]*?)<\/span>/i));
  const categorySlug = getCategoryFromLotPage(html);
  const signature = crypto
    .createHash('sha256')
    .update(html.replace(/\s+/g, ' ').trim())
    .digest('hex');

  return {
    code,
    file: path.relative(siteDir, filePath).replaceAll(path.sep, '/'),
    href: `lotes/${code}.html`,
    title: titleMain || title || code,
    name: name || title.replace(new RegExp(`^${code}\\s*[–-]\\s*`, 'i'), '') || code,
    categorySlug,
    units,
    signature,
  };
}

function readState(siteDir) {
  const statePath = path.join(siteDir, STATE_RELATIVE_PATH);
  try {
    return JSON.parse(readText(statePath));
  } catch (_error) {
    return { version: 1, pallets: {} };
  }
}

function writeState(siteDir, state) {
  const statePath = path.join(siteDir, STATE_RELATIVE_PATH);
  ensureDir(path.dirname(statePath));
  writeText(statePath, `${JSON.stringify(state, null, 2)}\n`);
}

function comparePalletCodesDesc(a, b) {
  const codeA = String(a.code || a);
  const codeB = String(b.code || b);
  const numberCompare = Number(codeB.replace(/\D/g, '')) - Number(codeA.replace(/\D/g, ''));
  if (numberCompare !== 0) return numberCompare;
  const prefixCompare = codeB.slice(0, 2).localeCompare(codeA.slice(0, 2));
  if (prefixCompare !== 0) return prefixCompare;
  return codeB.localeCompare(codeA);
}

function updateState(siteDir, pallets, config, now = new Date()) {
  const previous = readState(siteDir);
  const previousPallets = previous.pallets || {};
  const nowIso = now.toISOString();
  const oldIso = new Date(now.getTime() - (config.newDays + 1) * 24 * 60 * 60 * 1000).toISOString();
  const firstRun = Object.keys(previousPallets).length === 0;
  const initialNewCodes = new Set(
    firstRun
      ? [...pallets]
        .sort(comparePalletCodesDesc)
        .slice(0, config.markAllCurrentNew ? pallets.length : config.initialNewLimit)
        .map((pallet) => pallet.code)
      : [],
  );
  const visibleCodes = new Set(pallets.map((pallet) => pallet.code));
  const next = {
    version: 1,
    generated_at: nowIso,
    new_days: config.newDays,
    pallets: { ...previousPallets },
  };

  for (const pallet of pallets) {
    const existing = next.pallets[pallet.code] || {};
    const firstPublishedAt = existing.first_published_at || nowIso;
    const shouldBeNewOnFirstRun = config.markAllCurrentNew || initialNewCodes.has(pallet.code);
    const currentPublishedAt = existing.status === 'published'
      ? existing.current_published_at || firstPublishedAt
      : firstRun && !shouldBeNewOnFirstRun
        ? oldIso
        : nowIso;
    const changed = existing.last_signature && existing.last_signature !== pallet.signature;

    next.pallets[pallet.code] = {
      first_published_at: firstPublishedAt,
      current_published_at: currentPublishedAt,
      last_published_at: nowIso,
      last_changed_at: changed ? nowIso : existing.last_changed_at || currentPublishedAt,
      last_signature: pallet.signature,
      title: pallet.title,
      name: pallet.name,
      category_slug: pallet.categorySlug,
      href: pallet.href,
      status: 'published',
    };
  }

  const minimumNewCount = Math.max(0, Number(config.minimumNewCount || 0));
  if (minimumNewCount > 0) {
    const currentNewCodes = new Set(
      Object.entries(next.pallets)
        .filter(([, pallet]) => (
          pallet.status === 'published' &&
          isWithinDays(pallet.current_published_at || pallet.first_published_at, now, config.newDays)
        ))
        .map(([code]) => code),
    );

    for (const pallet of [...pallets].sort(comparePalletCodesDesc)) {
      if (currentNewCodes.size >= minimumNewCount) break;
      if (currentNewCodes.has(pallet.code)) continue;
      const entry = next.pallets[pallet.code];
      if (!entry || entry.status !== 'published') continue;
      entry.current_published_at = nowIso;
      entry.last_changed_at = entry.last_changed_at || nowIso;
      currentNewCodes.add(pallet.code);
    }
  }

  for (const [code, existing] of Object.entries(next.pallets)) {
    if (visibleCodes.has(code)) continue;
    next.pallets[code] = {
      ...existing,
      status: 'hidden',
      last_hidden_at: existing.status === 'hidden' ? existing.last_hidden_at || nowIso : nowIso,
    };
  }

  writeState(siteDir, next);
  return next;
}

function isWithinDays(isoDate, now, days) {
  const timestamp = Date.parse(isoDate || '');
  if (Number.isNaN(timestamp)) return false;
  return now.getTime() - timestamp <= days * 24 * 60 * 60 * 1000;
}

function getNewCodes(state, now = new Date()) {
  const days = Number(state.new_days || DEFAULT_NEW_DAYS);
  return new Set(
    Object.entries(state.pallets || {})
      .filter(([, pallet]) => (
        pallet.status === 'published' &&
        isWithinDays(pallet.current_published_at || pallet.first_published_at, now, days)
      ))
      .map(([code]) => code),
  );
}

function getPublicListCodes(siteDir) {
  const listPath = path.join(siteDir, 'lotes', 'index.html');
  if (!fs.existsSync(listPath)) return [];

  const seen = new Set();
  const codes = [];
  const html = readText(listPath);
  const matches = html.matchAll(/(?:href=["'][^"']*lotes\/|data-pallet-code=["'])([A-Z]{2}\d{4})(?:\.html|["'])/gi);

  for (const match of matches) {
    const code = match[1].toUpperCase();
    if (seen.has(code)) continue;
    seen.add(code);
    codes.push(code);
  }

  return codes;
}

function getExplicitNewCodes(config) {
  if (!config.newCodesFile || !fs.existsSync(config.newCodesFile)) return [];

  const seen = new Set();
  const codes = [];
  const matches = readText(config.newCodesFile).matchAll(/\b[A-Z]{2}\d{4}\b/gi);

  for (const match of matches) {
    const code = match[0].toUpperCase();
    if (seen.has(code)) continue;
    seen.add(code);
    codes.push(code);
  }

  return codes;
}

function resolveNewCodes(siteDir, state, config, now = new Date()) {
  const explicitCodes = getExplicitNewCodes(config);
  const publicCodes = getPublicListCodes(siteDir);

  if (explicitCodes.length) {
    const publicSet = new Set(publicCodes);
    const missing = explicitCodes.filter((code) => !publicSet.has(code));
    if (missing.length) {
      console.warn(`Warning: ${missing.length} explicit new pallets are not in public listing: ${missing.join(', ')}`);
    }
    return new Set(explicitCodes.filter((code) => publicSet.has(code)));
  }

  const minimumNewCount = Math.max(0, Number(config.minimumNewCount || 0));

  if (minimumNewCount > 0 && publicCodes.length) {
    return new Set(publicCodes.sort(comparePalletCodesDesc).slice(0, minimumNewCount));
  }

  return getNewCodes(state, now);
}

function relativeAssetPrefix(filePath, siteDir) {
  const relDir = path.dirname(path.relative(siteDir, filePath)).replaceAll(path.sep, '/');
  return relDir === '.' ? '' : `${relDir.split('/').map(() => '..').join('/')}/`;
}

function ensureCssLink(html, prefix) {
  const href = `${prefix}assets/new-lots.css?v=20260825`;
  if (html.includes(href)) return html;
  return html.replace(/<\/head>/i, `  <link rel="stylesheet" href="${href}">\n</head>`);
}

function ensureScript(html, prefix) {
  const src = `${prefix}assets/new-lots.js`;
  if (html.includes(src)) return html;
  return html.replace(/<\/body>/i, `<script src="${src}"></script>\n</body>`);
}

function ensureIndexSection(html, state, newCodes) {
  const section = `
      <section class="new-lots-home mt-4" aria-label="Ultimos lotes publicados">
        <a href="lotes/index.html?f=nuevos" class="new-lots-home-button">Ver nuevos publicados (${newCodes.size})</a>
      </section>`;

  if (html.includes('class="new-lots-home')) {
    return html.replace(/<section class="new-lots-home[\s\S]*?<\/section>/i, section);
  }

  if (/<div class="mt-4">\s*<a href="lotes\/index\.html"/i.test(html)) {
    return html.replace(/(<div class="mt-4">\s*<a href="lotes\/index\.html"[\s\S]*?<\/div>)/i, `$1\n${section}`);
  }

  return html.replace(/<\/main>/i, `${section}\n</main>`);
}

function ensureListControls(html, newCount) {
  const controls = `
<section class="new-lot-controls" aria-label="Filtros de lotes">
  <button type="button" class="new-filter is-active" data-new-filter="all" aria-pressed="true">Todos</button>
  <button type="button" class="new-filter" data-new-filter="new" aria-pressed="false">Nuevos (${newCount})</button>
</section>`;

  if (html.includes('class="new-lot-controls')) {
    return html.replace(/<section class="new-lot-controls[\s\S]*?<\/section>/i, controls);
  }

  return html.replace(/<\/header>\s*/i, `</header>\n${controls}\n`);
}

function removeHiddenTableRows(html, hiddenCodes) {
  return html.replace(/<tr\b[^>]*data-pallet-code=["']([A-Z]{2}\d{4})["'][^>]*>[\s\S]*?<\/tr>/gi, (match, code) => (
    hiddenCodes.has(code.toUpperCase()) || />\s*0\/0\s*</.test(match) ? '' : match
  ));
}

function removeHiddenCards(html, hiddenCodes) {
  return html.replace(/<div class="card"([^>]*)>([\s\S]*?)<\/div>\s*(?=<div class="card"|<\/div>\s*<\/div>\s*<script|<\/body>)/gi, (match, attrs, body) => {
    const code = attrs.match(/data-pallet-code=["']([A-Z]{2}\d{4})["']/i)?.[1]?.toUpperCase() ||
      body.match(/card-code">\s*([A-Z]{2}\d{4})\s*</i)?.[1]?.toUpperCase() ||
      body.match(/lotes\/([A-Z]{2}\d{4})\.html/i)?.[1]?.toUpperCase();
    return (code && hiddenCodes.has(code)) || />\s*0\s+unidades\s*</i.test(match) ? '' : match;
  });
}

function normalizeNewBadgeMarkup(html) {
  return html
    .replace(/class=\\"lot-code-with-badge\\"/gi, 'class="lot-code-with-badge"')
    .replace(/class=\\"card-code-row\\"/gi, 'class="card-code-row"')
    .replace(/class=\\"new-lot-badge\\"/gi, 'class="new-lot-badge"');
}

function markTableRows(html, newCodes) {
  return html.replace(/<tr\b([^>]*)>([\s\S]*?)<\/tr>/gi, (match, attrs, body) => {
    const code = body.match(/lotes\/([A-Z]{2}\d{4})\.html/i)?.[1]?.toUpperCase();
    if (!code) return match;
    const isNew = newCodes.has(code);
    let nextAttrs = attrs
      .replace(/\sdata-pallet-code=["'][^"']*["']/gi, '')
      .replace(/\sdata-new-lot=["'][^"']*["']/gi, '');
    nextAttrs += ` data-pallet-code="${code}" data-new-lot="${isNew ? '1' : '0'}"`;
    let nextBody = normalizeNewBadgeMarkup(body).replace(/<span class="new-lot-badge">Nuevo<\/span>\s*/gi, '');
    nextBody = nextBody.replace(
      new RegExp(`(<span class="lot-code-with-badge">\\s*)(<a[^>]+lotes/${code}\\.html[^>]*>\\s*${code}\\s*</a>)(\\s*</span>)`, 'i'),
      '$2',
    );
    if (isNew) {
      nextBody = nextBody.replace(
        new RegExp(`(<a[^>]+lotes/${code}\\.html[^>]*>\\s*${code}\\s*</a>)`, 'i'),
        '<span class="lot-code-with-badge">$1<span class="new-lot-badge">Nuevo</span></span>',
      );
    }
    return `<tr${nextAttrs}>${nextBody}</tr>`;
  });
}

function markCards(html, newCodes) {
  let next = normalizeNewBadgeMarkup(html);
  next = next.replace(
    /<div class="card"([^>]*)>\s*(<a href="(?:\.\.\/)?lotes\/([A-Z]{2}\d{4})\.html" class="card-link"><\/a>)/gi,
    (_match, attrs, link, codeValue) => {
      const code = codeValue.toUpperCase();
      const cleanAttrs = attrs
        .replace(/\sdata-pallet-code=["'][^"']*["']/gi, '')
        .replace(/\sdata-new-lot=["'][^"']*["']/gi, '');
      return `<div class="card"${cleanAttrs} data-pallet-code="${code}" data-new-lot="${newCodes.has(code) ? '1' : '0'}">\n    ${link}`;
    },
  );
  next = next.replace(
    /<div class="card-code-row">\s*(<div class="card-code">\s*([A-Z]{2}\d{4})\s*<\/div>)\s*(?:<span class="new-lot-badge">Nuevo<\/span>\s*)?<\/div>/gi,
    '$1',
  );
  next = next.replace(
    /(<div class="card-code">\s*([A-Z]{2}\d{4})\s*<\/div>)/gi,
    (match, codeBlock, codeValue) => (
      newCodes.has(codeValue.toUpperCase())
        ? `<div class="card-code-row">${codeBlock}<span class="new-lot-badge">Nuevo</span></div>`
        : match
    ),
  );
  return next;
}

function processIndex(siteDir, state, newCodes) {
  const filePath = path.join(siteDir, 'index.html');
  if (!fs.existsSync(filePath)) return false;
  const prefix = relativeAssetPrefix(filePath, siteDir);
  const original = readText(filePath);
  const updated = ensureScript(ensureCssLink(ensureIndexSection(original, state, newCodes), prefix), prefix);
  if (updated === original) return false;
  writeText(filePath, updated);
  return true;
}

function processListPage(filePath, siteDir, newCodes, hiddenCodes) {
  const prefix = relativeAssetPrefix(filePath, siteDir);
  const original = readText(filePath);
  const pageCodes = new Set(
    [...original.matchAll(/(?:lotes\/|\.\/)?([A-Z]{2}\d{4})\.html/gi)]
      .map((match) => match[1].toUpperCase()),
  );
  const newCount = [...pageCodes].filter((code) => newCodes.has(code) && !hiddenCodes.has(code)).length;
  let updated = original;
  updated = removeHiddenTableRows(updated, hiddenCodes);
  updated = removeHiddenCards(updated, hiddenCodes);
  updated = ensureCssLink(updated, prefix);
  updated = ensureListControls(updated, newCount);
  updated = markTableRows(updated, newCodes);
  updated = markCards(updated, newCodes);
  updated = ensureScript(updated, prefix);

  if (updated === original) return false;
  writeText(filePath, updated);
  return true;
}

function deleteHiddenLotPages(lotsDir, hiddenCodes) {
  const deleted = [];
  for (const code of hiddenCodes) {
    const filePath = path.join(lotsDir, `${code}.html`);
    if (!fs.existsSync(filePath)) continue;
    fs.unlinkSync(filePath);
    deleted.push(path.relative(path.dirname(lotsDir), filePath).replaceAll(path.sep, '/'));
  }
  return deleted;
}

function writeAssets(siteDir) {
  const cssPath = path.join(siteDir, CSS_RELATIVE_PATH);
  const jsPath = path.join(siteDir, JS_RELATIVE_PATH);
  ensureDir(path.dirname(cssPath));

  writeText(cssPath, `.new-lots-home {
  display: flex;
  justify-content: center;
  width: 100%;
  margin: 22px 0 34px;
  clear: both;
}

.new-lots-home-button {
  display: inline-flex;
  align-items: center;
  justify-content: center;
  max-width: min(100%, 520px);
  min-height: 44px;
  padding: 14px 24px;
  border-radius: 8px;
  background: #fff;
  color: #111827;
  font-weight: 800;
  line-height: 1.2;
  text-decoration: none;
  box-shadow: 0 8px 20px rgba(0, 0, 0, .16);
}

.new-lots-home-button:hover {
  color: #111827;
  background: #f9fafb;
  text-decoration: none;
}

@media (max-width: 640px) {
  .new-lots-home {
    margin: 18px 0 28px;
  }

  .new-lots-home-button {
    width: 100%;
    max-width: 360px;
    padding: 13px 18px;
    font-size: 18px;
  }
}

.new-lot-controls {
  display: flex;
  flex-wrap: wrap;
  gap: 8px;
  margin: 16px 0;
}

.new-filter {
  border: 1px solid var(--border, #e5e7eb);
  border-radius: 8px;
  padding: 8px 12px;
  background: #fff;
  color: var(--fg, #222);
  font-weight: 700;
  cursor: pointer;
}

.new-filter.is-active {
  border-color: darkorange;
  background: darkorange;
  color: #fff;
}

.lot-code-with-badge,
.card-code-row {
  display: inline-flex;
  align-items: center;
  gap: 12px;
  flex-wrap: wrap;
  vertical-align: middle;
}

.lot-code-with-badge {
  display: inline-flex;
  align-items: center;
  justify-content: center;
  gap: 12px;
  width: auto;
}

.lot-code-with-badge > a {
  flex: 0 0 auto;
}

.lot-code-with-badge > .new-lot-badge {
  flex: 0 0 auto;
}

.new-lot-badge {
  display: inline-flex;
  align-items: center;
  justify-content: center;
  width: fit-content;
  margin: 0;
  min-height: 24px;
  padding: 4px 10px;
  border: 1px solid rgba(255, 255, 255, .72);
  border-radius: 999px;
  background: linear-gradient(180deg, #22c55e 0%, #119647 100%);
  color: #fff;
  box-shadow: 0 5px 12px rgba(17, 150, 71, .28), inset 0 1px 0 rgba(255, 255, 255, .35);
  font-size: 11px;
  font-weight: 900;
  line-height: 1.1;
  text-transform: uppercase;
  white-space: nowrap;
}

.card .new-lot-badge {
  transform: translateY(1px);
}

.card-code-row {
  justify-content: flex-start;
}

.new-lot-empty {
  margin: 16px 0;
  padding: 14px;
  border: 1px dashed var(--border, #e5e7eb);
  border-radius: 8px;
  color: var(--muted, #6b7280);
  text-align: center;
}

`);

  writeText(jsPath, `(function () {
  const buttons = Array.from(document.querySelectorAll('[data-new-filter]'));
  if (!buttons.length) return;

  const rows = Array.from(document.querySelectorAll('tr[data-pallet-code]'));
  const cards = Array.from(document.querySelectorAll('.card[data-pallet-code]'));
  const groups = [rows, cards].filter((group) => group.length);
  const controls = document.querySelector('.new-lot-controls');
  const empty = document.createElement('p');
  empty.className = 'new-lot-empty';
  empty.hidden = true;
  empty.textContent = 'No hay lotes nuevos en esta vista.';
  controls && controls.insertAdjacentElement('afterend', empty);

  function apply(filter) {
    let visible = 0;

    buttons.forEach((button) => {
      const active = button.dataset.newFilter === filter;
      button.classList.toggle('is-active', active);
      button.setAttribute('aria-pressed', String(active));
    });

    groups.forEach((group) => {
      group.forEach((item) => {
        const show = filter === 'all' || item.dataset.newLot === '1';
        item.hidden = !show;
        if (show) visible += 1;
      });
    });

    empty.hidden = filter !== 'new' || visible > 0;
  }

  buttons.forEach((button) => {
    button.addEventListener('click', () => {
      const filter = button.dataset.newFilter || 'all';
      const url = new URL(window.location.href);
      if (filter === 'new') url.searchParams.set('f', 'nuevos');
      else url.searchParams.delete('f');
      window.history.replaceState(null, '', url);
      apply(filter);
    });
  });

  apply(new URLSearchParams(window.location.search).get('f') === 'nuevos' ? 'new' : 'all');
})();
`);
}

function main() {
  const config = buildConfig();
  const indexPath = path.join(config.siteDir, 'index.html');
  const lotsDir = path.join(config.siteDir, 'lotes');
  const categoriesDir = path.join(config.siteDir, 'categorias');

  if (!fs.existsSync(indexPath) || !fs.existsSync(lotsDir) || !fs.existsSync(categoriesDir)) {
    throw new Error(`Not a VentaDeLotes static site: ${config.siteDir}`);
  }

  const pallets = walkFiles(lotsDir, (filePath) => filePath.endsWith('.html'))
    .map((filePath) => extractPalletInfo(filePath, config.siteDir))
    .filter(Boolean);
  const visiblePallets = pallets.filter((pallet) => Number(pallet.units || 0) > 0);
  const hiddenCodes = new Set(pallets
    .filter((pallet) => Number(pallet.units || 0) <= 0)
    .map((pallet) => pallet.code));

  const now = new Date();
  const state = updateState(config.siteDir, visiblePallets, config, now);
  const newCodes = resolveNewCodes(config.siteDir, state, config, now);
  const listPages = [
    path.join(config.siteDir, 'lotes', 'index.html'),
    ...walkFiles(categoriesDir, (filePath) => filePath.endsWith('.html')),
  ];

  writeAssets(config.siteDir);

  const changed = [];
  if (processIndex(config.siteDir, state, newCodes)) changed.push('index.html');

  for (const pagePath of listPages) {
    if (processListPage(pagePath, config.siteDir, newCodes, hiddenCodes)) {
      changed.push(path.relative(config.siteDir, pagePath).replaceAll(path.sep, '/'));
    }
  }

  const deleted = deleteHiddenLotPages(lotsDir, hiddenCodes);

  console.log(`VentaDeLotes enhanced in ${config.siteDir}`);
  console.log(`Pallets indexed: ${visiblePallets.length}`);
  console.log(`Empty pallets hidden: ${hiddenCodes.size}`);
  console.log(`New pallets: ${newCodes.size}`);
  console.log(`HTML pages changed: ${changed.length}`);
  changed.forEach((file) => console.log(`- ${file}`));
  console.log(`Empty lot pages deleted: ${deleted.length}`);
  deleted.forEach((file) => console.log(`- ${file}`));
}

if (require.main === module) {
  main();
}

module.exports = {
  extractPalletInfo,
  getNewCodes,
  processIndex,
  processListPage,
  removeHiddenCards,
  removeHiddenTableRows,
  updateState,
};
