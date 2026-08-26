#!/usr/bin/env python3
import argparse
import re
import urllib.request
from pathlib import Path


CSS_HREF = "assets/new-lots.css?v=20260825"
JS_SRC = "assets/new-lots.js"
NEW_LOTS_CSS = """.new-lots-home {
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

"""


def parse_args():
    parser = argparse.ArgumentParser(description="Marca lotes nuevos en el HTML estatico de VentaDeLotes.")
    parser.add_argument("--site", default="wholesale/web/output", help="Directorio web/output.")
    parser.add_argument(
        "--new-codes-file",
        default="wholesale/data/new_published_pallets.txt",
        help="TXT con codigos nuevos, uno por linea.",
    )
    parser.add_argument(
        "--home-url",
        default="https://ventadelotes.bestcash.es/index.html",
        help="URL de portada publica que se usa si web/output/index.html no existe.",
    )
    return parser.parse_args()


def load_new_codes(path):
    text = Path(path).read_text(encoding="utf-8", errors="ignore")
    return set(re.findall(r"\b[A-Z]{2}\d{4}\b", text.upper()))


def asset_prefix(file_path, site_dir):
    rel_parent = file_path.parent.relative_to(site_dir)
    if str(rel_parent) == ".":
        return ""
    return "../" * len(rel_parent.parts)


def ensure_assets(html, prefix):
    css_href = f"{prefix}{CSS_HREF}"
    js_src = f"{prefix}{JS_SRC}"
    html = re.sub(
        rf'\s*<link rel="stylesheet" href="{re.escape(prefix)}assets/new-lots\.css\?v=[^"]+">\s*',
        "\n",
        html,
        flags=re.I,
    )
    if css_href not in html:
        html = re.sub(r"</head>", f'  <link rel="stylesheet" href="{css_href}">\n</head>', html, flags=re.I)
    if js_src not in html:
        html = re.sub(r"</body>", f'<script src="{js_src}"></script>\n</body>', html, flags=re.I)
    return html


def write_new_lot_css(site_dir):
    css_path = site_dir / "assets" / "new-lots.css"
    css_path.parent.mkdir(parents=True, exist_ok=True)
    existing = css_path.read_text(encoding="utf-8", errors="ignore") if css_path.exists() else ""
    marker = ".new-lot-controls"
    rest = existing[existing.find(marker):] if marker in existing else """
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

.new-lot-badge {
  display: inline-flex;
  align-items: center;
  justify-content: center;
  width: fit-content;
  margin: 0;
  min-height: 24px;
  padding: 4px 10px;
  border-radius: 999px;
  background: linear-gradient(180deg, #22c55e 0%, #119647 100%);
  color: #fff;
  font-size: 11px;
  font-weight: 900;
  line-height: 1.1;
  text-transform: uppercase;
  white-space: nowrap;
}

"""
    css_path.write_text(NEW_LOTS_CSS + rest, encoding="utf-8")


def ensure_controls(html, new_count):
    controls = f"""
<section class="new-lot-controls" aria-label="Filtros de lotes">
  <button type="button" class="new-filter is-active" data-new-filter="all" aria-pressed="true">Todos</button>
  <button type="button" class="new-filter" data-new-filter="new" aria-pressed="false">Nuevos ({new_count})</button>
</section>"""
    html = re.sub(r"<section class=\"new-lot-controls[\s\S]*?</section>\s*", "", html, flags=re.I)
    return re.sub(r"</header>\s*", f"</header>\n{controls}\n", html, count=1, flags=re.I)


def clean_badges(html):
    html = re.sub(r"<span class=\"new-lot-badge\">Nuevo</span>\s*", "", html, flags=re.I)
    html = re.sub(
        r"<span class=\"lot-code-with-badge\">\s*(<a[^>]+>\s*[A-Z]{2}\d{4}\s*</a>)\s*</span>",
        r"\1",
        html,
        flags=re.I,
    )
    html = re.sub(
        r"<div class=\"card-code-row\">\s*(<div class=\"card-code\">\s*[A-Z]{2}\d{4}\s*</div>)\s*</div>",
        r"\1",
        html,
        flags=re.I,
    )
    return html


def add_or_replace_attr(attrs, name, value):
    attrs = re.sub(rf"\s{name}=[\"'][^\"']*[\"']", "", attrs, flags=re.I)
    return f'{attrs} {name}="{value}"'


def mark_rows(html, new_codes):
    def repl(match):
        attrs, body = match.group(1), match.group(2)
        code_match = re.search(r"(?:\.\./)?lotes/([A-Z]{2}\d{4})\.html", body, flags=re.I)
        if not code_match:
            return match.group(0)
        code = code_match.group(1).upper()
        is_new = code in new_codes
        attrs2 = add_or_replace_attr(attrs, "data-pallet-code", code)
        attrs2 = add_or_replace_attr(attrs2, "data-new-lot", "1" if is_new else "0")
        body2 = body
        if is_new:
            body2 = re.sub(
                rf"(<a[^>]+(?:\.\./)?lotes/{code}\.html[^>]*>\s*{code}\s*</a>)",
                r'<span class="lot-code-with-badge">\1<span class="new-lot-badge">Nuevo</span></span>',
                body2,
                count=1,
                flags=re.I,
            )
        return f"<tr{attrs2}>{body2}</tr>"

    return re.sub(r"<tr\b([^>]*)>([\s\S]*?)</tr>", repl, html, flags=re.I)


def mark_cards(html, new_codes):
    def card_open_repl(match):
        attrs, link, code_raw = match.group(1), match.group(2), match.group(3)
        code = code_raw.upper()
        attrs2 = add_or_replace_attr(attrs, "data-pallet-code", code)
        attrs2 = add_or_replace_attr(attrs2, "data-new-lot", "1" if code in new_codes else "0")
        return f'<div class="card"{attrs2}>\n    {link}'

    html = re.sub(
        r"<div class=\"card\"([^>]*)>\s*(<a href=\"(?:\.\./)?lotes/([A-Z]{2}\d{4})\.html\" class=\"card-link\"></a>)",
        card_open_repl,
        html,
        flags=re.I,
    )

    def code_repl(match):
        block, code_raw = match.group(1), match.group(2)
        if code_raw.upper() not in new_codes:
            return block
        return f'<div class="card-code-row">{block}<span class="new-lot-badge">Nuevo</span></div>'

    return re.sub(
        r"(<div class=\"card-code\">\s*([A-Z]{2}\d{4})\s*</div>)",
        code_repl,
        html,
        flags=re.I,
    )


def page_codes(html):
    return {code.upper() for code in re.findall(r"(?:\.\./)?lotes/([A-Z]{2}\d{4})\.html", html, flags=re.I)}


def patch_list_page(path, site_dir, new_codes):
    original = path.read_text(encoding="utf-8", errors="ignore")
    html = clean_badges(original)
    prefix = asset_prefix(path, site_dir)
    count = len(page_codes(html) & new_codes)
    html = ensure_assets(html, prefix)
    html = ensure_controls(html, count)
    html = mark_rows(html, new_codes)
    html = mark_cards(html, new_codes)
    if html != original:
        path.write_text(html, encoding="utf-8")
        return True, count
    return False, count


def patch_home(site_dir, new_codes):
    path = site_dir / "index.html"
    if not path.exists():
        return False
    original = path.read_text(encoding="utf-8", errors="ignore")
    prefix = asset_prefix(path, site_dir)
    html = ensure_assets(original, prefix)
    section = (
        '\n      <section class="new-lots-home mt-4" aria-label="Ultimos lotes publicados">\n'
        f'        <a href="lotes/index.html?f=nuevos" class="new-lots-home-button">Ver nuevos publicados ({len(new_codes)})</a>\n'
        "      </section>"
    )
    if "class=\"new-lots-home" in html:
        html = re.sub(r"<section class=\"new-lots-home[\s\S]*?</section>", section.strip(), html, flags=re.I)
    elif "</main>" in html.lower():
        html = re.sub(r"</main>", f"{section}\n</main>", html, count=1, flags=re.I)
    if html != original:
        path.write_text(html, encoding="utf-8")
        return True
    return False


def ensure_home_file(site_dir, home_url):
    path = site_dir / "index.html"
    if path.exists() or not home_url:
        return False
    try:
        with urllib.request.urlopen(home_url, timeout=20) as response:
            html = response.read().decode("utf-8", errors="ignore")
    except Exception as exc:
        print(f"warning: no se pudo descargar portada publica {home_url}: {exc}")
        return False
    if "<html" not in html.lower():
        print(f"warning: la portada descargada no parece HTML valido: {home_url}")
        return False
    path.write_text(html, encoding="utf-8")
    print(f"home_index_created={path}")
    return True


def main():
    args = parse_args()
    site_dir = Path(args.site)
    new_codes = load_new_codes(args.new_codes_file)
    if not new_codes:
        raise SystemExit(f"No hay codigos nuevos en {args.new_codes_file}")
    ensure_home_file(site_dir, args.home_url)
    write_new_lot_css(site_dir)

    pages = [site_dir / "lotes" / "index.html"]
    pages.extend(sorted((site_dir / "categorias").glob("*.html")))
    changed = []
    counts = {}
    for page in pages:
        if not page.exists():
            continue
        did_change, count = patch_list_page(page, site_dir, new_codes)
        counts[str(page.relative_to(site_dir))] = count
        if did_change:
            changed.append(str(page.relative_to(site_dir)))

    if patch_home(site_dir, new_codes):
        changed.append("index.html")

    print(f"new_codes={len(new_codes)}")
    print(f"pages_changed={len(changed)}")
    for page in changed:
        print(f"- {page}")
    print("counts:")
    for page, count in counts.items():
        if count:
            print(f"{page}\t{count}")


if __name__ == "__main__":
    main()
