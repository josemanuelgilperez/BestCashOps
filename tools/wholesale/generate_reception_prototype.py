#!/usr/bin/env python3
import html
import json
import os
import re


REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
SOURCE_HTML = os.path.join(REPO_ROOT, "wholesale", "web", "output", "lotes", "MP0187.html")
OUTPUT_DIR = os.path.join(REPO_ROOT, "wholesale", "web", "output", "recepcion")
OUTPUT_HTML = os.path.join(OUTPUT_DIR, "prototipo-mp0187.html")
TITLE_LIMIT = 34


def text_from_cell(value):
    value = re.sub(r"<[^>]+>", "", value)
    value = html.unescape(value)
    return re.sub(r"\s+", " ", value).strip()


def short_title(value):
    if len(value) <= TITLE_LIMIT:
        return value
    return value[: TITLE_LIMIT - 1].rstrip() + "…"


def extract_items(source):
    with open(source, "r", encoding="utf-8") as fh:
        doc = fh.read()

    rows = re.findall(r"<tr>\s*(.*?)\s*</tr>", doc, flags=re.S)
    items = []
    for row in rows:
        asin_match = re.search(r'<td class="col-asin">([^<]+)</td>', row)
        if not asin_match:
            continue

        cells = re.findall(r"<td(?:\s+class=\"[^\"]+\")?>(.*?)</td>", row, flags=re.S)
        image_match = re.search(r'<img src="([^"]+)" class="product"', row)
        asin = text_from_cell(cells[0])
        quantity = int(text_from_cell(cells[1]) or "0")
        title = text_from_cell(cells[2])
        image = html.unescape(image_match.group(1)) if image_match else ""

        items.append(
            {
                "asin": asin,
                "quantity": quantity,
                "title": title,
                "titleShort": short_title(title),
                "image": image,
            }
        )

    return items


def render(items):
    payload = json.dumps(items, ensure_ascii=False)
    total_units = sum(item["quantity"] for item in items)
    total_refs = len(items)
    return f"""<!DOCTYPE html>
<html lang="es">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Recepcion MP0187</title>
  <style>
    :root {{
      color-scheme: light;
      --ink: #17202a;
      --muted: #5d6a75;
      --line: #d8dee4;
      --soft: #f5f7f9;
      --ok: #177245;
      --ok-soft: #e8f6ef;
      --accent: #0f609b;
      --danger: #b42318;
    }}

    * {{ box-sizing: border-box; }}

    body {{
      margin: 0;
      min-height: 100vh;
      background: #fff;
      color: var(--ink);
      font-family: Arial, Helvetica, sans-serif;
    }}

    .topbar {{
      position: sticky;
      top: 0;
      z-index: 10;
      background: rgba(255, 255, 255, 0.96);
      border-bottom: 1px solid var(--line);
      backdrop-filter: blur(10px);
    }}

    .topbar-inner {{
      width: min(1180px, calc(100% - 28px));
      margin: 0 auto;
      padding: 14px 0;
      display: grid;
      gap: 12px;
    }}

    .heading {{
      display: flex;
      align-items: flex-end;
      justify-content: space-between;
      gap: 16px;
    }}

    h1 {{
      margin: 0;
      font-size: 22px;
      line-height: 1.15;
      letter-spacing: 0;
    }}

    .meta {{
      margin-top: 3px;
      color: var(--muted);
      font-size: 13px;
    }}

    .progress-text {{
      color: var(--muted);
      font-size: 14px;
      white-space: nowrap;
    }}

    .progress {{
      overflow: hidden;
      height: 9px;
      background: #e7ebef;
      border-radius: 999px;
    }}

    .progress-fill {{
      width: 0%;
      height: 100%;
      background: var(--ok);
      transition: width 180ms ease;
    }}

    .controls {{
      display: grid;
      grid-template-columns: 1fr auto auto;
      gap: 10px;
      align-items: center;
    }}

    .search {{
      width: 100%;
      min-height: 42px;
      border: 1px solid var(--line);
      border-radius: 8px;
      padding: 0 12px;
      font-size: 15px;
    }}

    .tabs {{
      display: inline-grid;
      grid-template-columns: repeat(3, auto);
      border: 1px solid var(--line);
      border-radius: 8px;
      overflow: hidden;
      background: #fff;
    }}

    .tab,
    .reset {{
      min-height: 42px;
      border: 0;
      background: #fff;
      color: var(--ink);
      padding: 0 12px;
      font: inherit;
      cursor: pointer;
    }}

    .tab + .tab {{ border-left: 1px solid var(--line); }}
    .tab.is-active {{ background: var(--ink); color: #fff; }}

    .reset {{
      border: 1px solid var(--line);
      border-radius: 8px;
      color: var(--danger);
    }}

    main {{
      width: min(1180px, calc(100% - 28px));
      margin: 18px auto 42px;
    }}

    .grid {{
      display: grid;
      grid-template-columns: repeat(auto-fill, minmax(168px, 1fr));
      gap: 12px;
    }}

    .item {{
      display: grid;
      grid-template-rows: 150px auto;
      min-width: 0;
      border: 1px solid var(--line);
      border-radius: 8px;
      background: #fff;
      overflow: hidden;
      cursor: pointer;
      text-align: left;
    }}

    .item.is-done {{
      background: var(--ok-soft);
      border-color: #9bd5b8;
    }}

    .photo {{
      width: 100%;
      height: 150px;
      display: grid;
      place-items: center;
      background: var(--soft);
      border-bottom: 1px solid var(--line);
      overflow: hidden;
    }}

    .photo img {{
      display: block;
      width: 100%;
      height: 100%;
      object-fit: contain;
      mix-blend-mode: multiply;
    }}

    .no-image {{
      color: var(--muted);
      font-size: 13px;
    }}

    .body {{
      display: grid;
      grid-template-rows: 21px 40px 20px;
      gap: 4px;
      padding: 9px 10px 10px;
    }}

    .asin {{
      font-size: 15px;
      font-weight: 700;
      letter-spacing: 0;
      line-height: 21px;
      overflow: hidden;
      text-overflow: ellipsis;
      white-space: nowrap;
    }}

    .title {{
      color: var(--muted);
      font-size: 14px;
      line-height: 20px;
      overflow: hidden;
      display: -webkit-box;
      -webkit-box-orient: vertical;
      -webkit-line-clamp: 2;
    }}

    .unit {{
      color: var(--accent);
      font-size: 13px;
      font-weight: 700;
      line-height: 20px;
    }}

    .item.is-done .unit {{
      color: var(--ok);
    }}

    .empty {{
      display: none;
      padding: 42px 12px;
      text-align: center;
      color: var(--muted);
    }}

    @media (max-width: 720px) {{
      .heading {{
        align-items: flex-start;
        flex-direction: column;
      }}

      .progress-text {{ white-space: normal; }}

      .controls {{
        grid-template-columns: 1fr;
      }}

      .tabs {{
        grid-template-columns: repeat(3, 1fr);
      }}

      .grid {{
        grid-template-columns: repeat(2, minmax(0, 1fr));
        gap: 10px;
      }}

      .item {{
        grid-template-rows: 126px auto;
      }}

      .photo {{
        height: 126px;
      }}
    }}
  </style>
</head>
<body>
  <header class="topbar">
    <div class="topbar-inner">
      <div class="heading">
        <div>
          <h1>Recepcion MP0187</h1>
          <div class="meta">{total_refs} referencias · {total_units} unidades · Mobiliario</div>
        </div>
        <div class="progress-text" data-progress-text>0 de {total_units} unidades recibidas</div>
      </div>
      <div class="progress" aria-hidden="true">
        <div class="progress-fill" data-progress-fill></div>
      </div>
      <div class="controls">
        <input class="search" type="search" data-search placeholder="Buscar ASIN o titulo">
        <div class="tabs" aria-label="Filtro">
          <button class="tab is-active" type="button" data-filter="pending">Pendientes</button>
          <button class="tab" type="button" data-filter="done">Recibidos</button>
          <button class="tab" type="button" data-filter="all">Todos</button>
        </div>
        <button class="reset" type="button" data-reset>Reiniciar</button>
      </div>
    </div>
  </header>

  <main>
    <section class="grid" data-grid></section>
    <div class="empty" data-empty>No hay productos para este filtro.</div>
  </main>

  <script>
    const ITEMS = {payload};
    const STORAGE_KEY = "bestcash-recepcion-prototipo-MP0187-unidades-v2";
    const state = JSON.parse(localStorage.getItem(STORAGE_KEY) || "{{}}");
    const UNITS = ITEMS.flatMap((item) => {{
      return Array.from({{ length: item.quantity }}, (_, index) => ({{
        ...item,
        unitNumber: index + 1,
        unitTotal: item.quantity,
        unitId: `${{item.asin}}-${{index + 1}}`,
      }}));
    }});
    let filter = "pending";
    let query = "";

    const grid = document.querySelector("[data-grid]");
    const empty = document.querySelector("[data-empty]");
    const search = document.querySelector("[data-search]");
    const progressText = document.querySelector("[data-progress-text]");
    const progressFill = document.querySelector("[data-progress-fill]");

    function isDone(unit) {{
      return Boolean(state[unit.unitId]);
    }}

    function save() {{
      localStorage.setItem(STORAGE_KEY, JSON.stringify(state));
    }}

    function matches(unit) {{
      const done = isDone(unit);
      const haystack = `${{unit.asin}} ${{unit.title}}`.toLowerCase();
      if (query && !haystack.includes(query)) return false;
      if (filter === "pending") return !done;
      if (filter === "done") return done;
      return true;
    }}

    function updateProgress() {{
      const total = UNITS.length;
      const done = UNITS.reduce((sum, unit) => sum + (isDone(unit) ? 1 : 0), 0);
      const pct = total ? Math.round((done / total) * 100) : 0;
      progressText.textContent = `${{done}} de ${{total}} unidades recibidas`;
      progressFill.style.width = `${{pct}}%`;
    }}

    function render() {{
      grid.innerHTML = "";
      let visible = 0;

      for (const unit of UNITS) {{
        if (!matches(unit)) continue;
        visible += 1;

        const done = isDone(unit);
        const card = document.createElement("article");
        card.className = `item${{done ? " is-done" : ""}}`;
        card.setAttribute("role", "button");
        card.setAttribute("tabindex", "0");
        card.setAttribute("aria-pressed", done ? "true" : "false");
        card.innerHTML = `
          <div class="photo">
            ${{unit.image ? `<img src="${{unit.image}}" alt="">` : `<span class="no-image">Sin imagen</span>`}}
          </div>
          <div class="body">
            <div class="asin">${{unit.asin}}</div>
            <div class="title" title="${{unit.title.replace(/"/g, "&quot;")}}">${{unit.titleShort}}</div>
            <div class="unit">Unidad ${{unit.unitNumber}}/${{unit.unitTotal}}</div>
          </div>
        `;

        function toggleUnit() {{
          if (isDone(unit)) {{
            delete state[unit.unitId];
          }} else {{
            state[unit.unitId] = true;
          }}
          save();
          updateProgress();
          render();
        }}

        card.addEventListener("click", toggleUnit);
        card.addEventListener("keydown", (event) => {{
          if (event.key !== "Enter" && event.key !== " ") return;
          event.preventDefault();
          toggleUnit();
        }});

        grid.appendChild(card);
      }}

      empty.style.display = visible ? "none" : "block";
      updateProgress();
    }}

    search.addEventListener("input", () => {{
      query = search.value.trim().toLowerCase();
      render();
    }});

    document.querySelectorAll("[data-filter]").forEach((button) => {{
      button.addEventListener("click", () => {{
        filter = button.dataset.filter;
        document.querySelectorAll("[data-filter]").forEach((tab) => {{
          tab.classList.toggle("is-active", tab === button);
        }});
        render();
      }});
    }});

    document.querySelector("[data-reset]").addEventListener("click", () => {{
      if (!confirm("Reiniciar la recepcion de este pallet?")) return;
      for (const unit of UNITS) delete state[unit.unitId];
      save();
      render();
    }});

    render();
  </script>
</body>
</html>
"""


def main():
    if not os.path.exists(SOURCE_HTML):
        raise SystemExit(f"No existe {SOURCE_HTML}")

    items = extract_items(SOURCE_HTML)
    if not items:
        raise SystemExit("No se han encontrado productos en MP0187.html")

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    with open(OUTPUT_HTML, "w", encoding="utf-8") as fh:
        fh.write(render(items))

    print(f"Prototipo generado: {OUTPUT_HTML}")
    print(f"Referencias: {len(items)}")
    print(f"Unidades: {sum(item['quantity'] for item in items)}")


if __name__ == "__main__":
    main()
