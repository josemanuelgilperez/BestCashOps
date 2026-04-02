(function () {
  const table = document.getElementById("palletTable");
  if (!table) return;

  const tbody = table.querySelector("tbody");
  const headers = table.querySelectorAll("th.sortable");
  if (!tbody || !headers.length) return;

  let currentSort = { index: null, dir: "asc" };

  function getCellValue(row, index, type) {
    const cell = row.children[index];
    if (!cell) return "";
    let text = cell.innerText.replace("€", "").replace("kg", "").trim();

    if (type === "number") return parseFloat(text.replace(",", ".")) || 0;

    if (type === "status") {
      if (text.includes("Disponible")) return 1;
      if (text.includes("Reservado")) return 2;
      return 3;
    }

    return text.toLowerCase();
  }

  function clearSortIcons() {
    headers.forEach(h => h.classList.remove("sorted-asc", "sorted-desc"));
  }

  headers.forEach(header => {
    header.addEventListener("click", () => {
      const index = Array.from(header.parentNode.children).indexOf(header);
      const type = header.dataset.type || "text";
      const dir = (currentSort.index === index && currentSort.dir === "asc") ? "desc" : "asc";

      currentSort = { index, dir };

      clearSortIcons();
      header.classList.add(dir === "asc" ? "sorted-asc" : "sorted-desc");

      const rows = Array.from(tbody.querySelectorAll("tr"));

      rows.sort((a, b) => {
        const A = getCellValue(a, index, type);
        const B = getCellValue(b, index, type);

        if (A < B) return dir === "asc" ? -1 : 1;
        if (A > B) return dir === "asc" ? 1 : -1;
        return 0;
      });

      rows.forEach(r => tbody.appendChild(r));
    });
  });
})();

