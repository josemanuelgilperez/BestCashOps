document.addEventListener("DOMContentLoaded", function () {
  const table = document.getElementById("summaryTable");
  if (!table) return;

  // Buscador: filtra filas por cualquier texto visible del resumen
  const searchInput = document.getElementById("summarySearch");
  if (searchInput) {
    searchInput.addEventListener("input", function () {
      const term = this.value.trim().toLowerCase();
      const tbody = table.querySelector("tbody");
      const rows = tbody.querySelectorAll("tr");

      rows.forEach((row) => {
        const text = (row.innerText || "").toLowerCase();
        const match = !term || text.includes(term);
        row.style.display = match ? "" : "none";
      });
    });
  }

  const headers = table.querySelectorAll("th.sortable");
  if (!headers.length) return;

  headers.forEach((header) => {
    header.addEventListener("click", () => {
      const column = header.dataset.column || "";
      const columnIndex = Array.from(header.parentElement.children).indexOf(header);
      const tbody = table.querySelector("tbody");
      const rows = Array.from(tbody.querySelectorAll("tr"));
      const currentSort = header.dataset.sort || "none";

      headers.forEach((h) => h.classList.remove("sorted-asc", "sorted-desc"));

      const ascending = currentSort === "none" || currentSort === "desc";
      header.dataset.sort = ascending ? "asc" : "desc";
      header.classList.add(ascending ? "sorted-asc" : "sorted-desc");

      rows.sort((a, b) => {
        const getText = (row) => {
          const text = (row.cells[columnIndex]?.innerText || "").trim();
          if (column === "code") return text.toUpperCase();
          return text.toLowerCase();
        };
        const textA = getText(a);
        const textB = getText(b);
        return ascending ? textA.localeCompare(textB, "es") : textB.localeCompare(textA, "es");
      });

      tbody.innerHTML = "";
      rows.forEach((r) => tbody.appendChild(r));
    });
  });
});
