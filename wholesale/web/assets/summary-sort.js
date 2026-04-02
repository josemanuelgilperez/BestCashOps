document.addEventListener("DOMContentLoaded", function () {
  const table = document.getElementById("summaryTable");
  if (!table) return;

  // Buscador: filtra filas por código, nombre o estado
  const searchInput = document.getElementById("summarySearch");
  if (searchInput) {
    searchInput.addEventListener("input", function () {
      const term = this.value.trim().toLowerCase();
      const tbody = table.querySelector("tbody");
      const rows = tbody.querySelectorAll("tr");

      rows.forEach((row) => {
        const code = (row.cells[0]?.innerText || "").toLowerCase();
        const name = (row.cells[1]?.innerText || "").toLowerCase();
        const status = (row.cells[6]?.innerText || "").toLowerCase();

        const match = !term || code.includes(term) || name.includes(term) || status.includes(term);
        row.style.display = match ? "" : "none";
      });
    });
  }

  const headers = table.querySelectorAll("th.sortable");
  if (!headers.length) return;

  headers.forEach((header) => {
    header.addEventListener("click", () => {
      const column = header.dataset.column;
      const tbody = table.querySelector("tbody");
      const rows = Array.from(tbody.querySelectorAll("tr"));
      const currentSort = header.dataset.sort || "none";

      headers.forEach((h) => h.classList.remove("sorted-asc", "sorted-desc"));

      const ascending = currentSort === "none" || currentSort === "desc";
      header.dataset.sort = ascending ? "asc" : "desc";
      header.classList.add(ascending ? "sorted-asc" : "sorted-desc");

      rows.sort((a, b) => {
        const getText = (row, col) => {
          if (col === "code") return row.cells[0].innerText.trim().toUpperCase();
          if (col === "name") return row.cells[1].innerText.trim().toLowerCase();
          if (col === "status") return row.cells[6].innerText.trim().toLowerCase();
          return "";
        };
        const textA = getText(a, column);
        const textB = getText(b, column);
        return ascending ? textA.localeCompare(textB, "es") : textB.localeCompare(textA, "es");
      });

      tbody.innerHTML = "";
      rows.forEach((r) => tbody.appendChild(r));
    });
  });
});

