(function () {
  const page = document.querySelector("[data-reception-page]");
  if (!page) return;

  const token = page.getAttribute("data-token");
  const grid = document.querySelector("[data-grid]");
  const empty = document.querySelector("[data-empty]");
  const search = document.querySelector("[data-search]");
  const progressText = document.querySelector("[data-progress-text]");
  const progressFill = document.querySelector("[data-progress-fill]");
  let filter = "pending";
  let query = "";

  function cards() {
    return Array.from(document.querySelectorAll("[data-unit-id]"));
  }

  function isDone(card) {
    return card.getAttribute("data-status") !== "pending";
  }

  function matches(card) {
    const statusDone = isDone(card);
    const haystack = card.getAttribute("data-search-text") || "";
    if (query && !haystack.includes(query)) return false;
    if (filter === "pending") return !statusDone;
    if (filter === "done") return statusDone;
    return true;
  }

  function updateProgress() {
    const all = cards();
    const done = all.filter(isDone).length;
    const total = all.length;
    const pct = total ? Math.round((done / total) * 100) : 0;
    if (progressText) {
      progressText.textContent = `${done} de ${total} unidades recibidas`;
    }
    if (progressFill) {
      progressFill.style.setProperty("--progress", `${pct}%`);
    }
  }

  function applyFilter() {
    let visible = 0;
    for (const card of cards()) {
      const show = matches(card);
      card.hidden = !show;
      if (show) visible += 1;
    }
    if (empty) {
      empty.style.display = visible ? "none" : "block";
    }
    updateProgress();
  }

  async function setStatus(card, nextStatus) {
    const previous = card.getAttribute("data-status");
    const unitId = card.getAttribute("data-unit-id");
    card.setAttribute("data-status", nextStatus);
    card.classList.toggle("is-done", nextStatus === "received");
    card.classList.toggle("is-missing", nextStatus === "missing");
    card.classList.toggle("is-damaged", nextStatus === "damaged");
    card.setAttribute("aria-pressed", nextStatus !== "pending" ? "true" : "false");
    applyFilter();

    try {
      const response = await fetch(`/api/reception/${token}/units/${unitId}`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ status: nextStatus }),
      });
      const data = await response.json();
      if (!response.ok || !data.ok) {
        throw new Error(data.error || "No se pudo guardar.");
      }
    } catch (error) {
      card.setAttribute("data-status", previous);
      card.classList.toggle("is-done", previous === "received");
      card.classList.toggle("is-missing", previous === "missing");
      card.classList.toggle("is-damaged", previous === "damaged");
      card.setAttribute("aria-pressed", previous !== "pending" ? "true" : "false");
      applyFilter();
      window.alert(error.message || String(error));
    }
  }

  if (search) {
    search.addEventListener("input", () => {
      query = search.value.trim().toLowerCase();
      applyFilter();
    });
  }

  document.querySelectorAll("[data-filter]").forEach((button) => {
    button.addEventListener("click", () => {
      filter = button.dataset.filter;
      document.querySelectorAll("[data-filter]").forEach((tab) => {
        tab.classList.toggle("is-active", tab === button);
      });
      applyFilter();
    });
  });

  if (grid) {
    grid.addEventListener("click", (event) => {
      const card = event.target.closest("[data-unit-id]");
      if (!card) return;
      setStatus(card, card.getAttribute("data-status") === "pending" ? "received" : "pending");
    });

    grid.addEventListener("keydown", (event) => {
      if (event.key !== "Enter" && event.key !== " ") return;
      const card = event.target.closest("[data-unit-id]");
      if (!card) return;
      event.preventDefault();
      setStatus(card, card.getAttribute("data-status") === "pending" ? "received" : "pending");
    });
  }

  applyFilter();
})();
