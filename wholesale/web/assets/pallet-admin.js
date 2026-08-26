(function () {
  const params = new URLSearchParams(window.location.search);
  if (params.get("admin") !== "1") return;

  const panel = document.querySelector("[data-pallet-admin-panel]");
  const button = document.querySelector("[data-pallet-rename-button]");
  const input = document.querySelector("[data-pallet-name-input]");
  const status = document.querySelector("[data-pallet-admin-status]");
  const title = document.querySelector("[data-pallet-title]");
  const nameNode = document.querySelector("[data-pallet-name]");

  if (!panel || !button || !input || !title || !nameNode) return;

  panel.hidden = false;

  function setStatus(message, isError) {
    if (!status) return;
    status.textContent = message || "";
    status.classList.toggle("is-error", Boolean(isError));
  }

  function getApiUrl() {
    const fromUrl = params.get("adminApi");
    if (fromUrl) {
      localStorage.setItem("bestcashPalletAdminApi", fromUrl.replace(/\/$/, ""));
      return fromUrl.replace(/\/$/, "");
    }

    const saved = localStorage.getItem("bestcashPalletAdminApi");
    if (saved) return saved.replace(/\/$/, "");

    const value = window.prompt("URL API admin", "http://127.0.0.1:8091");
    if (!value) return null;
    const normalized = value.replace(/\/$/, "");
    localStorage.setItem("bestcashPalletAdminApi", normalized);
    return normalized;
  }

  function getToken() {
    const saved = sessionStorage.getItem("bestcashPalletAdminToken");
    if (saved) return saved;

    const value = window.prompt("Token admin");
    if (!value) return null;
    sessionStorage.setItem("bestcashPalletAdminToken", value);
    return value;
  }

  button.addEventListener("click", async () => {
    const code = panel.getAttribute("data-pallet-code");
    const newName = input.value.trim().replace(/\s+/g, " ");
    if (!code || !newName) {
      setStatus("Introduce un nombre válido.", true);
      return;
    }

    const apiUrl = getApiUrl();
    const token = getToken();
    if (!apiUrl || !token) {
      setStatus("Falta API o token.", true);
      return;
    }

    button.disabled = true;
    setStatus("Guardando...", false);

    try {
      const response = await fetch(`${apiUrl}/api/pallets/${code}/rename`, {
        method: "POST",
        headers: {
          "Authorization": `Bearer ${token}`,
          "Content-Type": "application/json",
        },
        body: JSON.stringify({ name: newName }),
      });

      const data = await response.json();
      if (!response.ok || !data.ok) {
        throw new Error(data.error || "No se pudo guardar.");
      }

      nameNode.textContent = data.new_name;
      document.title = `${code} – ${data.new_name}`;
      setStatus("Guardado en base de datos.", false);
    } catch (error) {
      setStatus(error.message || String(error), true);
    } finally {
      button.disabled = false;
    }
  });
})();
