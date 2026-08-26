(function () {
  const params = new URLSearchParams(window.location.search);
  const hostname = window.location.hostname.toLowerCase();
  const isManagementHost = hostname === "gestionpallets.bestcash.es";

  if (isManagementHost || params.get("adminLinks") === "1") {
    document.body.classList.add("is-management-app");
  }
})();
