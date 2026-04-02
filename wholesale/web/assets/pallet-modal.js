function openModal(src) {
  document.body.style.overflow = "hidden";
  document.getElementById("modalImage").src = src;
  document.getElementById("downloadLink").href = src;
  document.getElementById("imageModal").style.display = "block";
}

function closeModal() {
  document.body.style.overflow = "";
  document.getElementById("imageModal").style.display = "none";
}

document.getElementById("imageModal").addEventListener("click", (e) => {
  if (e.target.id === "imageModal" || e.target.classList.contains("modal-close")) {
    closeModal();
  }
});

