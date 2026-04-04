#!/usr/bin/env python3
# =================================================
# SUBIDA FTP DEL DIRECTORIO OUTPUT
# =================================================
# Sube web/output/ al servidor FTP manteniendo la estructura.
# Excluye archivos innecesarios (.DS_Store, pallets.json, etc.).
# No sube lotes/*.html de pallets Vendido (se conservan en remoto).
#
# Uso: python3 scripts/upload_ftp.py
#
# Variables de entorno (.env): FTP_HOST, FTP_USER, FTP_PASS, DB_*
# =================================================

import os
import sys
from ftplib import FTP
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

BASE_DIR = Path(__file__).resolve().parent.parent
REPO_ROOT = BASE_DIR.parent
for _p in (str(REPO_ROOT), str(BASE_DIR)):
    if _p not in sys.path:
        sys.path.insert(0, _p)

OUTPUT_DIR = BASE_DIR / "web" / "output"
LOTES_DIR = OUTPUT_DIR / "lotes"

# -----------------------------------
# CONFIGURACIÓN FTP
# -----------------------------------
FTP_HOST = os.getenv("FTP_HOST", "bestcash-es.espacioseguro.com")
FTP_USER = os.getenv("FTP_USER", "")
FTP_PASS = os.getenv("FTP_PASS", "")

# Archivos y carpetas que NO se suben
EXCLUIDOS = {
    ".DS_Store",
    "Thumbs.db",
    "pallets.json",       # dato interno, no necesario en el sitio público
    "__pycache__",
    ".git",
    ".gitignore",
}


def obtener_codigos_vendidos():
    """Códigos de pallets Vendido que no se suben (se conservan en remoto)."""
    try:
        from db import get_connection
        conn = get_connection()
        cur = conn.cursor()
        cur.execute("SELECT code FROM boxes WHERE status = 'Vendido'")
        codes = {r[0] for r in cur.fetchall()}
        cur.close()
        conn.close()
        return codes
    except Exception:
        return set()


def debe_excluir(nombre: str, ruta_relativa: str = "", codigos_vendidos: set = None) -> bool:
    if nombre in EXCLUIDOS:
        return True
    if nombre.endswith(".pyc"):
        return True
    if nombre.startswith("."):
        return True
    if codigos_vendidos and ruta_relativa.startswith("lotes/") and nombre.endswith(".html"):
        code = nombre[:-5]
        if code in codigos_vendidos:
            return True
    return False


def crear_directorio_remoto(ftp: FTP, ruta: str) -> None:
    """Crea el directorio remoto y subcarpetas. Vuelve a raíz al terminar."""
    if not ruta or ruta == ".":
        return
    partes = [p for p in ruta.replace("\\", "/").split("/") if p]
    for i, p in enumerate(partes):
        camino = "/".join(partes[: i + 1])
        try:
            ftp.cwd(camino)
        except Exception:
            try:
                ftp.mkd(p)
            except Exception as e:
                if "550" not in str(e) and "exists" not in str(e).lower():
                    print(f"⚠️ No se pudo crear {camino}: {e}")
    try:
        ftp.cwd("/")
    except Exception:
        pass


def subir_archivo(ftp: FTP, local: Path, remoto: str) -> None:
    """Sube un archivo en modo binario. remoto es la ruta completa desde raíz."""
    parent = str(Path(remoto).parent)
    if parent and parent != ".":
        crear_directorio_remoto(ftp, parent)
        try:
            ftp.cwd(parent)
            nombre = Path(remoto).name
        except Exception:
            ftp.cwd("/")
            nombre = remoto
    else:
        ftp.cwd("/")
        nombre = remoto

    with open(local, "rb") as f:
        ftp.storbinary(f"STOR {nombre}", f)
    print(f"  ✓ {remoto}")
    try:
        ftp.cwd("/")
    except Exception:
        pass


def subir_directorio(ftp: FTP, local_dir: Path, remoto_prefijo: str = "", codigos_vendidos: set = None) -> int:
    """
    Recorre local_dir y sube todos los archivos.
    remoto_prefijo: ruta base en el servidor.
    codigos_vendidos: códigos de pallets Vendido que no se suben.
    Devuelve número de archivos subidos.
    """
    subidos = 0
    items = sorted(local_dir.iterdir())

    for item in items:
        remoto = f"{remoto_prefijo}/{item.name}" if remoto_prefijo else item.name
        if debe_excluir(item.name, remoto, codigos_vendidos):
            continue

        if item.is_dir():
            n = subir_directorio(ftp, item, remoto, codigos_vendidos)
            subidos += n
        else:
            try:
                subir_archivo(ftp, item, remoto)
                subidos += 1
            except Exception as e:
                print(f"  ❌ Error subiendo {item.name}: {e}")

    return subidos


def subir_archivos_especificos(ftp: FTP, archivos: list[str]) -> int:
    """
    Sube solo los archivos indicados (rutas relativas a OUTPUT_DIR).
    Ej: ["lotes/index.html", "resumen_general.html", "lotes/MP1001.html"]
    """
    subidos = 0
    for rel in archivos:
        local = OUTPUT_DIR / rel.replace("/", os.sep)
        if not local.is_file():
            print(f"  ⚠ No existe {rel}, omitiendo")
            continue
        try:
            subir_archivo(ftp, local, rel)
            subidos += 1
        except Exception as e:
            print(f"  ❌ Error subiendo {rel}: {e}")
    return subidos


def main():
    if not FTP_USER or not FTP_PASS:
        print("❌ Define FTP_USER y FTP_PASS (env o .env)")
        sys.exit(1)

    if not OUTPUT_DIR.is_dir():
        print(f"❌ No existe {OUTPUT_DIR}")
        sys.exit(1)

    print(f"📤 Conectando a {FTP_HOST}...")
    try:
        ftp = FTP(FTP_HOST, timeout=30)
        ftp.login(FTP_USER, FTP_PASS)
        ftp.set_pasv(True)
        print("✅ Conectado")
    except Exception as e:
        print(f"❌ Error de conexión: {e}")
        sys.exit(1)

    try:
        codigos_vendidos = obtener_codigos_vendidos()
        if codigos_vendidos:
            print(f"ℹ️ Omitiendo {len(codigos_vendidos)} pallets Vendido en subida FTP")
        print(f"\n📂 Subiendo {OUTPUT_DIR} ...\n")
        total = subir_directorio(ftp, OUTPUT_DIR, codigos_vendidos=codigos_vendidos)
        print(f"\n🎉 {total} archivos subidos correctamente")
    except Exception as e:
        print(f"\n❌ Error durante la subida: {e}")
        sys.exit(1)
    finally:
        ftp.quit()


if __name__ == "__main__":
    main()
