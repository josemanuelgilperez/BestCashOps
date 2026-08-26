#!/usr/bin/env python3
import argparse
import json
import mimetypes
import os
import re
import sys
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import unquote, urlparse

from jinja2 import Environment, FileSystemLoader, select_autoescape


REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from tools.wholesale.reception_common import (
    get_client_by_token,
    get_client_pallet,
    load_client_pallets,
    load_reception_units,
    update_unit_status,
)


TEMPLATE_DIR = os.path.join(REPO_ROOT, "wholesale", "web", "templates")
ASSETS_DIR = os.path.join(REPO_ROOT, "wholesale", "web", "assets")

TOKEN_RE = r"(?P<token>[A-Za-z0-9_-]{20,80})"
BOX_RE = r"(?P<box_code>[A-Za-z0-9_-]{2,32})"
CLIENT_RE = re.compile(rf"^/recepcion/{TOKEN_RE}/?$")
PALLET_RE = re.compile(rf"^/recepcion/{TOKEN_RE}/pallet/{BOX_RE}/?$")
UNIT_API_RE = re.compile(rf"^/api/reception/{TOKEN_RE}/units/(?P<unit_id>\d+)$")


env = Environment(
    loader=FileSystemLoader(TEMPLATE_DIR),
    autoescape=select_autoescape(["html", "xml"]),
)
tmpl_client = env.get_template("reception_client.html")
tmpl_pallet = env.get_template("reception_pallet.html")


def parse_args():
    parser = argparse.ArgumentParser(description="Servidor de recepcion de pallets.")
    parser.add_argument("--host", default=os.getenv("RECEPTION_HOST", "127.0.0.1"))
    parser.add_argument("--port", type=int, default=int(os.getenv("RECEPTION_PORT", "8092")))
    return parser.parse_args()


def bytes_payload(value):
    if isinstance(value, bytes):
        return value
    return value.encode("utf-8")


def status_class(status):
    return status.lower().replace(" ", "-")


class ReceptionHandler(BaseHTTPRequestHandler):
    server_version = "BestCashReception/1.0"

    def log_message(self, fmt, *args):
        print("%s - %s" % (self.address_string(), fmt % args), flush=True)

    def send_payload(self, status, payload, content_type):
        data = bytes_payload(payload)
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(data)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(data)

    def send_html(self, status, html):
        self.send_payload(status, html, "text/html; charset=utf-8")

    def send_json(self, status, payload):
        data = json.dumps(payload, ensure_ascii=False)
        self.send_payload(status, data, "application/json; charset=utf-8")

    def send_not_found(self):
        self.send_html(404, "<h1>Pagina no encontrada</h1>")

    def send_static(self, path):
        rel = path.removeprefix("/assets/")
        rel = os.path.normpath(rel)
        if rel.startswith("..") or os.path.isabs(rel):
            self.send_not_found()
            return

        full_path = os.path.join(ASSETS_DIR, rel)
        if not os.path.isfile(full_path):
            self.send_not_found()
            return

        with open(full_path, "rb") as fh:
            data = fh.read()
        content_type = mimetypes.guess_type(full_path)[0] or "application/octet-stream"
        self.send_payload(200, data, content_type)

    def load_client_or_404(self, token):
        client = get_client_by_token(token)
        if not client:
            self.send_html(404, "<h1>Enlace de recepcion no valido</h1>")
            return None
        return client

    def do_GET(self):
        path = unquote(urlparse(self.path).path)

        if path.startswith("/assets/"):
            self.send_static(path)
            return

        match = CLIENT_RE.match(path)
        if match:
            token = match.group("token")
            client = self.load_client_or_404(token)
            if not client:
                return
            pallets = load_client_pallets(client["id"])
            self.send_html(200, tmpl_client.render(client=client, pallets=pallets))
            return

        match = PALLET_RE.match(path)
        if match:
            token = match.group("token")
            box_code = match.group("box_code").upper()
            client = self.load_client_or_404(token)
            if not client:
                return
            pallet = get_client_pallet(client["id"], box_code)
            if not pallet:
                self.send_html(404, "<h1>Pallet no asignado a este cliente</h1>")
                return
            units = load_reception_units(pallet["client_pallet_id"])
            self.send_html(200, tmpl_pallet.render(client=client, pallet=pallet, units=units))
            return

        self.send_not_found()

    def do_POST(self):
        path = unquote(urlparse(self.path).path)
        match = UNIT_API_RE.match(path)
        if not match:
            self.send_not_found()
            return

        token = match.group("token")
        client = self.load_client_or_404(token)
        if not client:
            return

        try:
            length = int(self.headers.get("Content-Length", "0"))
            payload = json.loads(self.rfile.read(length).decode("utf-8") or "{}")
            status = payload.get("status")
            note = payload.get("note")
            unit_id = int(match.group("unit_id"))
            ok = update_unit_status(client["id"], unit_id, status, note=note)
            if not ok:
                self.send_json(404, {"ok": False, "error": "Unidad no encontrada"})
                return
            self.send_json(200, {"ok": True, "unit_id": unit_id, "status": status})
        except ValueError as exc:
            self.send_json(400, {"ok": False, "error": str(exc)})
        except Exception as exc:
            self.send_json(500, {"ok": False, "error": str(exc)})


def main():
    args = parse_args()
    server = ThreadingHTTPServer((args.host, args.port), ReceptionHandler)
    print(f"Recepcion escuchando en http://{args.host}:{args.port}", flush=True)
    server.serve_forever()


if __name__ == "__main__":
    main()
