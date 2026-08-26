#!/usr/bin/env python3
import argparse
import json
import os
import re
import sys
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse


REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from db import get_connection


CODE_RE = re.compile(r"^/api/pallets/(MP\d+)/rename$", re.IGNORECASE)


def parse_args():
    parser = argparse.ArgumentParser(description="API administrativa para renombrar pallets.")
    parser.add_argument("--host", default=os.getenv("PALLET_ADMIN_HOST", "127.0.0.1"))
    parser.add_argument("--port", type=int, default=int(os.getenv("PALLET_ADMIN_PORT", "8091")))
    return parser.parse_args()


def _json_response(handler, status, payload):
    data = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    handler.send_response(status)
    handler.send_header("Content-Type", "application/json; charset=utf-8")
    handler.send_header("Content-Length", str(len(data)))
    handler.send_header("Access-Control-Allow-Origin", os.getenv("PALLET_ADMIN_CORS_ORIGIN", "*"))
    handler.send_header("Access-Control-Allow-Headers", "authorization, content-type")
    handler.send_header("Access-Control-Allow-Methods", "POST, OPTIONS")
    handler.end_headers()
    handler.wfile.write(data)


def _authorized(headers):
    token = os.getenv("PALLET_ADMIN_TOKEN")
    if not token:
        return False
    auth = headers.get("Authorization", "")
    return auth == f"Bearer {token}"


def _clean_name(value):
    value = re.sub(r"\s+", " ", (value or "").strip())
    if not value:
        raise ValueError("El nombre no puede estar vacío.")
    if len(value) > 120:
        raise ValueError("El nombre no puede superar 120 caracteres.")
    return value


def rename_pallet(code, new_name):
    conn = get_connection()
    cur = conn.cursor(dictionary=True)
    try:
        cur.execute("SELECT code, name FROM boxes WHERE code = %s LIMIT 1", (code,))
        before = cur.fetchone()
        if not before:
            return None

        cur.execute("UPDATE boxes SET name = %s WHERE code = %s", (new_name, code))
        conn.commit()
        return {
            "code": code,
            "old_name": before["name"],
            "new_name": new_name,
            "updated": cur.rowcount,
        }
    finally:
        cur.close()
        conn.close()


class PalletAdminHandler(BaseHTTPRequestHandler):
    server_version = "BestCashPalletAdmin/1.0"

    def log_message(self, fmt, *args):
        print("%s - %s" % (self.address_string(), fmt % args), flush=True)

    def do_OPTIONS(self):
        _json_response(self, 204, {})

    def do_POST(self):
        if not _authorized(self.headers):
            _json_response(self, 401, {"ok": False, "error": "No autorizado"})
            return

        path = urlparse(self.path).path
        match = CODE_RE.match(path)
        if not match:
            _json_response(self, 404, {"ok": False, "error": "Endpoint no encontrado"})
            return

        code = match.group(1).upper()
        try:
            length = int(self.headers.get("Content-Length", "0"))
            body = self.rfile.read(length).decode("utf-8")
            payload = json.loads(body or "{}")
            new_name = _clean_name(payload.get("name"))
            result = rename_pallet(code, new_name)
            if not result:
                _json_response(self, 404, {"ok": False, "error": f"No existe {code}"})
                return
            _json_response(self, 200, {"ok": True, **result})
        except ValueError as exc:
            _json_response(self, 400, {"ok": False, "error": str(exc)})
        except Exception as exc:
            _json_response(self, 500, {"ok": False, "error": str(exc)})


def main():
    args = parse_args()
    if not os.getenv("PALLET_ADMIN_TOKEN"):
        raise SystemExit("Define PALLET_ADMIN_TOKEN antes de arrancar el API.")

    server = ThreadingHTTPServer((args.host, args.port), PalletAdminHandler)
    print(f"API administrativa escuchando en http://{args.host}:{args.port}", flush=True)
    server.serve_forever()


if __name__ == "__main__":
    main()
