import sys
import types
import unittest
from datetime import date
from importlib.util import module_from_spec, spec_from_file_location
from pathlib import Path
from tempfile import NamedTemporaryFile, TemporaryDirectory
from unittest.mock import patch


MODULE_PATH = (
    Path(__file__).resolve().parents[1]
    / "wholesale"
    / "scripts"
    / "update_status_and_deploy.py"
)


def load_module():
    fake_dotenv = types.SimpleNamespace(load_dotenv=lambda: None)
    fake_db = types.SimpleNamespace(get_connection=lambda: None)
    fake_slugify = types.SimpleNamespace(slugify=lambda value: str(value).lower())
    fake_upload_ftp = types.SimpleNamespace(
        FTP_HOST="example.test",
        subir_archivos_especificos=lambda *args, **kwargs: None,
    )

    with patch.dict(
        sys.modules,
        {
            "dotenv": fake_dotenv,
            "db": fake_db,
            "slugify": fake_slugify,
            "scripts.upload_ftp": fake_upload_ftp,
        },
    ):
        spec = spec_from_file_location("update_status_and_deploy_under_test", MODULE_PATH)
        module = module_from_spec(spec)
        spec.loader.exec_module(module)
        return module


class FakeCursor:
    def __init__(self):
        self.executed = []
        self.rowcount = 0

    def execute(self, sql, params=None):
        self.executed.append((sql, params))
        if "UPDATE boxes" in sql:
            self.rowcount = 1

    def fetchall(self):
        return [
            ("reservado_para",),
            ("reservado_por",),
            ("fecha_reserva",),
            ("fecha_venta",),
        ]

    def close(self):
        pass


class FakeConnection:
    def __init__(self):
        self.cursor_instance = FakeCursor()
        self.committed = False

    def cursor(self):
        return self.cursor_instance

    def commit(self):
        self.committed = True

    def rollback(self):
        pass

    def close(self):
        pass


class UpdateStatusAndDeployTest(unittest.TestCase):
    def test_sold_row_keeps_reservation_names_from_csv(self):
        module = load_module()

        with NamedTemporaryFile("w+", encoding="utf-8", newline="", suffix=".csv") as handle:
            handle.write(
                "code,status,reservado_para,reservado_por,fecha_reserva,fecha_venta\n"
                "MP1214,Vendido,Arturo,Teo,,20/08/2026\n"
            )
            handle.flush()
            module.CSV_PATH = Path(handle.name)

            rows = list(module._iter_status_rows())

        self.assertEqual(rows[0]["code"], "MP1214")
        self.assertEqual(rows[0]["status"], "Vendido")
        self.assertEqual(rows[0]["reservado_para"], "Arturo")
        self.assertEqual(rows[0]["reservado_por"], "Teo")
        self.assertIsNone(rows[0]["fecha_reserva"])
        self.assertEqual(rows[0]["fecha_venta"], date(2026, 8, 20))

    def test_sold_update_writes_reservation_names_when_present(self):
        module = load_module()
        connection = FakeConnection()

        with NamedTemporaryFile("w+", encoding="utf-8", newline="", suffix=".csv") as handle:
            handle.write(
                "code,status,reservado_para,reservado_por,fecha_reserva,fecha_venta\n"
                "MP1214,Vendido,Arturo,Teo,,20/08/2026\n"
            )
            handle.flush()
            module.CSV_PATH = Path(handle.name)
            module.get_connection = lambda: connection

            affected = module.actualizar_estados_desde_csv()

        updates = [
            call for call in connection.cursor_instance.executed
            if "UPDATE boxes" in call[0]
        ]
        self.assertEqual(affected, {"MP1214"})
        self.assertTrue(connection.committed)
        self.assertIn("reservado_para = COALESCE", updates[0][0])
        self.assertEqual(
            updates[0][1],
            ("Vendido", "Arturo", "Teo", None, date(2026, 8, 20), "MP1214"),
        )

    def test_apply_new_lot_filters_runs_marker_and_uploads_all_category_pages(self):
        module = load_module()

        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            site = root / "web" / "output"
            (site / "categorias").mkdir(parents=True)
            (site / "lotes").mkdir()
            (site / "assets").mkdir()
            (site / "categorias" / "mobiliario.html").write_text("", encoding="utf-8")
            (site / "categorias" / "hogar.html").write_text("", encoding="utf-8")
            marker_script = root / "tools" / "wholesale" / "mark_new_lots.py"
            marker_script.parent.mkdir(parents=True)
            marker_file = root / "marker-ran.txt"
            marker_script.write_text(
                "import pathlib\n"
                f"pathlib.Path({str(marker_file)!r}).write_text('ok', encoding='utf-8')\n",
                encoding="utf-8",
            )
            new_codes_file = root / "wholesale" / "data" / "new_published_pallets.txt"
            new_codes_file.parent.mkdir(parents=True)
            new_codes_file.write_text("MP1214\n", encoding="utf-8")

            module.REPO_ROOT = root
            module.WEB_OUTPUT_DIR = site
            module.NEW_CODES_FILE = new_codes_file
            module.MARK_NEW_LOTS_SCRIPT = marker_script

            result = module.apply_new_lot_filters(
                ["lotes/MP1214.html", "categorias/mobiliario.html"]
            )

            self.assertEqual(marker_file.read_text(encoding="utf-8"), "ok")
            self.assertEqual(result.count("categorias/mobiliario.html"), 1)
            self.assertIn("categorias/hogar.html", result)
            self.assertIn("lotes/index.html", result)
            self.assertIn("assets/new-lots.css", result)
            self.assertIn("assets/new-lots.js", result)

    def test_validate_new_lot_list_pages_rejects_unmarked_category(self):
        module = load_module()

        with TemporaryDirectory() as tmp:
            site = Path(tmp)
            category = site / "categorias" / "mobiliario.html"
            category.parent.mkdir(parents=True)
            category.write_text(
                '<html><body><a href="../lotes/MP1214.html">MP1214</a></body></html>',
                encoding="utf-8",
            )
            module.WEB_OUTPUT_DIR = site

            with self.assertRaisesRegex(RuntimeError, "mobiliario.html"):
                module.validate_new_lot_list_pages(["categorias/mobiliario.html"])

    def test_validate_new_lot_list_pages_accepts_marked_category(self):
        module = load_module()

        with TemporaryDirectory() as tmp:
            site = Path(tmp)
            category = site / "categorias" / "mobiliario.html"
            category.parent.mkdir(parents=True)
            category.write_text(
                '<html><head><link rel="stylesheet" href="../assets/new-lots.css">'
                '</head><body><button data-new-filter="new">Nuevos</button>'
                '<table><tr data-pallet-code="MP1214" data-new-lot="1">'
                '<td><a href="../lotes/MP1214.html">MP1214</a></td>'
                '</tr></table><script src="../assets/new-lots.js"></script>'
                '</body></html>',
                encoding="utf-8",
            )
            module.WEB_OUTPUT_DIR = site

            module.validate_new_lot_list_pages(["categorias/mobiliario.html"])


if __name__ == "__main__":
    unittest.main()
