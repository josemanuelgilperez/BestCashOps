import sys
import types
import unittest
from argparse import Namespace
from importlib.util import module_from_spec, spec_from_file_location
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch


MODULE_PATH = (
    Path(__file__).resolve().parents[1]
    / "tools"
    / "wholesale"
    / "publish_wholesale.py"
)


def load_module():
    spec = spec_from_file_location("publish_wholesale_under_test", MODULE_PATH)
    module = module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def scoped_args(**overrides):
    values = {
        "boxes": None,
        "from_asins": None,
        "new_pallets": True,
    }
    values.update(overrides)
    return Namespace(**values)


class PublishWholesaleTest(unittest.TestCase):
    def test_write_new_codes_file_writes_resolved_current_scope(self):
        module = load_module()
        fake_finance = types.SimpleNamespace(
            resolve_scope_codes=lambda **kwargs: ["MP1400", "MP1422"]
        )

        with TemporaryDirectory() as tmp:
            module.REPO_ROOT = tmp
            with patch.dict(
                sys.modules,
                {"wholesale.pipeline.finance": fake_finance},
            ):
                module.write_new_codes_file(scoped_args())

            path = Path(tmp) / "wholesale" / "data" / "new_published_pallets.txt"
            self.assertEqual(path.read_text(encoding="utf-8"), "MP1400\nMP1422\n")

    def test_write_new_codes_file_rejects_empty_scope_and_clears_stale_file(self):
        module = load_module()
        fake_finance = types.SimpleNamespace(resolve_scope_codes=lambda **kwargs: [])

        with TemporaryDirectory() as tmp:
            module.REPO_ROOT = tmp
            path = Path(tmp) / "wholesale" / "data" / "new_published_pallets.txt"
            path.parent.mkdir(parents=True)
            path.write_text("MP1332\nMP1342\n", encoding="utf-8")

            with patch.dict(
                sys.modules,
                {"wholesale.pipeline.finance": fake_finance},
            ):
                with self.assertRaisesRegex(SystemExit, "no reutilizar una tanda anterior"):
                    module.write_new_codes_file(scoped_args())

            self.assertEqual(path.read_text(encoding="utf-8"), "")


if __name__ == "__main__":
    unittest.main()
