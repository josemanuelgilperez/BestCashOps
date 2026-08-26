import unittest
from importlib.util import module_from_spec, spec_from_file_location
from pathlib import Path

MODULE_PATH = (
    Path(__file__).resolve().parents[1]
    / "base"
    / "bestcash_modules"
    / "wallapop.py"
)
SPEC = spec_from_file_location("wallapop_formatting", MODULE_PATH)
wallapop = module_from_spec(SPEC)
SPEC.loader.exec_module(wallapop)

WALLAPOP_DESCRIPTION_MAX_CHARS = wallapop.WALLAPOP_DESCRIPTION_MAX_CHARS
WALLAPOP_TITLE_MAX_CHARS = wallapop.WALLAPOP_TITLE_MAX_CHARS
build_wallapop_description = wallapop.build_wallapop_description
build_wallapop_title = wallapop.build_wallapop_title


class WallapopFormattingTest(unittest.TestCase):
    def test_title_is_limited_to_wallapop_max_chars(self):
        title = "Casco de montar ligero para ninos uvex kidoxx ajustable individual"

        result = build_wallapop_title(title, asin="B012345678")

        self.assertLessEqual(len(result), WALLAPOP_TITLE_MAX_CHARS)
        self.assertEqual(result, "Casco de montar ligero para ninos uvex kidoxx")

    def test_description_is_limited_and_includes_bestcash_ref(self):
        description = build_wallapop_description(
            asin="B012345678",
            title="Casco infantil",
            description="Producto en buen estado. " * 80,
            features=["Ajustable", "Ligero"],
            brand="uvex",
        )

        self.assertLessEqual(len(description), WALLAPOP_DESCRIPTION_MAX_CHARS)
        self.assertTrue(description.endswith("REF. BESTCASH B012345678"))


if __name__ == "__main__":
    unittest.main()
