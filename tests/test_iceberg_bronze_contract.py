"""Validate contract v3 table resolution and Bronze physical layouts."""

from __future__ import annotations

import unittest
from pathlib import Path

from tools.contracts.ampere_contract import load_contract


ROOT = Path(__file__).resolve().parents[1]


class IcebergContractTests(unittest.TestCase):
    def test_resolves_all_tables_profiles_columns_and_layouts(self) -> None:
        contract = load_contract(ROOT / "tools/contracts/ampere_tables.json")
        self.assertEqual(contract.version, 3)
        self.assertEqual(
            {layer: len(contract.layer_tables(layer)) for layer in ("bronze", "silver", "gold")},
            {"bronze": 17, "silver": 17, "gold": 8},
        )
        self.assertEqual(contract.table("bronze", "bronze_apply_registry").namespace, "ops")
        self.assertEqual(
            contract.table("bronze", "order_product").partition_spec,
            ({"source": "order_date", "transform": "months"},),
        )
        self.assertEqual(
            contract.table("bronze", "assortment").write["mode"],
            "overwrite_partitions",
        )
        payments = contract.table("bronze", "payments")
        self.assertEqual(payments.sort_order[0]["source"], "payment_date")
        self.assertEqual(payments.write["distribution"], "range")
        self.assertEqual(
            contract.table("silver", "fact_order_product").write["target_file_size_bytes"],
            134217728,
        )


if __name__ == "__main__":
    unittest.main()
