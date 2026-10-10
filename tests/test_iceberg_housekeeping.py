"""Check that housekeeping resolves scope and thresholds from contract v3."""

from __future__ import annotations

import importlib.util
import sys
import unittest
from datetime import datetime, timezone
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
MODULE_PATH = ROOT / "docker/spark/iceberg_raw_etl/app/iceberg_housekeeping_connect.py"
SPEC = importlib.util.spec_from_file_location("iceberg_housekeeping_connect", MODULE_PATH)
housekeeping = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = housekeeping
SPEC.loader.exec_module(housekeeping)


class HousekeepingContractTests(unittest.TestCase):
    def test_contract_drives_all_42_housekeeping_targets(self) -> None:
        self.assertEqual(len(housekeeping.CONTRACT.tables), 42)
        self.assertEqual(
            {layer: len(housekeeping.CONTRACT.layer_tables(layer)) for layer in ("bronze", "silver", "gold")},
            {"bronze": 17, "silver": 17, "gold": 8},
        )
        self.assertEqual(
            housekeeping._contract_table("iceberg_bronze", "ops", "bronze_apply_registry").profile_name,
            "bronze_ops_registry",
        )

    def test_housekeeping_cutoffs_and_compaction_use_table_profile(self) -> None:
        as_of = datetime(2026, 10, 10, 12, 0, tzinfo=timezone.utc)
        spec = housekeeping.CONTRACT.table("silver", "fact_order_product")
        expire, orphan = housekeeping._procedure_sql(
            "iceberg_silver", "silver", "fact_order_product", as_of,
            dry_run=False, spec=spec,
        )
        self.assertIn("2026-09-26 12:00:00", expire)
        self.assertIn("2026-09-26 12:00:00", orphan)
        compact = housekeeping._compaction_sql(
            "iceberg_silver", "silver", "fact_order_product", spec
        )
        self.assertIn("'target-file-size-bytes', '134217728'", compact)
        self.assertIn("'min-input-files', '5'", compact)
        self.assertIn("'delete-file-threshold', '1'", compact)

    def test_rejects_invalid_cutoff_and_identifiers(self) -> None:
        with self.assertRaisesRegex(ValueError, "UTC"):
            housekeeping._cutoff_sql(datetime(2026, 9, 23, 13, 0))
        with self.assertRaisesRegex(ValueError, "identifier"):
            housekeeping._identifier("orders; DROP TABLE")


if __name__ == "__main__":
    unittest.main()
