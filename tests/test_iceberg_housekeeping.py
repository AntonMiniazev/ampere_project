"""Check Iceberg housekeeping scope and procedure order without deleting files."""

from __future__ import annotations

import ast
import importlib.util
import json
import sys
import unittest
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace


MODULE_PATH = (
    Path(__file__).resolve().parents[1]
    / "docker/spark/iceberg_raw_etl/app/iceberg_housekeeping_connect.py"
)
SPEC = importlib.util.spec_from_file_location("iceberg_housekeeping_connect", MODULE_PATH)
housekeeping = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = housekeeping
SPEC.loader.exec_module(housekeeping)


class FakeResult:
    def __init__(self, rows: list[object]) -> None:
        self.rows = rows

    def collect(self) -> list[object]:
        return self.rows

    def toLocalIterator(self):
        return iter(self.rows)


class FakeSpark:
    def __init__(self) -> None:
        self.statements: list[str] = []
        self.conf = SimpleNamespace(set=lambda key, value: None)

    def sql(self, statement: str) -> FakeResult:
        self.statements.append(statement)
        if statement.startswith("SHOW TABLES"):
            for catalog, schema in housekeeping.NAMESPACES:
                if f"`{catalog}`.`{schema}`" in statement:
                    names = housekeeping.EXPECTED_TABLES[catalog, schema]
                    if (catalog, schema) == ("iceberg_bronze", "bronze"):
                        names = names | {"clients_repair_backup_20261005"}
                    return FakeResult(
                        [
                            SimpleNamespace(tableName=name, isTemporary=False)
                            for name in names
                        ]
                    )
            raise AssertionError(statement)
        if ".system.expire_snapshots" in statement:
            return FakeResult(
                [SimpleNamespace(asDict=lambda: {"deleted_data_files_count": 0})]
            )
        return FakeResult([])


class HousekeepingTests(unittest.TestCase):
    def setUp(self) -> None:
        self.cutoff = datetime(2026, 9, 23, 13, 0, tzinfo=timezone.utc)

    def test_live_run_expires_then_removes_orphans_for_45_tables(self) -> None:
        spark = FakeSpark()
        self.assertEqual(housekeeping.run_housekeeping(spark, cutoff=self.cutoff), 45)
        calls = [sql for sql in spark.statements if sql.startswith("CALL")]
        self.assertEqual(len(calls), 90)
        for expire, orphan in zip(calls[::2], calls[1::2]):
            self.assertIn(".system.expire_snapshots", expire)
            self.assertIn("retain_last => 1", expire)
            self.assertIn(".system.remove_orphan_files", orphan)
            self.assertIn("dry_run => false", orphan)
            self.assertIn("2026-09-23 13:00:00", expire)
            self.assertIn("2026-09-23 13:00:00", orphan)
        self.assertFalse(any("rewrite_data_files" in sql for sql in calls))
        self.assertFalse(
            any("clients_repair_backup_20261005" in sql for sql in spark.statements)
        )
        policy_calls = [
            sql for sql in spark.statements if sql.startswith("ALTER TABLE")
        ]
        self.assertEqual(len(policy_calls), 45)
        self.assertTrue(
            all("write.metadata.previous-versions-max" in sql for sql in policy_calls)
        )

    def test_dry_run_only_previews_orphans(self) -> None:
        spark = FakeSpark()
        self.assertEqual(
            housekeeping.run_housekeeping(spark, cutoff=self.cutoff, dry_run=True),
            45,
        )
        calls = [sql for sql in spark.statements if sql.startswith("CALL")]
        self.assertEqual(len(calls), 45)
        self.assertTrue(all("dry_run => true" in sql for sql in calls))
        self.assertFalse(
            any(sql.startswith("ALTER TABLE") for sql in spark.statements)
        )

    def test_allowlist_matches_bronze_contract_and_published_models(self) -> None:
        root = MODULE_PATH.parents[4]
        contract = json.loads(
            (root / "tools/iceberg/contracts/ampere_tables.json").read_text(
                encoding="utf-8"
            )
        )
        bronze_tables = contract["catalog"]["layers"]["bronze"]["tables"]
        for schema in ("bronze", "ops"):
            names = {
                item["table_name"]
                for item in bronze_tables
                if item["schema_name"] == schema
            }
            self.assertEqual(
                names, housekeeping.EXPECTED_TABLES["iceberg_bronze", schema]
            )

        publisher_path = root / "docker/dbt_iceberg/publish_catalog.py"
        publisher_source = ast.parse(publisher_path.read_text(encoding="utf-8"))
        merge_keys = next(
            ast.literal_eval(node.value)
            for node in publisher_source.body
            if isinstance(node, ast.Assign)
            and any(
                isinstance(target, ast.Name) and target.id == "MERGE_KEYS"
                for target in node.targets
            )
        )
        for layer in ("silver", "gold"):
            self.assertEqual(
                set(merge_keys[layer]),
                housekeeping.EXPECTED_TABLES[f"iceberg_{layer}", layer],
            )

    def test_rejects_invalid_cutoff_and_identifiers(self) -> None:
        with self.assertRaisesRegex(ValueError, "UTC"):
            housekeeping._cutoff_sql(datetime(2026, 9, 23, 13, 0))
        with self.assertRaisesRegex(ValueError, "identifier"):
            housekeeping._identifier("orders; DROP TABLE")


if __name__ == "__main__":
    unittest.main()
