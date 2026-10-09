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
    def __init__(self, file_stats: dict[str, tuple[int, int]] | None = None) -> None:
        self.statements: list[str] = []
        self.file_stats = file_stats or {}
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
        if statement.startswith("SELECT COUNT(*)"):
            name = statement.rsplit("FROM ", 1)[1]
            file_count, total_bytes = self.file_stats.get(name, (12, 8 * 1024**2))
            return FakeResult(
                [SimpleNamespace(file_count=file_count, total_bytes=total_bytes)]
            )
        if ".system.rewrite_data_files" in statement:
            return FakeResult(
                [SimpleNamespace(asDict=lambda: {"rewritten_data_files_count": 12})]
            )
        if ".system.expire_snapshots" in statement:
            return FakeResult(
                [SimpleNamespace(asDict=lambda: {"deleted_data_files_count": 0})]
            )
        return FakeResult([])


class HousekeepingTests(unittest.TestCase):
    def setUp(self) -> None:
        self.cutoff = datetime(2026, 9, 23, 13, 0, tzinfo=timezone.utc)

    def test_live_run_compacts_then_expires_and_removes_orphans(self) -> None:
        spark = FakeSpark()
        self.assertEqual(housekeeping.run_housekeeping(spark, cutoff=self.cutoff), 42)
        calls = [sql for sql in spark.statements if sql.startswith("CALL")]
        self.assertEqual(len(calls), 126)
        for compact, expire, orphan in zip(calls[::3], calls[1::3], calls[2::3]):
            self.assertIn(".system.rewrite_data_files", compact)
            self.assertIn("strategy => 'binpack'", compact)
            self.assertIn("'max-concurrent-file-group-rewrites', '1'", compact)
            self.assertIn("'max-file-group-size-bytes', '536870912'", compact)
            self.assertIn("'min-input-files', '2'", compact)
            self.assertIn(".system.expire_snapshots", expire)
            self.assertIn("retain_last => 1", expire)
            self.assertIn(".system.remove_orphan_files", orphan)
            self.assertIn("dry_run => false", orphan)
            self.assertIn("2026-09-23 13:00:00", expire)
            self.assertIn("2026-09-23 13:00:00", orphan)
        self.assertTrue(
            any("'bronze.clients'" in sql for sql in calls if "rewrite_data_files" in sql)
        )
        self.assertFalse(
            any("clients_repair_backup_20261005" in sql for sql in spark.statements)
        )
        policy_calls = [
            sql for sql in spark.statements if sql.startswith("ALTER TABLE")
        ]
        self.assertEqual(len(policy_calls), 42)
        self.assertTrue(
            all("write.metadata.previous-versions-max" in sql for sql in policy_calls)
        )

    def test_dry_run_only_previews_orphans(self) -> None:
        spark = FakeSpark()
        self.assertEqual(
            housekeeping.run_housekeeping(spark, cutoff=self.cutoff, dry_run=True),
            42,
        )
        calls = [sql for sql in spark.statements if sql.startswith("CALL")]
        self.assertEqual(len(calls), 42)
        self.assertTrue(all("dry_run => true" in sql for sql in calls))
        self.assertFalse(
            any(sql.startswith("ALTER TABLE") for sql in spark.statements)
        )

    def test_skips_large_or_single_file_tables_without_skipping_cleanup(self) -> None:
        spark = FakeSpark(
            {
                "`iceberg_bronze`.`bronze`.`order_product`.`data_files`": (
                    100, housekeeping.MAX_COMPACTION_TABLE_BYTES + 1
                ),
                "`iceberg_bronze`.`bronze`.`clients`.`data_files`": (1, 1024),
            }
        )
        self.assertEqual(housekeeping.run_housekeeping(spark, cutoff=self.cutoff), 42)
        calls = [sql for sql in spark.statements if sql.startswith("CALL")]
        compact = [sql for sql in calls if ".system.rewrite_data_files" in sql]
        self.assertEqual(len(compact), 40)
        self.assertFalse(any("'bronze.order_product'" in sql for sql in compact))
        self.assertFalse(any("'bronze.clients'" in sql for sql in compact))
        self.assertEqual(
            sum(".system.expire_snapshots" in sql for sql in calls), 42
        )
        self.assertEqual(
            sum(".system.remove_orphan_files" in sql for sql in calls), 42
        )

    def test_clients_with_three_active_files_are_eligible(self) -> None:
        spark = FakeSpark(
            {"`iceberg_bronze`.`bronze`.`clients`.`data_files`": (3, 4_829_800)}
        )
        self.assertTrue(
            housekeeping._compact_table(
                spark, "iceberg_bronze", "bronze", "clients", dry_run=False
            )
        )
        calls = [sql for sql in spark.statements if "rewrite_data_files" in sql]
        self.assertEqual(len(calls), 1)
        self.assertIn("'min-input-files', '2'", calls[0])

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
