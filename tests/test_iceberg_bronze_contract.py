"""Check that Iceberg Bronze tables match the canonical contract."""

from __future__ import annotations

import json
import logging
import os
import sys
import unittest
from pathlib import Path
from unittest.mock import patch


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "docker" / "spark" / "iceberg_raw_etl" / "app"))

from iceberg_bronze.catalog import _bronze_tables, ensure_iceberg_table  # noqa: E402


class RecordingSpark:
    """Collect catalog DDL without a live Lakekeeper dependency."""

    def __init__(self) -> None:
        self.statements: list[str] = []

    def sql(self, statement: str) -> None:
        """Record an attempted Spark SQL statement."""
        self.statements.append(statement)


class IcebergContractTests(unittest.TestCase):
    """Guard schema and partition parity for every Bronze table."""

    @classmethod
    def setUpClass(cls) -> None:
        """Load the production Bronze contract once for the suite."""
        cls.contract_path = ROOT / "tools" / "iceberg" / "contracts" / "ampere_tables.json"
        cls.tables = json.loads(cls.contract_path.read_text(encoding="utf-8"))["catalog"][
            "layers"
        ]["bronze"]["tables"]

    def test_all_contract_tables_have_matching_iceberg_ddl(self) -> None:
        """Create each table with its contract columns and partition key."""
        with patch.dict(os.environ, {"ICEBERG_CONTRACT_PATH": str(self.contract_path)}):
            _bronze_tables.cache_clear()
            spark = RecordingSpark()
            for spec in self.tables:
                schema = spec["schema_name"]
                table = spec["table_name"]
                result = ensure_iceberg_table(
                    spark,
                    catalog="iceberg_bronze",
                    schema=schema,
                    table=table,
                    logger=logging.getLogger(__name__),
                )
                self.assertEqual(
                    result, f"`iceberg_bronze`.`{schema}`.`{table}`"
                )
                statement = spark.statements[-1]
                self.assertIn("USING iceberg", statement)
                expected_columns = spec["columns"]
                if schema == "ops":
                    expected_columns = json.loads(
                        (ROOT / "docker" / "spark" / "iceberg_raw_etl" / "app" /
                         "iceberg_bronze" / "bronze_apply_registry_schema.json").read_text(
                            encoding="utf-8"
                        )
                    )["fields"]
                for column in expected_columns:
                    self.assertIn(
                        f"`{column['name']}` {column.get('type_text', column.get('type')).lower()}",
                        statement,
                    )
                partition = (
                    "source_table"
                    if schema == "ops"
                    else spec.get("stream_group", {})
                    .get("group_config", {})
                    .get("partition_key")
                )
                if partition and any(c["name"] == partition for c in expected_columns):
                    self.assertIn(f"PARTITIONED BY (`{partition}`)", statement)
                else:
                    self.assertNotIn("PARTITIONED BY", statement)
            self.assertEqual(len(spark.statements), 2 * len(self.tables))


if __name__ == "__main__":
    unittest.main()
