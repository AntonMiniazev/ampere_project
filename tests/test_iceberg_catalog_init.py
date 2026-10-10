"""Tests for contract-driven Spark Connect Iceberg catalog initialization."""

from __future__ import annotations

import importlib.util
import sys
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch


ROOT = Path(__file__).resolve().parents[1]
APP_DIR = ROOT / "docker/spark/connect_client/app"
sys.path.insert(0, str(APP_DIR))

SPEC = importlib.util.spec_from_file_location(
    "initialize_iceberg_catalog",
    APP_DIR / "initialize_iceberg_catalog.py",
)
assert SPEC and SPEC.loader
initialize_catalog = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(initialize_catalog)


class CatalogInitTests(unittest.TestCase):
    def test_table_ddl_preserves_bronze_month_partition_and_sort_order(self) -> None:
        contract = initialize_catalog.load_contract()
        table = contract.table("bronze", "delivery_tracking")
        spark = MagicMock()

        with patch.object(initialize_catalog, "validate_spark_table"):
            initialize_catalog._create_table(spark, contract, table)

        statements = [call.args[0] for call in spark.sql.call_args_list]
        create = next(statement for statement in statements if "CREATE TABLE" in statement)
        self.assertIn("PARTITIONED BY (months(`status_datetime`))", create)
        self.assertIn("'format-version' = '2'", create)
        self.assertIn("write.target-file-size-bytes", create)
        self.assertIn(
            "ALTER TABLE `iceberg_bronze`.`bronze`.`delivery_tracking` "
            "WRITE ORDERED BY `status_datetime` ASC",
            statements,
        )

    def test_initialize_uses_existing_spark_connect_session(self) -> None:
        session = MagicMock()
        builder = MagicMock()
        builder.remote.return_value.appName.return_value.getOrCreate.return_value = session
        contract = initialize_catalog.load_contract()

        with (
            patch.object(initialize_catalog, "SparkSession") as spark_session,
            patch.object(initialize_catalog, "load_contract", return_value=contract),
            patch.object(initialize_catalog, "_create_table") as create_table,
        ):
            spark_session.builder = builder
            initialize_catalog.initialize("sc://spark-connect.test:15002")

        builder.remote.assert_called_once_with("sc://spark-connect.test:15002")
        self.assertEqual(create_table.call_count, len(contract.tables))
        session.stop.assert_called_once_with()


if __name__ == "__main__":
    unittest.main()
