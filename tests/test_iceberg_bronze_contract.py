"""Validate contract v3 table resolution and Bronze physical layouts."""

from __future__ import annotations

import gzip
import json
import unittest
from pathlib import Path
from unittest.mock import MagicMock

from tools.contracts.ampere_contract import load_contract
from tools.contracts.spark_conformance import _active_partition_spec


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

    def test_active_partition_spec_reads_gzip_metadata_through_binary_file(self) -> None:
        metadata_uri = (
            "s3://warehouse/assortment/metadata/"
            "00000-test.gz.metadata.json"
        )
        metadata = {
            "default-spec-id": 1,
            "partition-specs": [
                {
                    "spec-id": 1,
                    "fields": [
                        {"source-id": 7, "field-id": 1000, "name": "event_month", "transform": "month"}
                    ],
                }
            ],
            "current-schema-id": 0,
            "schemas": [
                {
                    "schema-id": 0,
                    "fields": [{"id": 7, "name": "event_date", "type": "date", "required": False}],
                }
            ],
        }
        spark = MagicMock()
        spark.sql.return_value.collect.return_value = [{"file": metadata_uri}]
        spark.read.format.return_value.load.return_value.select.return_value.first.return_value = {
            "content": gzip.compress(json.dumps(metadata).encode("utf-8"))
        }

        observed = _active_partition_spec(spark, "iceberg_bronze.bronze.assortment")

        self.assertEqual(observed, [("event_date", "month")])
        spark.read.format.assert_called_once_with("binaryFile")
        spark.read.format.return_value.load.assert_called_once_with(
            "s3a://warehouse/assortment/metadata/00000-test.gz.metadata.json"
        )
        spark.read.format.return_value.load.return_value.select.assert_called_once_with("content")


if __name__ == "__main__":
    unittest.main()
