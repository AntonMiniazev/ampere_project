"""Validate contract v5 layer resolution and key physical layouts."""

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
        self.assertEqual(contract.version, 5)
        self.assertEqual(
            {layer: len(contract.layer_tables(layer)) for layer in ("raw", "bronze", "silver", "gold")},
            {"raw": 16, "bronze": 17, "silver": 17, "gold": 8},
        )
        self.assertEqual(contract.layer_inventory["total"], 58)
        orders = contract.table("raw", "orders")
        self.assertEqual(orders.namespace, "raw")
        self.assertEqual(orders.ingestion["kind"], "compact_source_change_metadata")
        self.assertEqual(orders.ingestion["entity_keys"], ("_dataset_id", "_source_row_id"))
        self.assertEqual(orders.ingestion["source_order"], ("_batch_sequence", "_change_id"))
        self.assertNotIn("before_image_columns", orders.ingestion)
        self.assertNotIn("_before_order_date", orders.column_names)
        for table in contract.layer_tables("raw"):
            bronze = contract.table("bronze", table.name)
            bronze_columns = {column["name"]: column["type_text"] for column in bronze.columns}
            self.assertEqual(bronze_columns["_dataset_id"], "string")
            self.assertEqual(bronze_columns["_source_row_id"], "string")
            self.assertEqual(bronze_columns["_source_batch_sequence"], "bigint")
            self.assertEqual(bronze_columns["_source_change_id"], "bigint")
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

    def test_raw_sort_order_is_read_from_active_iceberg_metadata(self) -> None:
        from tools.contracts.spark_conformance import _active_sort_order

        metadata_uri = "s3://warehouse/orders/metadata/00000-test.metadata.json"
        metadata = {
            "default-sort-order-id": 1,
            "sort-orders": [{"order-id": 1, "fields": [
                {"source-id": 2, "transform": "identity", "direction": "asc", "null-order": "nulls-first"},
                {"source-id": 3, "transform": "identity", "direction": "asc", "null-order": "nulls-first"},
            ]}],
            "current-schema-id": 0,
            "schemas": [{"schema-id": 0, "fields": [
                {"id": 2, "name": "_batch_sequence"},
                {"id": 3, "name": "_change_id"},
            ]}],
        }
        spark = MagicMock()
        spark.sql.return_value.collect.return_value = [{"file": metadata_uri}]
        spark.read.format.return_value.load.return_value.select.return_value.first.return_value = {
            "content": json.dumps(metadata).encode("utf-8")
        }

        observed = _active_sort_order(spark, "iceberg_raw.raw.orders")

        self.assertEqual(observed, [("_batch_sequence", "asc"), ("_change_id", "asc")])


if __name__ == "__main__":
    unittest.main()
