"""Checks typed bounded staging and committed-batch visibility markers."""

from __future__ import annotations

import hashlib
import io
import json
import sys
import unittest
from datetime import date
from pathlib import Path
from uuid import UUID
import polars as pl
from botocore.exceptions import ClientError

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from tools.source_batch_staging import StagedChangeBatch  # noqa: E402


class MemoryS3:
    def __init__(self):
        self.objects: dict[tuple[str, str], bytes] = {}

    def head_object(self, *, Bucket, Key):
        if (Bucket, Key) not in self.objects:
            raise ClientError({"Error": {"Code": "404"}}, "HeadObject")
        return {}

    def get_object(self, *, Bucket, Key):
        if (Bucket, Key) not in self.objects:
            raise ClientError({"Error": {"Code": "404"}}, "GetObject")
        return {"Body": io.BytesIO(self.objects[(Bucket, Key)])}

    def put_object(self, *, Bucket, Key, Body, Metadata):
        self.objects[(Bucket, Key)] = Body


class StagedChangeBatchTests(unittest.TestCase):
    def test_ordered_typed_chunks_and_zero_change_manifest_are_published(self):
        client = MemoryS3()
        writer = StagedChangeBatch(
            dataset_id="dataset-1",
            batch_id="batch-1",
            batch_sequence=7,
            business_date=date(2026, 1, 2),
            contract_version=6,
            source_tables=("orders", "payments"),
            client=client,
            bucket="test",
            prefix="staging",
            chunk_rows=2,
        )
        frame = pl.DataFrame({
            "id": [1, 2, 3],
            "client_id": [10, 20, 30],
            "order_date": [date(2026, 1, 2)] * 3,
            "order_source_id": [1, 1, 2],
            "store_id": [2, 2, 3],
            "total_amount": ["1.25", "2.50", "3.75"],
            "source_row_id": ["row-1", "row-2", "row-3"],
        })
        self.assertEqual(writer.append("orders", frame, "I"), 3)
        self.assertEqual(writer.append("orders", frame.head(1), "U"), 1)
        self.assertEqual(writer.row_counts, {"orders": 4, "payments": 0})

        manifest = writer.prepare()
        self.assertEqual(manifest["tables"]["payments"]["files"], [])
        self.assertEqual(manifest["tables"]["orders"]["operation_counts"], {"B": 0, "I": 3, "U": 1, "D": 0})
        files = manifest["tables"]["orders"]["files"]
        self.assertEqual([item["rows"] for item in files], [2, 1, 1])

        chunks = [
            pl.read_parquet(io.BytesIO(client.objects[("test", item["key"])]))
            for item in files
        ]
        changes = pl.concat(chunks).sort("_change_id")
        self.assertEqual(changes.get_column("_change_id").to_list(), [1, 2, 3, 4])
        self.assertEqual(changes.get_column("_op").to_list(), ["I", "I", "I", "U"])
        self.assertEqual(changes.get_column("_source_row_id").to_list(), ["row-1", "row-2", "row-3", "row-1"])
        self.assertEqual(changes.get_column("_business_date").to_list(), [date(2026, 1, 2)] * 4)

        marker_key = f"{writer._base}/COMMITTED.json"
        self.assertNotIn(("test", marker_key), client.objects)
        writer.publish_committed()
        marker = json.loads(client.objects[("test", marker_key)])
        self.assertTrue(marker["postgres_committed"])

    def test_database_uuid_identity_is_written_as_parquet_string(self):
        client = MemoryS3()
        writer = StagedChangeBatch(
            dataset_id="dataset-1", batch_id="batch-uuid", batch_sequence=2,
            business_date=date(2026, 1, 1), contract_version=6,
            source_tables=("orders",), client=client, bucket="test", prefix="staging",
        )
        source_id = UUID("36ff796f-2dca-4d9f-a739-2d2bb48ba161")
        writer.append("orders", pl.DataFrame({
            "id": [1], "client_id": [10], "order_date": [date(2026, 1, 1)],
            "order_source_id": [1], "store_id": [2], "total_amount": ["12.50"],
            "source_row_id": [source_id],
        }), "I")
        manifest = writer.prepare()
        staged = pl.read_parquet(io.BytesIO(client.objects[("test", manifest["tables"]["orders"]["files"][0]["key"])]))
        self.assertEqual(staged.get_column("_source_row_id").to_list(), [str(source_id)])

    def test_all_null_values_keep_the_contract_column_type(self):
        client = MemoryS3()
        writer = StagedChangeBatch(
            dataset_id="dataset-1", batch_id="batch-null", batch_sequence=3,
            business_date=date(2026, 1, 1), contract_version=6,
            source_tables=("delivery_tracking",), client=client,
            bucket="test", prefix="staging",
        )
        writer.append(
            "delivery_tracking",
            pl.DataFrame({
                "order_id": [11], "courier_id": [None], "delivery_status_id": [None],
                "status": [None], "status_datetime": [None], "source_row_id": ["row-null"],
            }),
            "I",
        )
        manifest = writer.prepare()
        staged = pl.read_parquet(
            io.BytesIO(client.objects[("test", manifest["tables"]["delivery_tracking"]["files"][0]["key"])])
        )
        self.assertEqual(staged.schema["status"], pl.String)
        self.assertEqual(staged.schema["status_datetime"], pl.Datetime("us"))

    def test_immutable_object_rejects_conflicting_payload(self):
        client = MemoryS3()
        from tools.source_batch_staging import _put_immutable

        _put_immutable(client, "test", "one", b"first", hashlib.sha256(b"first").hexdigest())
        with self.assertRaisesRegex(RuntimeError, "immutable"):
            _put_immutable(client, "test", "one", b"second", hashlib.sha256(b"second").hexdigest())


if __name__ == "__main__":
    unittest.main()
