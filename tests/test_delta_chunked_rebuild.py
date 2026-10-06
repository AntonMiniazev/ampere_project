"""Verify bounded rebuild progress and its nonpartitioned Gold merge."""

from __future__ import annotations

import importlib.util
import json
import os
import sys
import tempfile
import unittest
from datetime import date
from pathlib import Path
from unittest.mock import patch

import duckdb
import pyarrow as pa
from deltalake import DeltaTable, write_deltalake


SCRIPT_DIR = Path(__file__).resolve().parents[1] / "docker/dbt/scripts"
sys.path.insert(0, str(SCRIPT_DIR))


def load_script(name: str):
    """Load image scripts without executing their command-line entrypoints."""
    spec = importlib.util.spec_from_file_location(name, SCRIPT_DIR / f"{name}.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


rebuild = load_script("rebuild_in_windows")
publisher = load_script("publish_silver_tables")


class FakeClient:
    """Hold checkpoint objects through simulated task retries."""

    def __init__(self):
        self.objects = {}

    def get_object(self, *, Bucket, Key):
        """Return a stored checkpoint or a MinIO-style missing-key error."""
        from botocore.exceptions import ClientError

        if (Bucket, Key) not in self.objects:
            raise ClientError({"Error": {"Code": "NoSuchKey"}}, "GetObject")

        class Body:
            """Expose the S3 response body shape."""

            def __init__(self, data):
                self.data = data

            def read(self):
                return self.data

        return {"Body": Body(self.objects[(Bucket, Key)])}

    def put_object(self, *, Bucket, Key, Body, ContentType):
        """Keep the latest durable checkpoint in memory."""
        self.objects[(Bucket, Key)] = Body


class DeltaChunkedRebuildTests(unittest.TestCase):
    """Exercise the recovery boundary and keyed Gold helper publication."""

    def test_retry_resumes_after_completed_window(self):
        """A failed window reruns while completed Silver work is retained."""
        client = FakeClient()
        calls = []

        def fail_once(layer, start, end, first):
            """Fail one Silver window after its predecessor checkpoint."""
            calls.append((layer, start, first))
            if len(calls) == 2:
                raise RuntimeError("interrupted")

        environment = {
            "LOGICAL_DATE": "2025-12-31",
            "REBUILD_START_DATE": "2025-12-01",
            "REBUILD_WINDOW_DAYS": "14",
            "REBUILD_RUN_ID": "manual__2025-12-31T00:00:00+00:00",
        }
        with patch.dict(os.environ, environment), patch.object(
            rebuild, "checkpoint_client", return_value=client
        ), patch.object(rebuild, "run_window", side_effect=fail_once):
            with self.assertRaisesRegex(RuntimeError, "interrupted"):
                rebuild.main()

        self.assertEqual(calls[:2], [
            ("silver", "2025-12-01", True),
            ("silver", "2025-12-15", False),
        ])
        with patch.dict(os.environ, environment), patch.object(
            rebuild, "checkpoint_client", return_value=client
        ), patch.object(rebuild, "run_window", side_effect=fail_once):
            rebuild.main()

        self.assertEqual(calls.count(("silver", "2025-12-01", True)), 1)
        self.assertEqual(calls[-3:], [
            ("gold", "2025-12-01", True),
            ("gold", "2025-12-15", False),
            ("gold", "2025-12-29", False),
        ])
        self.assertEqual(len(client.objects), 1)
        state = json.loads(next(iter(client.objects.values())))
        self.assertEqual(len(state["silver_done"]), 3)
        self.assertEqual(len(state["gold_done"]), 3)

    def test_gold_delivery_cost_window_merge_retains_other_orders(self):
        """Later Gold windows keep delivery costs from prior windows."""
        with tempfile.TemporaryDirectory() as temp_dir:
            target = str(Path(temp_dir) / "dim_delivery_cost")
            write_deltalake(
                target,
                pa.table({"order_id": [1, 2], "tariff": [10, 20]}),
                mode="overwrite",
            )
            connection = duckdb.connect()
            try:
                connection.execute(
                    "CREATE TABLE staged AS SELECT * FROM "
                    "(VALUES (2, 21), (3, 30)) t(order_id, tariff)"
                )
                with patch.dict(os.environ, {
                    "GOLD_WINDOW_START": date(2025, 12, 15).isoformat(),
                    "GOLD_WINDOW_END": date(2025, 12, 29).isoformat(),
                }), patch.object(publisher, "validate_publish_contract"), patch.object(
                    publisher, "prefix_has_delta_log", return_value=True
                ):
                    publisher.publish_replacement_model(
                        connection, None, "unused", "unused", {}, target,
                        "gold", "dim_delivery_cost_mart", "dim_delivery_cost",
                        "staged", "_gold_partition_date", "daily_refresh",
                    )
                result = DeltaTable(target).to_pyarrow_table().to_pydict()
                self.assertEqual(
                    dict(zip(result["order_id"], result["tariff"], strict=True)),
                    {1: 10, 2: 21, 3: 30},
                )
            finally:
                connection.close()


if __name__ == "__main__":
    unittest.main()
