"""Check that Delta daily publication retains untouched historical rows."""

from __future__ import annotations

import importlib.util
import sys
import tempfile
import unittest
from datetime import date
from pathlib import Path

import duckdb
import pyarrow as pa
from deltalake import DeltaTable, write_deltalake


SCRIPT_DIR = Path(__file__).resolve().parents[1] / "docker/dbt/scripts"
sys.path.insert(0, str(SCRIPT_DIR))
SPEC = importlib.util.spec_from_file_location(
    "publish_silver_tables", SCRIPT_DIR / "publish_silver_tables.py"
)
publisher = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(publisher)


class DeltaDailyPublishTests(unittest.TestCase):
    """Exercise the real delta-rs merge on a small partitioned table."""

    def test_daily_merge_preserves_same_date_history(self) -> None:
        """An old order untouched by the slice survives repeated publishes."""
        with tempfile.TemporaryDirectory() as temp_dir:
            target = str(Path(temp_dir) / "fact_orders")
            baseline = pa.table(
                {
                    "order_id": [1, 2],
                    "order_date": [date(2025, 12, 16)] * 2,
                    "total_amount": [10, 20],
                    "_silver_partition_date": [date(2025, 12, 16)] * 2,
                }
            )
            write_deltalake(
                target, baseline, mode="overwrite", partition_by=["_silver_partition_date"]
            )
            con = duckdb.connect()
            try:
                con.execute(
                    "CREATE TABLE staged_fact_orders AS SELECT * FROM "
                    "(VALUES (1, DATE '2025-12-16', 11), "
                    "(3, DATE '2025-12-16', 30)) "
                    "AS t(order_id, order_date, total_amount)"
                )
                for _ in range(2):
                    publisher.merge_partitioned_delta(
                        con, target, "staged_fact_orders", "order_date",
                        "_silver_partition_date", "2025-12-16", ("order_id",), {},
                    )
                actual = (
                    DeltaTable(target)
                    .to_pyarrow_table()
                    .select(["order_id", "total_amount"])
                    .to_pydict()
                )
                self.assertEqual(
                    dict(zip(actual["order_id"], actual["total_amount"], strict=True)),
                    {1: 11, 2: 20, 3: 30},
                )
            finally:
                con.close()


if __name__ == "__main__":
    unittest.main()
