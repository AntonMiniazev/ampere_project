"""Check that repeated Raw extracts produce one Iceberg MERGE source row."""

from __future__ import annotations

import os
import sys
import unittest
from pathlib import Path

from pyspark.sql import SparkSession


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "docker" / "spark" / "iceberg_raw_etl" / "app"))
sys.path.insert(0, str(ROOT / "docker" / "spark" / "raw_etl" / "app"))

from iceberg_bronze.mutable_dims import latest_rows_by_merge_key  # noqa: E402


class MutableDimensionDedupTests(unittest.TestCase):
    """Exercise overlapping Raw runs before Iceberg MERGE."""

    @classmethod
    def setUpClass(cls) -> None:
        """Start one local Spark session for the source selection check."""
        os.environ["PYSPARK_PYTHON"] = sys.executable
        cls.spark = (
            SparkSession.builder.master("local[1]")
            .appName("iceberg-mutable-dedup-test")
            .config("spark.ui.enabled", "false")
            .getOrCreate()
        )

    @classmethod
    def tearDownClass(cls) -> None:
        """Release the local Spark session."""
        cls.spark.stop()

    def test_latest_raw_run_wins_per_business_key(self) -> None:
        """Preserve one client row when two extracts cover the same date."""
        source = self.spark.sql(
            """
            SELECT * FROM VALUES
              ('1', 'old', '2026-04-21T10:00:00Z', 'run-1', 'first.json'),
              ('1', 'new', '2026-04-21T11:00:00Z', 'run-2', 'second.json'),
              ('2', 'only', '2026-04-21T10:00:00Z', 'run-1', 'first.json')
            AS source(id, payload, _bronze_last_apply_ts,
                      _bronze_last_run_id, _bronze_last_manifest_path)
            """
        )

        actual = {
            row.id: row.payload
            for row in latest_rows_by_merge_key(source, ["id"]).collect()
        }

        self.assertEqual(actual, {"1": "new", "2": "only"})


if __name__ == "__main__":
    unittest.main()
