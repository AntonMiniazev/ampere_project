"""Exercise the Bronze Iceberg write paths with a local Spark catalog."""

from __future__ import annotations

import logging
import os
import sys
import tempfile
import unittest
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "docker/spark/iceberg_raw_etl/app"))
sys.path.insert(0, str(ROOT / "docker/spark/raw_etl/app"))
os.environ["ICEBERG_CONTRACT_PATH"] = str(ROOT / "tools/uc/contracts/ampere_tables.json")

from iceberg_bronze.apply_utils import merge_to_iceberg  # noqa: E402
from iceberg_bronze.catalog import align_df_to_iceberg_schema, ensure_iceberg_table  # noqa: E402
from iceberg_bronze.facts_events import stabilize_merge_source  # noqa: E402
from iceberg_bronze.main import _registry_progress  # noqa: E402


class IcebergRegistryTests(unittest.TestCase):
    """Keep failed historical batches visible after newer successes."""

    def test_unresolved_failures_anchor_landing_search(self) -> None:
        """Exclude retried successes but retain older failed partitions."""
        spark = SparkSession.builder.master("local[2]").appName("iceberg-registry").getOrCreate()
        spark.sparkContext.setLogLevel("ERROR")
        try:
            history = spark.sql(
                "SELECT * FROM VALUES "
                "('payments','old','2025-12-16','failed','v1','2026-10-04T00:00:00'),"
                "('payments','resolved','2026-01-01','failed','v1','2026-10-04T00:00:00'),"
                "('payments','resolved','2026-01-01','applied','v1','2026-10-05T00:00:00'),"
                "('payments','new','2026-09-29','applied','v2','2026-10-05T01:00:00'),"
                "('delivery_tracking','old','2025-12-16','failed','v1','2026-10-04T00:00:00') "
                "AS t(source_table,run_id,partition_value,status,contract_version,apply_ts_utc)"
            )
            progress = _registry_progress(history)
            self.assertEqual(progress["payments"].latest_partition_value, "2026-09-29")
            self.assertEqual(progress["payments"].earliest_failed_partition_value, "2025-12-16")
            self.assertEqual(progress["payments"].latest_contract_version, "v2")
            self.assertIsNone(progress["delivery_tracking"].latest_partition_value)
            self.assertEqual(progress["delivery_tracking"].earliest_failed_partition_value, "2025-12-16")
        finally:
            spark.stop()


@unittest.skipUnless(os.getenv("ICEBERG_RUNTIME_JAR"), "Iceberg runtime JAR not supplied")
class IcebergSparkTests(unittest.TestCase):
    """Check snapshot replacement and mutable-dimension merge against Iceberg."""

    def test_snapshot_overwrite_and_dimension_merge(self) -> None:
        """Run both write strategies on Iceberg tables with the real schema."""
        logger = logging.getLogger(__name__)
        with tempfile.TemporaryDirectory() as temp_dir:
            warehouse = Path(temp_dir).as_uri()
            spark = (
                SparkSession.builder.master("local[2]")
                .appName("ampere-iceberg-smoke")
                .config("spark.jars", os.environ["ICEBERG_RUNTIME_JAR"])
                .config(
                    "spark.sql.extensions",
                    "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
                )
                .getOrCreate()
            )
            spark.sparkContext.setLogLevel("ERROR")
            try:
                # The production app sets its REST catalog after Spark starts,
                # before the first catalog resolution. Check the same timing.
                spark.conf.set(
                    "spark.sql.catalog.iceberg_bronze",
                    "org.apache.iceberg.spark.SparkCatalog",
                )
                spark.conf.set("spark.sql.catalog.iceberg_bronze.type", "hadoop")
                spark.conf.set("spark.sql.catalog.iceberg_bronze.warehouse", warehouse)
                snapshot_table = ensure_iceberg_table(
                    spark,
                    catalog="iceberg_bronze",
                    schema="bronze",
                    table="assortment",
                    logger=logger,
                )
                for product in (1, 2):
                    source = spark.createDataFrame(
                        [(product, 10, "2026-10-04")],
                        ["product_id", "store_id", "snapshot_date"],
                    )
                    aligned = align_df_to_iceberg_schema(
                        spark,
                        source,
                        catalog="iceberg_bronze",
                        schema="bronze",
                        table="assortment",
                        logger=logger,
                    )
                    aligned.writeTo(snapshot_table).overwritePartitions()
                self.assertEqual(
                    [row.product_id for row in spark.table(snapshot_table).collect()],
                    [2],
                )

                dimension_table = ensure_iceberg_table(
                    spark,
                    catalog="iceberg_bronze",
                    schema="bronze",
                    table="clients",
                    logger=logger,
                )
                for fullname in ("first", "second"):
                    source = spark.createDataFrame(
                        [(7, fullname)], ["id", "fullname"]
                    )
                    aligned = align_df_to_iceberg_schema(
                        spark,
                        source,
                        catalog="iceberg_bronze",
                        schema="bronze",
                        table="clients",
                        logger=logger,
                    )
                    merge_to_iceberg(spark, aligned, dimension_table, ["id"])
                self.assertEqual(
                    [(row.id, row.fullname) for row in spark.table(dimension_table).collect()],
                    [(7, "second")],
                )

                # Raw lineage uses input_file_name(), which Spark treats as a
                # non-deterministic source for Iceberg MERGE. The checkpointed
                # source must still merge with static partition pruning.
                payments_table = ensure_iceberg_table(
                    spark,
                    catalog="iceberg_bronze",
                    schema="bronze",
                    table="payments",
                    logger=logger,
                )
                payment_source = spark.sql(
                    "SELECT 9 AS order_id, DATE '2026-10-04' AS payment_date, "
                    "'2026-10-04' AS event_date"
                ).withColumn("method", F.rand().cast("string"))
                aligned_payment = align_df_to_iceberg_schema(
                    spark,
                    payment_source,
                    catalog="iceberg_bronze",
                    schema="bronze",
                    table="payments",
                    logger=logger,
                )
                merge_to_iceberg(
                    spark,
                    stabilize_merge_source(aligned_payment),
                    payments_table,
                    ["order_id", "payment_date"],
                    partition_column="event_date",
                    partition_values=["2026-10-04"],
                )
                self.assertEqual(spark.table(payments_table).count(), 1)
            finally:
                spark.stop()


if __name__ == "__main__":
    unittest.main()
