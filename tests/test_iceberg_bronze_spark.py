"""Exercise the Bronze Iceberg write paths with a local Spark catalog."""

from __future__ import annotations

import logging
import os
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "docker/spark/iceberg_raw_etl/app"))
sys.path.insert(0, str(ROOT / "docker/spark/connect_client/app"))
sys.path.insert(0, str(ROOT))
os.environ["ICEBERG_CONTRACT_PATH"] = str(ROOT / "tools/contracts/ampere_tables.json")

from iceberg_bronze.apply_utils import merge_to_iceberg  # noqa: E402
from iceberg_bronze.catalog import align_df_to_iceberg_schema, ensure_iceberg_table  # noqa: E402
from iceberg_bronze.facts_events import stabilize_merge_source  # noqa: E402
from iceberg_bronze.main import _processed_batches, _registry_progress  # noqa: E402
from iceberg_bronze.snapshots import apply_snapshot_batches  # noqa: E402
from initialize_iceberg_catalog import _create_table  # noqa: E402
from tools.contracts.ampere_contract import load_contract  # noqa: E402


class IcebergRegistryTests(unittest.TestCase):
    """Keep failed historical batches visible after newer successes."""

    def test_unresolved_failures_anchor_landing_search(self) -> None:
        """Exclude retried successes but retain older failed partitions."""
        builder = SparkSession.builder.master("local[2]").appName("iceberg-registry")
        if os.getenv("ICEBERG_RUNTIME_JAR"):
            builder = builder.config("spark.jars", os.environ["ICEBERG_RUNTIME_JAR"])
            builder = builder.config(
                "spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
            )
        spark = builder.getOrCreate()
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

    def test_superseded_snapshot_registry_rows_remain_replayable(self) -> None:
        """Only applied or terminal skips suppress replay of a Raw batch."""
        spark = SparkSession.builder.master("local[2]").appName("iceberg-registry-replay").getOrCreate()
        spark.sparkContext.setLogLevel("ERROR")
        try:
            history = spark.sql(
                "SELECT * FROM VALUES "
                "('assortment','applied','2026-10-01','applied','ok'),"
                "('assortment','empty','2026-10-02','skipped','empty batch'),"
                "('assortment','old','2026-10-03','skipped','superseded by latest snapshot'),"
                "('clients','other','2026-10-04','applied','ok') "
                "AS t(source_table,run_id,partition_value,status,details)"
            )
            processed = _processed_batches(
                history, "assortment", ["applied", "empty", "old", "other"]
            )
            self.assertEqual(
                processed,
                {("applied", "2026-10-01"), ("empty", "2026-10-02")},
            )
        finally:
            spark.stop()

    def test_snapshot_rebuild_keeps_each_date_and_latest_retry(self) -> None:
        """Apply each snapshot date, selecting only the latest run per date."""
        spark = MagicMock()
        batches = []
        for partition_value, run_id, ingest_ts in (
            ("2026-10-01", "old", "2026-10-01T05:00:00+00:00"),
            ("2026-10-01", "retry", "2026-10-01T06:00:00+00:00"),
            ("2026-10-02", "next-day", "2026-10-02T05:00:00+00:00"),
        ):
            manifest = {
                "manifest_version": 1,
                "source_system": "postgres-pre-raw",
                "source_schema": "source",
                "source_table": "assortment",
                "contract_name": "ampere",
                "contract_version": "v3",
                "run_id": run_id,
                "ingest_ts_utc": ingest_ts,
                "batch_type": "snapshot",
                "storage_format": "parquet",
                "schema_hash": "hash",
                "checksum": "checksum",
                "file_count": 1,
                "row_count": 1,
                "files": [{"path": f"s3a://raw/{run_id}.parquet", "size_bytes": 1, "row_count": 1, "checksum": "checksum"}],
                "checks": [],
                "min_max": {},
                "null_counts": {},
                "producer": {},
                "source_extract": {},
                "snapshot_date": partition_value,
            }
            batches.append(
                {
                    "manifest": manifest,
                    "manifest_path": f"s3a://raw/{run_id}/_manifest.json",
                    "partition_kind": "snapshot_date",
                    "partition_value": partition_value,
                    "run_id": run_id,
                    "ingest_ts_utc": ingest_ts,
                }
            )

        registry_rows = []
        with patch("iceberg_bronze.snapshots.F.lit", side_effect=lambda value: value):
            apply_snapshot_batches(
                spark=spark,
                table="assortment",
                bronze_table_name="iceberg_bronze.bronze.assortment",
                registry_schema=None,
                registry_rows=registry_rows,
                source_system="postgres-pre-raw",
                source_schema="source",
                sorted_batches=batches,
                expected_schema_hash=None,
                expected_contract_version=None,
                logger=logging.getLogger("snapshot-test"),
            )

        self.assertEqual(spark.read.parquet.call_count, 2)
        self.assertEqual(spark.read.parquet.call_args_list[0].args[0], "s3a://raw/retry.parquet")
        self.assertEqual(spark.read.parquet.call_args_list[1].args[0], "s3a://raw/next-day.parquet")
        self.assertCountEqual(
            [(row["run_id"], row["status"], row["details"]) for row in registry_rows],
            [
                ("old", "skipped", "superseded by latest snapshot"),
                ("retry", "applied", "ok"),
                ("next-day", "applied", "ok"),
            ],
        )

    def test_mutable_merge_guards_against_older_extract(self) -> None:
        """Build the source-date guard independently of the runtime JAR."""
        spark = MagicMock()
        source = MagicMock()
        source.columns = ["id", "fullname", "_bronze_last_manifest_path"]
        merge_to_iceberg(
            spark, source, "iceberg_bronze.bronze.clients", ["id"],
            source_extract_date="2026-07-09",
        )
        sql = spark.sql.call_args.args[0]
        self.assertIn("WHEN MATCHED AND", sql)
        self.assertIn("extract_date=([0-9]{4}-[0-9]{2}-[0-9]{2})", sql)
        self.assertIn("<= '2026-07-09'", sql)


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
                contract = load_contract(ROOT / "tools/contracts/ampere_tables.json")
                for name in ("assortment", "clients", "payments"):
                    _create_table(spark, contract, contract.table("bronze", name))
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

                # A later retry of an older extract must not revert a client
                # already updated by a newer Raw partition.
                for extract_date, fullname in (
                    ("2026-08-12", "august"),
                    ("2026-07-09", "stale-july"),
                ):
                    source = spark.createDataFrame(
                        [(7, fullname, f"s3a://raw/clients/extract_date={extract_date}/run_id=x/_manifest.json")],
                        ["id", "fullname", "_bronze_last_manifest_path"],
                    )
                    aligned = align_df_to_iceberg_schema(
                        spark, source, catalog="iceberg_bronze", schema="bronze",
                        table="clients", logger=logger,
                    )
                    merge_to_iceberg(
                        spark, aligned, dimension_table, ["id"],
                        source_extract_date=extract_date,
                    )
                client = spark.table(dimension_table).where("id = 7").first()
                self.assertEqual(client.fullname, "august")

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
