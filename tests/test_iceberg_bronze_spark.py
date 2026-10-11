"""Exercise the Bronze Iceberg write paths with a local Spark catalog."""

from __future__ import annotations

import logging
import os
import sys
import tempfile
import unittest
from datetime import date, datetime, timezone
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
from iceberg_bronze.journal_apply import (  # noqa: E402
    SOURCE_ROW_BUCKETS,
    _apply_batch_slices,
    _apply_full_rebuild,
    _bronze_delete_rows,
    _full_rebuild_transfer_id,
    _raw_source_column_for_bronze,
    reserve_full_rebuild,
    reserve_incremental_transfer,
)
from iceberg_bronze.main import (  # noqa: E402
    _candidate_runs,
    _processed_batches,
    _registry_progress,
    _search_start_date,
)
from iceberg_bronze.snapshots import apply_snapshot_batches  # noqa: E402
from initialize_iceberg_catalog import _create_table  # noqa: E402
from tools.contracts.ampere_contract import load_contract  # noqa: E402
from dags.utils.stream_group_config import (  # noqa: E402
    build_bronze_execution_groups,
    build_bronze_stream_groups,
)


class BronzeExecutionGroupTests(unittest.TestCase):
    """Keep the two Spark applications ordered and cover every source table."""

    def test_snapshots_and_mutable_dims_precede_facts_and_events(self) -> None:
        groups = build_bronze_execution_groups()
        self.assertEqual(
            [group["name"] for group in groups],
            ["snapshots_mutable_dims", "facts_events"],
        )
        self.assertFalse(groups[0]["complete_transfer"])
        self.assertTrue(groups[1]["complete_transfer"])
        self.assertEqual(
            set(groups[0]["tables"]),
            set(build_bronze_stream_tables("snapshots", "mutable_dims")),
        )
        self.assertEqual(
            set(groups[1]["tables"]),
            set(build_bronze_stream_tables("facts", "events")),
        )
        all_tables = [table for group in groups for table in group["tables"]]
        self.assertEqual(len(all_tables), len(set(all_tables)))
        contract_tables = {table.name for table in load_contract().layer_tables("raw")}
        self.assertEqual(set(all_tables), contract_tables)


def build_bronze_stream_tables(*groups: str) -> list[str]:
    configured = {
        group["group"]: group["tables"]
        for group in build_bronze_stream_groups()
    }
    return [table for group in groups for table in configured[group]]


class IcebergRegistryTests(unittest.TestCase):
    """Keep failed historical batches visible after newer successes."""

    def test_full_rebuild_retry_reuses_started_transfer_boundary(self) -> None:
        connection = MagicMock()
        connection.__enter__.return_value = connection
        cursor = MagicMock()
        connection.cursor.return_value.__enter__.return_value = cursor
        cursor.fetchone.side_effect = [
            ("51e4343e-cf56-4181-9796-3f27865ebe2b",),
            None,
            ("rebuild-id", 1, 37),
        ]
        cursor.fetchall.side_effect = [
            [("raw-transfer-1",), ("raw-transfer-2",)],
            [("clients", 24)],
            [("raw-transfer-1", "batch-1", 1), ("raw-transfer-2", "batch-2", 37)],
            [("batch-1", "clients", 10), ("batch-2", "clients", 14)],
        ]
        with patch("iceberg_bronze.journal_apply._pg_connect", return_value=connection):
            transfer = reserve_full_rebuild()

        self.assertEqual(transfer["transfer_id"], "rebuild-id")
        self.assertEqual(transfer["first_sequence"], 1)
        self.assertEqual(transfer["last_sequence"], 37)
        self.assertEqual(
            transfer["transfer_ids"], ["raw-transfer-1", "raw-transfer-2"]
        )
        self.assertEqual(transfer["expected_counts"], {"clients": 24})
        self.assertEqual([batch["batch_sequence"] for batch in transfer["batches"]], [1, 37])
        self.assertEqual(transfer["batch_counts"]["batch-1"]["clients"], 10)
        self.assertTrue(transfer["rebuild"])

    def test_daily_apply_waits_for_started_full_rebuild(self) -> None:
        connection = MagicMock()
        connection.__enter__.return_value = connection
        cursor = MagicMock()
        connection.cursor.return_value.__enter__.return_value = cursor
        cursor.fetchone.side_effect = [
            (True, "51e4343e-cf56-4181-9796-3f27865ebe2b", 37),
            ("51e4343e-cf56-4181-9796-3f27865ebe2b",),
            (1,),
        ]
        with (
            patch("iceberg_bronze.journal_apply._pg_connect", return_value=connection),
            self.assertRaisesRegex(RuntimeError, "full rebuild is in progress"),
        ):
            reserve_incremental_transfer()

    def test_full_rebuild_waits_for_started_daily_transfer(self) -> None:
        connection = MagicMock()
        connection.__enter__.return_value = connection
        cursor = MagicMock()
        connection.cursor.return_value.__enter__.return_value = cursor
        cursor.fetchone.side_effect = [
            ("51e4343e-cf56-4181-9796-3f27865ebe2b",),
            (1,),
        ]
        with (
            patch("iceberg_bronze.journal_apply._pg_connect", return_value=connection),
            self.assertRaisesRegex(RuntimeError, "daily Bronze transfer is in progress"),
        ):
            reserve_full_rebuild()

    def test_bronze_lineage_columns_map_to_raw_journal_names(self) -> None:
        self.assertEqual(
            _raw_source_column_for_bronze("_source_batch_sequence"),
            "_batch_sequence",
        )
        self.assertEqual(
            _raw_source_column_for_bronze("_source_change_id"),
            "_change_id",
        )
        self.assertEqual(_raw_source_column_for_bronze("_dataset_id"), "_dataset_id")

    def test_corrected_full_rebuild_gets_new_stable_acknowledgment_identity(self) -> None:
        import uuid

        dataset_id = "51e4343e-cf56-4181-9796-3f27865ebe2b"
        rebuilt = _full_rebuild_transfer_id(dataset_id, 37)
        prior = uuid.uuid5(
            uuid.NAMESPACE_URL,
            f"ampere-bronze-rebuild:{dataset_id}:37:contract-v6",
        )

        self.assertEqual(rebuilt, _full_rebuild_transfer_id(dataset_id, 37))
        self.assertNotEqual(rebuilt, prior)

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
            self.assertEqual(
                progress["payments"].earliest_failed_partition_value, "2025-12-16"
            )
            self.assertEqual(progress["payments"].latest_contract_version, "v2")
            self.assertIsNone(progress["delivery_tracking"].latest_partition_value)
            self.assertEqual(
                progress["delivery_tracking"].earliest_failed_partition_value,
                "2025-12-16",
            )
        finally:
            spark.stop()

    def test_superseded_snapshot_registry_rows_remain_replayable(self) -> None:
        """Only applied or terminal skips suppress replay of a Raw batch."""
        spark = (
            SparkSession.builder.master("local[2]")
            .appName("iceberg-registry-replay")
            .getOrCreate()
        )
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
                "files": [
                    {
                        "path": f"s3a://raw/{run_id}.parquet",
                        "size_bytes": 1,
                        "row_count": 1,
                        "checksum": "checksum",
                    }
                ],
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
        self.assertEqual(
            spark.read.parquet.call_args_list[0].args[0], "s3a://raw/retry.parquet"
        )
        self.assertEqual(
            spark.read.parquet.call_args_list[1].args[0], "s3a://raw/next-day.parquet"
        )
        self.assertCountEqual(
            [(row["run_id"], row["status"], row["details"]) for row in registry_rows],
            [
                ("old", "skipped", "superseded by latest snapshot"),
                ("retry", "applied", "ok"),
                ("next-day", "applied", "ok"),
            ],
        )

    def test_snapshot_full_rebuild_replaces_table(self) -> None:
        """A full rebuild replaces old daily partitions with the latest batch."""
        spark = MagicMock()
        source = MagicMock()
        source.withColumn.return_value = source
        writer = MagicMock()
        source.writeTo.return_value = writer
        spark.read.parquet.return_value = source
        manifest = {
            "manifest_version": 1,
            "source_system": "postgres-pre-raw",
            "source_schema": "source",
            "source_table": "stores",
            "contract_name": "ampere",
            "contract_version": "v3",
            "run_id": "latest",
            "ingest_ts_utc": "2026-10-10T05:00:00+00:00",
            "batch_type": "snapshot",
            "storage_format": "parquet",
            "schema_hash": "hash",
            "checksum": "checksum",
            "file_count": 1,
            "row_count": 1,
            "files": [
                {
                    "path": "s3a://raw/latest.parquet",
                    "size_bytes": 1,
                    "row_count": 1,
                    "checksum": "checksum",
                }
            ],
            "checks": [],
            "min_max": {},
            "null_counts": {},
            "producer": {},
            "source_extract": {},
            "snapshot_date": "2026-10-10",
        }
        batch = {
            "manifest": manifest,
            "manifest_path": "s3a://raw/latest/_manifest.json",
            "partition_kind": "snapshot_date",
            "partition_value": "2026-10-10",
            "run_id": "latest",
            "ingest_ts_utc": "2026-10-10T05:00:00+00:00",
        }

        with patch("iceberg_bronze.snapshots.F.lit", side_effect=lambda value: value):
            apply_snapshot_batches(
                spark=spark,
                table="stores",
                bronze_table_name="iceberg_bronze.bronze.stores",
                registry_schema=None,
                registry_rows=[],
                source_system="postgres-pre-raw",
                source_schema="source",
                sorted_batches=[batch],
                expected_schema_hash=None,
                expected_contract_version=None,
                logger=logging.getLogger("snapshot-test"),
                replace_table=True,
            )

        writer.overwrite.assert_called_once_with(True)
        writer.overwritePartitions.assert_not_called()

    def test_snapshot_full_rebuild_discovers_only_latest_partition(self) -> None:
        spark = MagicMock()
        with (
            patch(
                "iceberg_bronze.main._list_partitions",
                return_value=[date(2026, 10, 8), date(2026, 10, 9), date(2026, 10, 10)],
            ),
            patch("iceberg_bronze.main.list_dirs", return_value=["run_id=latest"]),
            patch("iceberg_bronze.main.exists", return_value=True),
        ):
            candidates = _candidate_runs(
                spark,
                "s3a://raw/stores",
                "snapshot_date",
                None,
                latest_only=True,
            )

        self.assertEqual(len(candidates), 1)
        self.assertEqual(candidates[0]["partition_value"], "2026-10-10")

    def test_mutable_daily_search_uses_raw_state_and_registry_recovery_window(
        self,
    ) -> None:
        raw_state_ingest = datetime(2026, 10, 10, 5, tzinfo=timezone.utc)
        start = _search_start_date(
            "extract_date",
            date(2026, 10, 8),
            raw_state_ingest,
            date(2026, 10, 10),
            0,
            has_registry_rows=True,
        )

        self.assertEqual(start, date(2026, 10, 7))

    def test_mutable_merge_guards_against_older_extract(self) -> None:
        """Build the source-date guard independently of the runtime JAR."""
        spark = MagicMock()
        source = MagicMock()
        source.columns = ["id", "fullname", "_bronze_last_manifest_path"]
        merge_to_iceberg(
            spark,
            source,
            "iceberg_bronze.bronze.clients",
            ["id"],
            source_extract_date="2026-07-09",
        )
        sql = spark.sql.call_args.args[0]
        self.assertIn("WHEN MATCHED AND", sql)
        self.assertIn("extract_date=([0-9]{4}-[0-9]{2}-[0-9]{2})", sql)
        self.assertIn("<= '2026-07-09'", sql)


class BoundedBronzeReplayTests(unittest.TestCase):
    def test_delete_metadata_maps_raw_ordering_columns_to_bronze_names(self) -> None:
        spark = SparkSession.builder.master("local[2]").appName("bronze-delete-lineage").getOrCreate()
        spark.sparkContext.setLogLevel("ERROR")
        try:
            changes = spark.createDataFrame(
                [("dataset", "entity-a", 17, 42, "D")],
                ["_dataset_id", "_source_row_id", "_batch_sequence", "_change_id", "_op"],
            )
            deletes = _bronze_delete_rows(changes)
            self.assertEqual(
                deletes.columns,
                ["_dataset_id", "_source_row_id", "_source_batch_sequence", "_source_change_id"],
            )
            self.assertEqual(
                list(deletes.select("_source_batch_sequence", "_source_change_id").first()),
                [17, 42],
            )
        finally:
            spark.stop()

    def test_source_row_hash_buckets_keep_entity_events_together(self) -> None:
        spark = SparkSession.builder.master("local[2]").appName("bounded-bronze-buckets").getOrCreate()
        spark.sparkContext.setLogLevel("ERROR")
        try:
            frame = spark.createDataFrame(
                [
                    ("dataset", "batch", 1, 1, "entity-a", "I"),
                    ("dataset", "batch", 1, 2, "entity-a", "D"),
                    ("dataset", "batch", 1, 3, "entity-b", "I"),
                ],
                ["_dataset_id", "_batch_id", "_batch_sequence", "_change_id", "_source_row_id", "_op"],
            )
            spark.table = MagicMock(return_value=frame)
            transfer = {
                "dataset_id": "dataset",
                "transfer_id": "rebuild",
                "batch_counts": {"batch": {"clients": 3}},
            }
            batch = {"batch_id": "batch", "batch_sequence": 1}
            seen = []

            def record(_spark, _target, _table, changes, _transfer):
                seen.append(changes.select("_source_row_id", "_change_id").collect())

            with patch("iceberg_bronze.journal_apply._merge_incremental", side_effect=record):
                count = _apply_batch_slices(spark, "bronze.clients", "clients", transfer, batch)

            self.assertEqual(count, 3)
            self.assertEqual(len(seen), 2)
            buckets = {tuple(sorted(row._source_row_id for row in group)) for group in seen}
            self.assertEqual(buckets, {("entity-a", "entity-a"), ("entity-b",)})
            self.assertGreaterEqual(SOURCE_ROW_BUCKETS, len(seen))
        finally:
            spark.stop()

    def test_full_rebuild_replays_batches_in_order_after_table_clear(self) -> None:
        spark = MagicMock()
        transfer = {
            "batches": [
                {"batch_id": "first", "batch_sequence": 2},
                {"batch_id": "second", "batch_sequence": 8},
            ],
            "expected_counts": {"clients": 3},
        }
        with patch(
            "iceberg_bronze.journal_apply._apply_batch_slices",
            side_effect=[1, 2],
        ) as apply:
            total = _apply_full_rebuild(spark, "bronze.clients", "clients", transfer)

        spark.sql.assert_called_once_with("DELETE FROM bronze.clients")
        self.assertEqual(total, 3)
        self.assertEqual([call.args[-1]["batch_sequence"] for call in apply.call_args_list], [2, 8])


@unittest.skipUnless(
    os.getenv("ICEBERG_RUNTIME_JAR"), "Iceberg runtime JAR not supplied"
)
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
                    source = spark.createDataFrame([(7, fullname)], ["id", "fullname"])
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
                    [
                        (row.id, row.fullname)
                        for row in spark.table(dimension_table).collect()
                    ],
                    [(7, "second")],
                )

                # A later retry of an older extract must not revert a client
                # already updated by a newer Raw partition.
                for extract_date, fullname in (
                    ("2026-08-12", "august"),
                    ("2026-07-09", "stale-july"),
                ):
                    source = spark.createDataFrame(
                        [
                            (
                                7,
                                fullname,
                                f"s3a://raw/clients/extract_date={extract_date}/run_id=x/_manifest.json",
                            )
                        ],
                        ["id", "fullname", "_bronze_last_manifest_path"],
                    )
                    aligned = align_df_to_iceberg_schema(
                        spark,
                        source,
                        catalog="iceberg_bronze",
                        schema="bronze",
                        table="clients",
                        logger=logger,
                    )
                    merge_to_iceberg(
                        spark,
                        aligned,
                        dimension_table,
                        ["id"],
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
