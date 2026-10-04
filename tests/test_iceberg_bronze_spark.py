"""Exercise the Bronze Iceberg write paths with a local Spark catalog."""

from __future__ import annotations

import logging
import os
import sys
import tempfile
import unittest
from pathlib import Path

from pyspark.sql import SparkSession


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "docker/spark/iceberg_raw_etl/app"))
os.environ["ICEBERG_CONTRACT_PATH"] = str(ROOT / "tools/uc/contracts/ampere_tables.json")

from iceberg_bronze.apply_utils import merge_to_iceberg  # noqa: E402
from iceberg_bronze.catalog import align_df_to_iceberg_schema, ensure_iceberg_table  # noqa: E402


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
                .config(
                    "spark.sql.catalog.iceberg_bronze",
                    "org.apache.iceberg.spark.SparkCatalog",
                )
                .config("spark.sql.catalog.iceberg_bronze.type", "hadoop")
                .config("spark.sql.catalog.iceberg_bronze.warehouse", warehouse)
                .getOrCreate()
            )
            spark.sparkContext.setLogLevel("ERROR")
            try:
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
            finally:
                spark.stop()


if __name__ == "__main__":
    unittest.main()
