"""CLI inputs for the isolated Iceberg Bronze application."""

from __future__ import annotations

import argparse

from etl_utils import parse_date


def parse_iceberg_bronze_args() -> argparse.Namespace:
    """Read Raw selection and Lakekeeper settings from the SparkApplication."""
    parser = argparse.ArgumentParser(description="Apply Raw batches to Iceberg Bronze")
    parser.add_argument("--tables", default="")
    parser.add_argument("--table-config", default="")
    parser.add_argument("--groups-config", default="")
    parser.add_argument("--schema", default="source")
    parser.add_argument("--run-date", type=parse_date)
    parser.add_argument("--mode", default="snapshot")
    parser.add_argument("--partition-key", default="snapshot_date")
    parser.add_argument("--event-date-column", default="")
    parser.add_argument("--lookback-days", type=int, default=0)
    parser.add_argument("--raw-bucket", default="ampere-raw")
    parser.add_argument("--raw-prefix", default="postgres-pre-raw")
    parser.add_argument("--source-system", default="postgres-pre-raw")
    parser.add_argument("--shuffle-partitions", type=int, default=0)
    parser.add_argument("--iceberg-catalog", default="iceberg_bronze")
    parser.add_argument("--iceberg-bronze-schema", default="bronze")
    parser.add_argument("--iceberg-ops-schema", default="ops")
    parser.add_argument("--lakekeeper-warehouse", default="ampere-bronze")
    parser.add_argument("--lakekeeper-uri", required=True)
    parser.add_argument("--lakekeeper-oauth-uri", required=True)
    parser.add_argument("--lakekeeper-scope", required=True)
    parser.add_argument("--app-name", default="raw-to-iceberg-bronze")
    parser.add_argument("--image", default="")
    return parser.parse_args()
