"""Load daily budget CSV to Unity Catalog silver.budget_orders_sales table.

Behavior:
- If incoming budget_name does not exist in target table: append rows.
- If incoming budget_name exists in target table: replace rows for that name.
"""

from __future__ import annotations

import argparse
import csv
import logging
import socket
import sys
from pathlib import Path
from urllib.parse import urlparse

from pyspark.sql import SparkSession

for candidate in [Path(__file__).resolve(), *Path(__file__).resolve().parents]:
    if (candidate / "pyproject.toml").exists():
        sys.path.insert(0, str(candidate))
        break

from tools.py_utils.spark_connect import configure_grpc_roots, quote_ident, spark_remote  # noqa: E402


APP_NAME = "load-budget-orders-sales-to-uc"
INPUT_CSV = Path(__file__).with_name("budget_parameters_daily.csv")
TARGET_TABLE = "ampere.silver.budget_orders_sales"
DEFAULT_BOOTSTRAP_LOCATION = "s3://ampere-silver/silver/budget_orders_sales"


def _parse_args() -> argparse.Namespace:
    """Parse CLI arguments for budget load runtime."""
    parser = argparse.ArgumentParser(
        description="Load budget_parameters_daily.csv into Unity Catalog Delta table."
    )
    parser.add_argument(
        "--spark-remote",
        default=spark_remote(),
        help="Spark Connect URI. Defaults to AMPERE_SPARK_REMOTE or local ingress.",
    )
    parser.add_argument(
        "--input-csv",
        default=str(INPUT_CSV),
        help=f"Input CSV path. Default: {INPUT_CSV}",
    )
    parser.add_argument(
        "--target-table",
        default=TARGET_TABLE,
        help=f"Target Unity Catalog table. Default: {TARGET_TABLE}",
    )
    parser.add_argument(
        "--connect-timeout-seconds",
        type=int,
        default=8,
        help="Socket timeout for Spark Connect endpoint precheck. Default: 8.",
    )
    parser.add_argument(
        "--bootstrap-location",
        default=DEFAULT_BOOTSTRAP_LOCATION,
        help=(
            "Delta location used to bootstrap table when UC metadata exists "
            "but path/log is missing."
        ),
    )
    return parser.parse_args()


def main() -> None:
    """Load CSV rows to UC table with replace-by-budget_name semantics."""
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s - %(message)s"
    )
    logger = logging.getLogger(APP_NAME)
    args = _parse_args()

    parsed = urlparse(args.spark_remote.replace("sc://", "http://", 1))
    host = parsed.hostname
    port = parsed.port
    if not host or not port:
        raise ValueError(f"Could not parse Spark Connect host/port from: {args.spark_remote}")
    try:
        with socket.create_connection((host, port), timeout=args.connect_timeout_seconds):
            logger.info("Spark Connect endpoint is reachable: %s:%s", host, port)
    except OSError as exc:
        raise ConnectionError(
            f"Cannot reach Spark Connect endpoint {host}:{port}. "
            "Pass --spark-remote with a reachable endpoint."
        ) from exc

    configure_grpc_roots()

    spark = SparkSession.builder.remote(args.spark_remote).appName(APP_NAME).getOrCreate()
    try:
        parts = args.target_table.split(".")
        if len(parts) != 3:
            raise ValueError(
                "Expected fully-qualified target table name: catalog.schema.table"
            )
        catalog, schema, table = parts
        catalog_sql = quote_ident(catalog)
        schema_sql = quote_ident(schema)
        table_like_sql = "'" + table.replace("'", "''") + "'"

        table_exists = (
            len(
                spark.sql(
                    f"SHOW TABLES IN {catalog_sql}.{schema_sql} LIKE {table_like_sql}"
                ).collect()
            )
            > 0
        )
        if not table_exists:
            raise ValueError(
                f"Target table does not exist in UC: {args.target_table}. "
                "Create it first before loading."
            )

        csv_path = Path(args.input_csv).resolve()
        if not csv_path.exists():
            raise FileNotFoundError(f"Input CSV not found: {csv_path}")

        logger.info("Reading CSV locally: %s", csv_path)
        with csv_path.open("r", encoding="utf-8-sig", newline="") as handle:
            reader = csv.DictReader(handle)
            csv_rows = list(reader)

        required_columns = {
            "budget_name",
            "budget_date",
            "store_id",
            "orders_budget_daily",
            "sales_amount_daily",
        }
        source_columns = set(csv_rows[0].keys()) if csv_rows else set()
        missing_columns = required_columns.difference(source_columns)
        if missing_columns:
            raise ValueError(
                f"Input CSV missing required columns: {sorted(missing_columns)}"
            )
        if not csv_rows:
            raise ValueError("Input CSV has no data rows.")

        cleaned_rows: list[dict[str, str]] = []
        for row in csv_rows:
            budget_name = (row.get("budget_name") or "").strip()
            budget_date = (row.get("budget_date") or "").strip()
            store_id = (row.get("store_id") or "").strip()
            orders_budget_daily = (row.get("orders_budget_daily") or "").strip()
            sales_amount_daily = (row.get("sales_amount_daily") or "").strip()
            if not all(
                [
                    budget_name,
                    budget_date,
                    store_id,
                    orders_budget_daily,
                    sales_amount_daily,
                ]
            ):
                continue
            cleaned_rows.append(
                {
                    "budget_name": budget_name,
                    "budget_date": budget_date,
                    "store_id": store_id,
                    "orders_budget_daily": orders_budget_daily,
                    "sales_amount_daily": sales_amount_daily,
                }
            )

        budget_names = sorted({row["budget_name"] for row in cleaned_rows})
        if not budget_names:
            raise ValueError("No valid budget_name values found in input CSV.")

        budget_names_sql = ", ".join(
            "'" + name.replace("'", "''") + "'" for name in sorted(budget_names)
        )
        logger.info(
            "Replacing existing rows for %s budget_name value(s): %s",
            len(budget_names),
            ", ".join(sorted(budget_names)),
        )
        try:
            spark.sql(
                f"DELETE FROM {args.target_table} WHERE budget_name IN ({budget_names_sql})"
            ).collect()
        except Exception as exc:
            if (
                "DELTA_TABLE_NOT_FOUND" not in str(exc)
                and "DELTA_PATH_DOES_NOT_EXIST" not in str(exc)
            ):
                raise
            logger.warning(
                "Target exists in catalog but Delta path/log is missing. "
                "Bootstrapping empty Delta table at %s and retrying DELETE.",
                args.bootstrap_location,
            )
            location_sql = args.bootstrap_location.replace("`", "``")
            spark.sql(
                f"""
                CREATE TABLE IF NOT EXISTS delta.`{location_sql}` (
                  budget_name STRING,
                  budget_date DATE,
                  store_id SMALLINT,
                  orders_budget_daily DECIMAL(12,4),
                  sales_amount_daily DECIMAL(12,4)
                )
                USING DELTA
                """
            ).collect()
            spark.sql(
                f"DELETE FROM {args.target_table} WHERE budget_name IN ({budget_names_sql})"
            ).collect()

        def _sql_str(value: str) -> str:
            return "'" + value.replace("'", "''") + "'"

        def _values_sql(row: dict[str, str]) -> str:
            return (
                "("
                f"{_sql_str(row['budget_name'])}, "
                f"DATE {_sql_str(row['budget_date'])}, "
                f"CAST({int(row['store_id'])} AS SMALLINT), "
                f"CAST({_sql_str(row['orders_budget_daily'])} AS DECIMAL(12,4)), "
                f"CAST({_sql_str(row['sales_amount_daily'])} AS DECIMAL(12,4))"
                ")"
            )

        batch_size = 200
        logger.info("Appending %s rows into %s", len(cleaned_rows), args.target_table)
        for idx in range(0, len(cleaned_rows), batch_size):
            batch = cleaned_rows[idx : idx + batch_size]
            values_block = ", ".join(_values_sql(row) for row in batch)
            spark.sql(
                f"INSERT INTO {args.target_table} "
                "(budget_name, budget_date, store_id, orders_budget_daily, sales_amount_daily) "
                f"VALUES {values_block}"
            ).collect()
        logger.info("Load finished successfully.")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
