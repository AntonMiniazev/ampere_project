"""Check that daily Iceberg publication preserves old rows and is repeatable."""

from __future__ import annotations

import importlib.util
import unittest
from pathlib import Path

import duckdb


MODULE_PATH = Path(__file__).resolve().parents[1] / "docker/dbt_iceberg/publish_catalog.py"
SPEC = importlib.util.spec_from_file_location("publish_catalog", MODULE_PATH)
publish_catalog = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(publish_catalog)


class PublishTests(unittest.TestCase):
    """Exercise the SQL against attached catalogs with no external services."""

    def setUp(self) -> None:
        """Create local stand-ins for staged DuckDB and published Iceberg."""
        self.con = duckdb.connect()
        for alias in ("staged_silver", "publish_silver"):
            self.con.execute(f"ATTACH ':memory:' AS {alias}")
            self.con.execute(f"CREATE SCHEMA {alias}.silver")
        self.con.execute(
            "CREATE TABLE staged_silver.silver.fact_orders "
            "(order_id INTEGER, order_date DATE, total_amount INTEGER)"
        )
        self.con.execute(
            "CREATE TABLE publish_silver.silver.fact_orders "
            "(order_id INTEGER, order_date DATE, total_amount INTEGER)"
        )

    def tearDown(self) -> None:
        """Close the ephemeral DuckDB catalogs."""
        self.con.close()

    def test_daily_upsert_retains_history_and_is_idempotent(self) -> None:
        """A late update must not erase other orders from its old date."""
        self.con.execute(
            "INSERT INTO publish_silver.silver.fact_orders VALUES "
            "(1, DATE '2025-12-16', 10), (2, DATE '2025-12-16', 20)"
        )
        self.con.execute(
            "INSERT INTO staged_silver.silver.fact_orders VALUES "
            "(1, DATE '2025-12-16', 11), (3, DATE '2026-10-06', 30)"
        )
        for _ in range(2):
            publish_catalog.publish_table(
                self.con, "silver", "fact_orders", "daily_refresh"
            )
        rows = self.con.execute(
            "SELECT order_id, total_amount FROM publish_silver.silver.fact_orders "
            "ORDER BY order_id"
        ).fetchall()
        self.assertEqual(rows, [(1, 11), (2, 20), (3, 30)])

    def test_duplicate_source_keys_fail_before_publish(self) -> None:
        """Ambiguous updates leave published history untouched."""
        self.con.execute(
            "INSERT INTO publish_silver.silver.fact_orders VALUES "
            "(1, DATE '2025-12-16', 10)"
        )
        self.con.execute(
            "INSERT INTO staged_silver.silver.fact_orders VALUES "
            "(1, DATE '2025-12-16', 11), (1, DATE '2025-12-16', 12)"
        )
        with self.assertRaisesRegex(ValueError, "duplicate merge keys"):
            publish_catalog.publish_table(
                self.con, "silver", "fact_orders", "daily_refresh"
            )
        self.assertEqual(
            self.con.execute(
                "SELECT total_amount FROM publish_silver.silver.fact_orders"
            ).fetchone(),
            (10,),
        )

    def test_gold_daily_merge_retains_older_sales(self) -> None:
        """Gold publication retains dates outside the current staged slice."""
        for alias in ("staged_gold", "publish_gold"):
            self.con.execute(f"ATTACH ':memory:' AS {alias}")
            self.con.execute(f"CREATE SCHEMA {alias}.gold")
            self.con.execute(
                f"CREATE TABLE {alias}.gold.fct_orders_sales "
                "(order_id INTEGER, order_date DATE, total_amount INTEGER)"
            )
        self.con.execute(
            "INSERT INTO publish_gold.gold.fct_orders_sales VALUES "
            "(1, DATE '2025-12-16', 10), (2, DATE '2026-10-05', 20)"
        )
        self.con.execute(
            "INSERT INTO staged_gold.gold.fct_orders_sales VALUES "
            "(2, DATE '2026-10-05', 25), (3, DATE '2026-10-06', 30)"
        )
        publish_catalog.publish_table(
            self.con, "gold", "fct_orders_sales", "daily_refresh"
        )
        rows = self.con.execute(
            "SELECT order_id, total_amount FROM publish_gold.gold.fct_orders_sales "
            "ORDER BY order_id"
        ).fetchall()
        self.assertEqual(rows, [(1, 10), (2, 25), (3, 30)])


if __name__ == "__main__":
    unittest.main()
