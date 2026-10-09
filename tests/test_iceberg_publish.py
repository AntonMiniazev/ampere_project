"""Check that daily Iceberg publication preserves old rows and is repeatable."""

from __future__ import annotations

import importlib.util
import sys
import tempfile
import unittest
from concurrent.futures import ThreadPoolExecutor
from datetime import date
from pathlib import Path
from unittest.mock import patch

import duckdb


MODULE_PATH = (
    Path(__file__).resolve().parents[1] / "docker/dbt_iceberg/publish_catalog.py"
)
sys.path.insert(0, str(MODULE_PATH.parent))
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

    def test_daily_retry_skips_unchanged_matches(self) -> None:
        """A replay must not rewrite rows whose values already match."""
        self.con.execute(
            "INSERT INTO publish_silver.silver.fact_orders VALUES "
            "(1, DATE '2025-12-16', 10)"
        )
        self.con.execute(
            "INSERT INTO staged_silver.silver.fact_orders VALUES "
            "(1, DATE '2025-12-16', 10), (2, DATE '2026-10-07', 20)"
        )

        class CountMergeActions:
            """Record the update and insert actions of local DuckDB merges."""

            def __init__(self, connection: duckdb.DuckDBPyConnection) -> None:
                self.connection = connection
                self.actions: list[list[tuple[str]]] = []

            def execute(self, statement: str) -> duckdb.DuckDBPyConnection:
                """Capture merge actions without changing production SQL."""
                if statement.startswith("MERGE INTO"):
                    result = self.connection.execute(
                        statement + " RETURNING merge_action"
                    )
                    self.actions.append(result.fetchall())
                    return result
                return self.connection.execute(statement)

        recording = CountMergeActions(self.con)
        publish_catalog.publish_table(recording, "silver", "fact_orders", "daily_refresh")
        publish_catalog.publish_table(recording, "silver", "fact_orders", "daily_refresh")
        self.assertEqual(recording.actions, [[("INSERT",)], []])

        self.con.execute(
            "UPDATE staged_silver.silver.fact_orders SET total_amount = 11 "
            "WHERE order_id = 1"
        )
        publish_catalog.publish_table(recording, "silver", "fact_orders", "daily_refresh")
        self.assertEqual(recording.actions[-1], [("UPDATE",)])

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

    def test_gold_daily_merge_retains_older_aggregate_months(self) -> None:
        """Gold publication retains aggregate months outside the current slice."""
        for alias in ("staged_gold", "publish_gold"):
            self.con.execute(f"ATTACH ':memory:' AS {alias}")
            self.con.execute(f"CREATE SCHEMA {alias}.gold")
            self.con.execute(
                f"CREATE TABLE {alias}.gold.curie_marketing_sales_budget_monthly_store "
                "(month DATE, store_id INTEGER, sales_amount INTEGER)"
            )
        self.con.execute(
            "INSERT INTO publish_gold.gold.curie_marketing_sales_budget_monthly_store VALUES "
            "(DATE '2025-12-01', 1, 10), (DATE '2026-10-01', 1, 20)"
        )
        self.con.execute(
            "INSERT INTO staged_gold.gold.curie_marketing_sales_budget_monthly_store VALUES "
            "(DATE '2026-10-01', 1, 25), (DATE '2026-10-01', 2, 30)"
        )
        publish_catalog.publish_table(
            self.con, "gold", "curie_marketing_sales_budget_monthly_store", "daily_refresh"
        )
        rows = self.con.execute(
            "SELECT month, store_id, sales_amount "
            "FROM publish_gold.gold.curie_marketing_sales_budget_monthly_store "
            "ORDER BY month, store_id"
        ).fetchall()
        self.assertEqual(
            rows,
            [(date(2025, 12, 1), 1, 10), (date(2026, 10, 1), 1, 25), (date(2026, 10, 1), 2, 30)],
        )

    def test_complete_dimension_synchronizes_missing_and_changed_keys(self) -> None:
        """A complete dimension removes stale members without dropping its table."""
        for alias in ("staged_silver", "publish_silver"):
            self.con.execute(
                f"CREATE TABLE {alias}.silver.dim_assortment "
                "(assortment_key VARCHAR, product_id INTEGER)"
            )
        self.con.execute(
            "INSERT INTO publish_silver.silver.dim_assortment VALUES "
            "('1|1', 1), ('1|2', 2)"
        )
        self.con.execute(
            "INSERT INTO staged_silver.silver.dim_assortment VALUES "
            "('1|1', 10), ('1|3', 3)"
        )
        for _ in range(2):
            publish_catalog.publish_table(
                self.con, "silver", "dim_assortment", "daily_refresh"
            )
        self.assertEqual(
            self.con.execute(
                "SELECT assortment_key, product_id "
                "FROM publish_silver.silver.dim_assortment ORDER BY assortment_key"
            ).fetchall(),
            [("1|1", 10), ("1|3", 3)],
        )

    def test_complete_dimension_rejects_duplicate_keys(self) -> None:
        """Duplicate staged keys cannot trigger a destructive synchronization."""
        for alias in ("staged_silver", "publish_silver"):
            self.con.execute(
                f"CREATE TABLE {alias}.silver.dim_assortment "
                "(assortment_key VARCHAR, product_id INTEGER)"
            )
        self.con.execute(
            "INSERT INTO publish_silver.silver.dim_assortment VALUES ('1|1', 1)"
        )
        self.con.execute(
            "INSERT INTO staged_silver.silver.dim_assortment VALUES "
            "('1|1', 10), ('1|1', 11)"
        )
        with self.assertRaisesRegex(ValueError, "duplicate merge keys"):
            publish_catalog.publish_table(
                self.con, "silver", "dim_assortment", "daily_refresh"
            )
        self.assertEqual(
            self.con.execute(
                "SELECT product_id FROM publish_silver.silver.dim_assortment"
            ).fetchall(),
            [(1,)],
        )

    def test_complete_dimension_rejects_empty_source(self) -> None:
        """An empty complete snapshot must not delete the published dimension."""
        for alias in ("staged_silver", "publish_silver"):
            self.con.execute(
                f"CREATE TABLE {alias}.silver.dim_assortment "
                "(assortment_key VARCHAR, product_id INTEGER)"
            )
        self.con.execute(
            "INSERT INTO publish_silver.silver.dim_assortment VALUES ('1|1', 1)"
        )
        with self.assertRaisesRegex(ValueError, "empty table"):
            publish_catalog.publish_table(
                self.con, "silver", "dim_assortment", "daily_refresh"
            )
        self.assertEqual(
            self.con.execute(
                "SELECT count(*) FROM publish_silver.silver.dim_assortment"
            ).fetchone(),
            (1,),
        )

    def test_complete_dimension_recovers_after_cleanup_failure(self) -> None:
        """Retry removes stale rows left after a successful upsert commit."""
        for alias in ("staged_silver", "publish_silver"):
            self.con.execute(
                f"CREATE TABLE {alias}.silver.dim_assortment "
                "(assortment_key VARCHAR, product_id INTEGER)"
            )
        self.con.execute(
            "INSERT INTO publish_silver.silver.dim_assortment VALUES "
            "('1|1', 1), ('1|2', 2)"
        )
        self.con.execute(
            "INSERT INTO staged_silver.silver.dim_assortment VALUES "
            "('1|1', 10), ('1|3', 3)"
        )

        class FailDeleteOnce:
            """Simulate a failure between the upsert and cleanup commits."""

            def __init__(self, connection: duckdb.DuckDBPyConnection) -> None:
                self.connection = connection
                self.failed = False

            def execute(self, statement: str) -> duckdb.DuckDBPyConnection:
                """Fail the first cleanup statement and run every other query."""
                if statement.startswith("DELETE FROM") and not self.failed:
                    self.failed = True
                    raise RuntimeError("simulated cleanup failure")
                return self.connection.execute(statement)

        with self.assertRaisesRegex(RuntimeError, "simulated cleanup failure"):
            publish_catalog.publish_table(
                FailDeleteOnce(self.con), "silver", "dim_assortment", "daily_refresh"
            )
        self.assertEqual(
            self.con.execute(
                "SELECT assortment_key FROM publish_silver.silver.dim_assortment "
                "ORDER BY assortment_key"
            ).fetchall(),
            [("1|1",), ("1|2",), ("1|3",)],
        )
        publish_catalog.publish_table(
            self.con, "silver", "dim_assortment", "daily_refresh"
        )
        self.assertEqual(
            self.con.execute(
                "SELECT assortment_key, product_id "
                "FROM publish_silver.silver.dim_assortment ORDER BY assortment_key"
            ).fetchall(),
            [("1|1", 10), ("1|3", 3)],
        )

    def test_gold_month_store_grain_matches_on_retry(self) -> None:
        """A monthly store aggregate updates one stable grain on each retry."""
        for alias in ("staged_gold", "publish_gold"):
            self.con.execute(f"ATTACH ':memory:' AS {alias}")
            self.con.execute(f"CREATE SCHEMA {alias}.gold")
            self.con.execute(
                f"CREATE TABLE {alias}.gold.curie_marketing_sales_budget_monthly_store "
                "(month DATE, store_id INTEGER, sales_amount INTEGER)"
            )
        self.con.execute(
            "INSERT INTO publish_gold.gold.curie_marketing_sales_budget_monthly_store VALUES "
            "(DATE '2026-10-01', 1, 10), (DATE '2026-10-01', 2, 20)"
        )
        self.con.execute(
            "INSERT INTO staged_gold.gold.curie_marketing_sales_budget_monthly_store "
            "VALUES (DATE '2026-10-01', 1, 15)"
        )
        for _ in range(2):
            publish_catalog.publish_table(
                self.con, "gold", "curie_marketing_sales_budget_monthly_store", "daily_refresh"
            )
        self.assertEqual(
            self.con.execute(
                "SELECT month, store_id, sales_amount "
                "FROM publish_gold.gold.curie_marketing_sales_budget_monthly_store"
            ).fetchall(),
            [(date(2026, 10, 1), 1, 15), (date(2026, 10, 1), 2, 20)],
        )

    def test_full_history_synchronizes_fact_and_removes_stale_rows(self) -> None:
        """Only a complete fact source may remove rows absent from staging."""
        self.con.execute(
            "INSERT INTO publish_silver.silver.fact_orders VALUES "
            "(1, DATE '2025-12-16', 10), (2, DATE '2026-10-05', 20)"
        )
        self.con.execute(
            "INSERT INTO staged_silver.silver.fact_orders VALUES "
            "(1, DATE '2025-12-16', 11), (3, DATE '2026-10-06', 30)"
        )
        publish_catalog.publish_table(self.con, "silver", "fact_orders", "full_history")
        self.assertEqual(
            self.con.execute(
                "SELECT order_id, total_amount FROM publish_silver.silver.fact_orders "
                "ORDER BY order_id"
            ).fetchall(),
            [(1, 11), (3, 30)],
        )

    def test_full_history_creates_missing_table(self) -> None:
        """A first full rebuild may establish a missing published table."""
        self.con.execute("DROP TABLE publish_silver.silver.fact_orders")
        self.con.execute(
            "INSERT INTO staged_silver.silver.fact_orders VALUES "
            "(1, DATE '2025-12-16', 10)"
        )
        publish_catalog.publish_table(self.con, "silver", "fact_orders", "full_history")
        self.assertEqual(
            self.con.execute(
                "SELECT order_id FROM publish_silver.silver.fact_orders"
            ).fetchall(),
            [(1,)],
        )

    def test_separate_layer_connections_publish_in_parallel(self) -> None:
        """Silver and Gold workers must not share a DuckDB connection."""
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            table_names = {
                "silver": "fact_orders",
                "gold": "curie_marketing_sales_budget_monthly_store",
            }
            for layer, table in table_names.items():
                for kind in ("staged", "publish"):
                    path = root / f"{kind}_{layer}.duckdb"
                    con = duckdb.connect(str(path))
                    con.execute(f"CREATE SCHEMA {layer}")
                    if layer == "silver":
                        con.execute(
                            f"CREATE TABLE {layer}.{table} "
                            "(order_id INTEGER, order_date DATE, total_amount INTEGER)"
                        )
                    else:
                        con.execute(
                            f"CREATE TABLE {layer}.{table} "
                            "(month DATE, store_id INTEGER, sales_amount INTEGER)"
                        )
                    if kind == "staged":
                        values = (
                            "(1, DATE '2026-10-06', 20)"
                            if layer == "silver"
                            else "(DATE '2026-10-01', 1, 20)"
                        )
                        con.execute(f"INSERT INTO {layer}.{table} VALUES {values}")
                    else:
                        values = (
                            "(1, DATE '2026-10-06', 10)"
                            if layer == "silver"
                            else "(DATE '2026-10-01', 1, 10)"
                        )
                        con.execute(f"INSERT INTO {layer}.{table} VALUES {values}")
                    con.close()

            def local_catalogs(con, workspace, layers):
                for layer in layers:
                    for kind in ("staged", "publish"):
                        path = root / f"{kind}_{layer}.duckdb"
                        con.execute(
                            f"ATTACH '{path}' "
                            f"AS {kind}_{layer} "
                            + ("(READ_ONLY)" if kind == "staged" else "")
                        )

            settings = {"memory_limit": "256MB", "threads": 2}
            with patch.object(publish_catalog, "attach_catalogs", local_catalogs):
                with ThreadPoolExecutor(max_workers=2) as executor:
                    futures = [
                        executor.submit(
                            publish_catalog.publish_layer,
                            root / "ampere_work.duckdb",
                            settings,
                            layer,
                            [table],
                            "daily_refresh",
                            {(layer, table): 1},
                        )
                        for layer, table in table_names.items()
                    ]
                    for future in futures:
                        future.result()
            for layer, table in table_names.items():
                con = duckdb.connect(str(root / f"publish_{layer}.duckdb"))
                measure = "total_amount" if layer == "silver" else "sales_amount"
                self.assertEqual(
                    con.execute(f"SELECT {measure} FROM {layer}.{table}").fetchone(),
                    (20,),
                )
                con.close()


if __name__ == "__main__":
    unittest.main()
