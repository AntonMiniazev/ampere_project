"""Regression checks for complete, deterministic status-history batch staging."""

from __future__ import annotations

import unittest

import duckdb


class SilverHistoryBatchingTests(unittest.TestCase):
    def test_full_history_batches_equal_single_pass_deduplication(self) -> None:
        con = duckdb.connect()
        try:
            con.execute(
                """
                CREATE TABLE source_history AS SELECT * FROM (VALUES
                    (1, 2, TIMESTAMP '2026-01-01 08:00:00', 'old', 1),
                    (1, 3, TIMESTAMP '2026-01-01 08:00:00', 'new', 2),
                    (2, 4, TIMESTAMP '2026-01-02 08:00:00', 'two', 1),
                    (NULL, 5, TIMESTAMP '2026-01-03 08:00:00', 'null-order', 1),
                    (3, 6, TIMESTAMP '2026-01-04 08:00:00', 'three-a', 1),
                    (3, 7, TIMESTAMP '2026-01-04 08:00:00', 'three-b', 2),
                    (4, 8, TIMESTAMP '2026-01-05 08:00:00', 'four', 1)
                ) AS rows(order_id, order_status_id, status_datetime, payload, applied)
                """
            )
            con.execute(
                """
                CREATE TABLE expected AS
                SELECT * EXCLUDE (row_num)
                FROM (
                    SELECT *, row_number() OVER (
                        PARTITION BY order_id, status_datetime ORDER BY applied DESC
                    ) AS row_num
                    FROM source_history
                )
                WHERE row_num = 1
                """
            )

            batches = 4
            for batch_index in range(batches):
                create_or_insert = (
                    "CREATE TABLE actual AS "
                    if batch_index == 0
                    else "INSERT INTO actual "
                )
                con.execute(
                    create_or_insert
                    + f"""
                    SELECT * EXCLUDE (row_num)
                    FROM (
                        SELECT *, row_number() OVER (
                            PARTITION BY order_id, status_datetime ORDER BY applied DESC
                        ) AS row_num
                        FROM source_history
                        WHERE coalesce(hash(cast(order_id AS integer)) % {batches}, 0)
                            = {batch_index}
                    )
                    WHERE row_num = 1
                    """
                )

            self.assertEqual(
                con.execute(
                    "SELECT * FROM expected ORDER BY order_id NULLS FIRST, status_datetime"
                ).fetchall(),
                con.execute(
                    "SELECT * FROM actual ORDER BY order_id NULLS FIRST, status_datetime"
                ).fetchall(),
            )
            self.assertEqual(
                con.execute(
                    f"""
                    SELECT count(*) FROM (
                        SELECT order_id
                        FROM source_history
                        GROUP BY order_id
                        HAVING count(DISTINCT coalesce(hash(cast(order_id AS integer)) % {batches}, 0)) > 1
                    )
                    """
                ).fetchone()[0],
                0,
            )
        finally:
            con.close()


if __name__ == "__main__":
    unittest.main()
