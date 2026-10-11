"""Verify daily commits and month-sized backfill transaction boundaries."""

from __future__ import annotations

import sys
import unittest
from datetime import date
from pathlib import Path
from dataclasses import replace
from types import ModuleType, SimpleNamespace
from unittest.mock import MagicMock, patch


ROOT = Path(__file__).resolve().parents[1]
GENERATOR_DIR = ROOT / "docker/order_data_generator/model"
package = ModuleType("order_data_generator")
package.__path__ = [str(GENERATOR_DIR)]
sys.modules["order_data_generator"] = package

from order_data_generator import db, main  # noqa: E402
from order_data_generator.config import load_config  # noqa: E402


class GeneratorTransactionTests(unittest.TestCase):
    @staticmethod
    def _config():
        return replace(load_config(), schema="source")

    @staticmethod
    def _result(value):
        result = MagicMock()
        result.scalar_one_or_none.return_value = value
        return result

    def test_copy_readback_captures_database_after_images_in_bounded_queries(self) -> None:
        connection = MagicMock()
        connection.info = {"ampere_source_batch_stager": SimpleNamespace(chunk_rows=2)}
        connection.execute.side_effect = [MagicMock(), MagicMock()]
        with patch.object(db, "MUTATION_RETURNING_BATCH_ROWS", 2), patch.object(
            db, "_capture_result", side_effect=[2, 1]
        ) as capture:
            count = db._capture_copied_rows(
                connection, "source", "delivery_tracking", ["id-1", "id-2", "id-3"]
            )
        self.assertEqual(count, 3)
        self.assertEqual(connection.execute.call_count, 2)
        self.assertEqual(capture.call_count, 2)
        for call_args in connection.execute.call_args_list:
            self.assertIn("SELECT *", str(call_args.args[0]))
            self.assertIn("source_row_id=ANY", str(call_args.args[0]))

    def test_generation_holds_one_advisory_locked_transaction_for_the_day(self) -> None:
        connection = MagicMock()
        transaction = MagicMock()
        transaction.__enter__.return_value = connection
        engine = MagicMock()
        engine.begin.return_value = transaction
        connection.execute.side_effect = [
            MagicMock(),
            self._result(date(2026, 10, 10)),
        ]
        stager = MagicMock()
        stager.row_counts = {"orders": 1}
        batch = {
            "dataset_id": "dataset", "batch_id": "batch", "batch_sequence": 2,
            "stager": stager,
        }

        with (
            patch.object(main, "get_engine", return_value=engine),
            patch.object(
                main, "prepare_clients_update_and_generation", return_value=([], [])
            ) as prepare_clients,
            patch.object(main, "update_churned") as update_churned,
            patch.object(main, "prepare_orders_statuses") as prepare_orders,
            patch.object(
                main, "start_generation_batch", return_value=batch
            ) as start_batch,
            patch.object(
                main, "complete_generation_batch", return_value={"orders": 1}
            ) as complete_batch,
        ):
            generated = main.run_generation(date(2026, 10, 10), self._config())

        self.assertTrue(generated)
        engine.begin.assert_called_once_with()
        self.assertGreaterEqual(connection.execute.call_count, 2)
        self.assertEqual(start_batch.call_args.args[0], connection)
        self.assertEqual(complete_batch.call_args.args, (connection, batch, {"orders": 1}))
        self.assertEqual(prepare_clients.call_args.kwargs["connection"], connection)
        self.assertEqual(update_churned.call_args.kwargs["connection"], connection)
        self.assertEqual(prepare_orders.call_args.kwargs["connection"], connection)
        stager.prepare.assert_called_once_with()
        stager.publish_committed.assert_called_once_with()
        transaction.__exit__.assert_called_once_with(None, None, None)

    def test_completed_date_is_skipped_without_running_mutations(self) -> None:
        connection = MagicMock()
        transaction = MagicMock()
        transaction.__enter__.return_value = connection
        engine = MagicMock()
        engine.begin.return_value = transaction
        with (
            patch.object(main, "get_engine", return_value=engine),
            patch.object(main, "prepare_clients_update_and_generation") as prepare,
            patch.object(
                main, "start_generation_batch", return_value=None
            ) as start_batch,
        ):
            generated = main.run_generation(date(2026, 10, 10), self._config())

        self.assertFalse(generated)
        prepare.assert_not_called()
        self.assertEqual(connection.execute.call_count, 1)
        start_batch.assert_called_once()

    def test_generation_rejects_a_gap_in_the_source_dates(self) -> None:
        connection = MagicMock()
        transaction = MagicMock()
        transaction.__enter__.return_value = connection
        engine = MagicMock()
        engine.begin.return_value = transaction
        with (
            patch.object(main, "get_engine", return_value=engine),
            patch.object(main, "prepare_clients_update_and_generation") as prepare,
            patch.object(
                main,
                "start_generation_batch",
                side_effect=ValueError("Generation must be chronological"),
            ),
            self.assertRaisesRegex(ValueError, "must be chronological"),
        ):
            main.run_generation(date(2026, 10, 11), self._config())

        prepare.assert_not_called()

    def test_range_commits_per_month_and_reseeds_each_date(self) -> None:
        config = SimpleNamespace(schema="source")
        dates = [date(2026, 1, 30), date(2026, 1, 31), date(2026, 2, 1)]
        transactions = []
        connections = []

        def begin():
            connection = MagicMock()
            transaction = MagicMock()
            transaction.__enter__.return_value = connection
            transactions.append(transaction)
            connections.append(connection)
            return transaction

        engine = MagicMock()
        engine.begin.side_effect = begin
        stager = MagicMock()
        with (
            patch.object(main, "get_engine", return_value=engine),
            patch.object(
                main, "_run_generation_in_transaction", side_effect=[(True, stager), (False, None), (True, stager)]
            ) as run,
            patch.object(main, "set_seed") as set_seed,
        ):
            generated = main.run_generation_range(dates[0], dates[-1], config, seed=42)

        self.assertEqual(generated, 2)
        self.assertEqual([call.args[0] for call in run.call_args_list], dates)
        self.assertEqual(engine.begin.call_count, 2)
        self.assertEqual(stager.publish_committed.call_count, 2)
        self.assertIs(run.call_args_list[0].args[2], connections[0])
        self.assertIs(run.call_args_list[1].args[2], connections[0])
        self.assertIs(run.call_args_list[2].args[2], connections[1])
        self.assertEqual(set_seed.call_count, 3)
        self.assertEqual(
            [call.args[0] for call in set_seed.call_args_list],
            [(42 + item.toordinal()) % (2**32) for item in dates],
        )

    def test_failed_date_rolls_back_the_active_month_transaction(self) -> None:
        dates = [date(2026, 1, 30), date(2026, 1, 31)]
        connection = MagicMock()
        transaction = MagicMock()
        transaction.__enter__.return_value = connection
        engine = MagicMock()
        engine.begin.return_value = transaction
        stager = MagicMock()

        with (
            patch.object(main, "get_engine", return_value=engine),
            patch.object(
                main,
                "_run_generation_in_transaction",
                side_effect=[(True, stager), RuntimeError("synthetic day failure")],
            ),
            self.assertRaisesRegex(RuntimeError, "synthetic day failure"),
        ):
            main.run_generation_range(
                dates[0], dates[-1], SimpleNamespace(schema="source")
            )

        transaction.__exit__.assert_called_once()
        self.assertIs(transaction.__exit__.call_args.args[0], RuntimeError)

    def test_range_accepts_ten_months_and_generates_dates_sequentially(self) -> None:
        start_date = date(2026, 1, 1)
        end_date = date(2026, 10, 31)
        engine = MagicMock()
        transaction = MagicMock()
        transaction.__enter__.return_value = MagicMock()
        engine.begin.return_value = transaction
        stager = MagicMock()
        with (
            patch.object(main, "get_engine", return_value=engine),
            patch.object(
                main, "_run_generation_in_transaction", return_value=(True, stager)
            ) as run,
        ):
            generated = main.run_generation_range(
                start_date, end_date, SimpleNamespace(schema="source")
            )

        self.assertEqual(generated, 304)
        self.assertEqual(run.call_count, 304)
        self.assertEqual(engine.begin.call_count, 10)
        self.assertEqual(run.call_args_list[0].args[0], start_date)
        self.assertEqual(run.call_args_list[-1].args[0], end_date)


if __name__ == "__main__":
    unittest.main()
