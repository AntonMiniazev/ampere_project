"""Verify a business date uses one shared PostgreSQL transaction."""

from __future__ import annotations

import sys
import unittest
from datetime import date
from pathlib import Path
from types import ModuleType, SimpleNamespace
from unittest.mock import MagicMock, patch


ROOT = Path(__file__).resolve().parents[1]
GENERATOR_DIR = ROOT / "docker/order_data_generator/model"
package = ModuleType("order_data_generator")
package.__path__ = [str(GENERATOR_DIR)]
sys.modules["order_data_generator"] = package

from order_data_generator import main  # noqa: E402


class GeneratorTransactionTests(unittest.TestCase):
    @staticmethod
    def _result(value):
        result = MagicMock()
        result.scalar_one_or_none.return_value = value
        return result

    def test_generation_holds_one_advisory_locked_transaction_for_the_day(self) -> None:
        connection = MagicMock()
        transaction = MagicMock()
        transaction.__enter__.return_value = connection
        engine = MagicMock()
        engine.begin.return_value = transaction
        connection.execute.side_effect = [
            MagicMock(),
            self._result(date(2026, 10, 9)),
            self._result(date(2026, 10, 10)),
        ]

        with (
            patch.object(main, "get_engine", return_value=engine),
            patch.object(main, "prepare_clients_update_and_generation", return_value=([], [])) as prepare_clients,
            patch.object(main, "update_churned") as update_churned,
            patch.object(main, "prepare_orders_statuses") as prepare_orders,
        ):
            generated = main.run_generation(
                date(2026, 10, 10), SimpleNamespace(schema="source")
            )

        self.assertTrue(generated)
        engine.begin.assert_called_once_with()
        self.assertEqual(connection.execute.call_count, 3)
        self.assertEqual(prepare_clients.call_args.kwargs["connection"], connection)
        self.assertEqual(update_churned.call_args.kwargs["connection"], connection)
        self.assertEqual(prepare_orders.call_args.kwargs["connection"], connection)
        transaction.__exit__.assert_called_once_with(None, None, None)

    def test_completed_date_is_skipped_without_running_mutations(self) -> None:
        connection = MagicMock()
        transaction = MagicMock()
        transaction.__enter__.return_value = connection
        engine = MagicMock()
        engine.begin.return_value = transaction
        connection.execute.side_effect = [
            MagicMock(),
            self._result(date(2026, 10, 10)),
        ]

        with (
            patch.object(main, "get_engine", return_value=engine),
            patch.object(main, "prepare_clients_update_and_generation") as prepare,
        ):
            generated = main.run_generation(
                date(2026, 10, 10), SimpleNamespace(schema="source")
            )

        self.assertFalse(generated)
        prepare.assert_not_called()
        self.assertEqual(connection.execute.call_count, 2)

    def test_generation_rejects_a_gap_in_the_source_dates(self) -> None:
        connection = MagicMock()
        transaction = MagicMock()
        transaction.__enter__.return_value = connection
        engine = MagicMock()
        engine.begin.return_value = transaction
        connection.execute.side_effect = [
            MagicMock(),
            self._result(date(2026, 10, 9)),
        ]

        with (
            patch.object(main, "get_engine", return_value=engine),
            patch.object(main, "prepare_clients_update_and_generation") as prepare,
            self.assertRaisesRegex(ValueError, "must be chronological"),
        ):
            main.run_generation(date(2026, 10, 11), SimpleNamespace(schema="source"))

        prepare.assert_not_called()

    def test_range_is_inclusive_resumable_and_reseeds_each_date(self) -> None:
        config = SimpleNamespace(schema="source")
        dates = [date(2026, 10, 10), date(2026, 10, 11), date(2026, 10, 12)]
        with (
            patch.object(main, "run_generation", side_effect=[True, False, True]) as run,
            patch.object(main, "set_seed") as set_seed,
        ):
            generated = main.run_generation_range(
                dates[0], dates[-1], config, seed=42
            )

        self.assertEqual(generated, 2)
        self.assertEqual([call.args[0] for call in run.call_args_list], dates)
        self.assertEqual(set_seed.call_count, 3)
        self.assertEqual(
            [call.args[0] for call in set_seed.call_args_list],
            [(42 + item.toordinal()) % (2**32) for item in dates],
        )

    def test_range_rejects_more_than_31_days(self) -> None:
        with self.assertRaisesRegex(ValueError, "maximum supported is 31"):
            main.run_generation_range(
                date(2026, 1, 1),
                date(2026, 2, 1),
                SimpleNamespace(schema="source"),
            )


if __name__ == "__main__":
    unittest.main()
