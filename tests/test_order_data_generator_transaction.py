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
from order_data_generator.generators import orders_gen  # noqa: E402
import polars as pl  # noqa: E402


class GeneratorTransactionTests(unittest.TestCase):
    def test_new_request_on_latest_date_creates_an_additional_batch(self):
        connection = MagicMock()
        connection.info = {}
        latest, inserted, tables = MagicMock(), MagicMock(), MagicMock()
        latest.scalar_one.return_value = date(2026, 10, 18)
        inserted.scalar_one.return_value = 293
        tables.scalars.return_value.all.return_value = ['orders']
        connection.execute.side_effect = [
            self._mapping_result({'dataset_id':'dataset','project_start_date':date(2026,1,1)}),
            self._mapping_result(None), latest, inserted, MagicMock(),
            *[MagicMock() for _ in range(4)], tables,
        ]
        with patch.object(db, 'ensure_generation_request_schema'), patch.object(db, 'StagedChangeBatch') as writer:
            batch = db.start_generation_batch(connection, date(2026,10,18),
                scenario_seed=None, config_fingerprint='config', source_code_version='image',
                generation_request_id='dag:second-run')
        self.assertEqual(batch['batch_sequence'], 293)
        params = connection.execute.call_args_list[3].args[1]
        self.assertEqual(params['batch_kind'], 'manual')
        self.assertEqual(params['generation_request_id'], 'dag:second-run')
        writer.assert_called_once()

    def test_request_retry_reuses_its_completed_batch(self):
        connection = MagicMock()
        tables = MagicMock()
        tables.scalars.return_value.all.return_value = ['orders']
        connection.execute.side_effect = [
            self._mapping_result({'dataset_id':'dataset','project_start_date':date(2026,1,1)}),
            self._mapping_result({'batch_id':'batch','batch_sequence':293,'business_date':date(2026,10,18),
                'status':'completed','scenario_seed':None,'config_fingerprint':'config'}), tables,
        ]
        with patch.object(db, 'ensure_generation_request_schema'), patch.object(db.StagedChangeBatch, 'recover_committed') as recover:
            result = db.start_generation_batch(connection,date(2026,10,18),scenario_seed=None,
                config_fingerprint='config',source_code_version='image',generation_request_id='dag:run')
        self.assertIsNone(result)
        recover.assert_called_once()
        self.assertTrue(all('INSERT' not in str(c.args[0]) for c in connection.execute.call_args_list))

    def test_new_request_cannot_generate_an_older_date(self):
        connection = MagicMock()
        latest = MagicMock()
        latest.scalar_one.return_value = date(2026,10,19)
        connection.execute.side_effect = [
            self._mapping_result({'dataset_id':'dataset','project_start_date':date(2026,1,1)}),
            self._mapping_result(None), latest,
        ]
        with patch.object(db, 'ensure_generation_request_schema'), self.assertRaisesRegex(ValueError,'timeline'):
            db.start_generation_batch(connection,date(2026,10,18),scenario_seed=None,
                config_fingerprint='config',source_code_version='image',generation_request_id='dag:run')
        self.assertTrue(all('INSERT' not in str(c.args[0]) for c in connection.execute.call_args_list))

    def test_request_cannot_be_reused_for_another_date(self):
        connection = MagicMock()
        connection.execute.side_effect = [
            self._mapping_result({'dataset_id':'dataset','project_start_date':date(2026,1,1)}),
            self._mapping_result({'business_date':date(2026,10,18)}),
        ]
        with patch.object(db, 'ensure_generation_request_schema'), self.assertRaisesRegex(ValueError,'another business date'):
            db.start_generation_batch(connection,date(2026,10,19),scenario_seed=None,
                config_fingerprint='config',source_code_version='image',generation_request_id='dag:run')

    @staticmethod
    def _config():
        return replace(load_config(), schema="source")

    @staticmethod
    def _result(value):
        result = MagicMock()
        result.scalar_one_or_none.return_value = value
        return result

    @staticmethod
    def _mapping_result(value):
        result = MagicMock()
        result.mappings.return_value.one_or_none.return_value = value
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

    def test_store_zone_map_accepts_multiple_zones_per_store(self) -> None:
        mapping = pl.DataFrame({"store_id": [1, 1, 2], "zone_id": [10, 11, 20]})
        stores = pl.DataFrame({"store_id": [1, 2]})
        with patch.object(orders_gen, "read_sql", side_effect=[mapping, stores]):
            zones_by_store = orders_gen.load_store_zone_map("source")

        self.assertEqual(zones_by_store, {1: (10, 11), 2: (20,)})

    def test_store_zone_map_rejects_stores_without_zones(self) -> None:
        mapping = pl.DataFrame({"store_id": [1], "zone_id": [10]})
        stores = pl.DataFrame({"store_id": [1, 2]})
        with (
            patch.object(orders_gen, "read_sql", side_effect=[mapping, stores]),
            self.assertRaisesRegex(ValueError, r"missing stores=\[2\]"),
        ):
            orders_gen.load_store_zone_map("source")

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
            generated = main.run_generation(date(2026, 10, 10), self._config(),
                generation_request_id='dag:daily-run')

        self.assertTrue(generated)
        engine.begin.assert_called_once_with()
        self.assertGreaterEqual(connection.execute.call_count, 2)
        self.assertEqual(start_batch.call_args.args[0], connection)
        self.assertTrue(start_batch.call_args.kwargs["allow_completed_mismatch"])
        self.assertEqual(start_batch.call_args.kwargs['generation_request_id'], 'dag:daily-run')
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
        self.assertTrue(start_batch.call_args.kwargs["allow_completed_mismatch"])

    def test_daily_run_recovers_completed_batch_with_backfill_seed(self) -> None:
        connection = MagicMock()
        dataset_result = self._mapping_result(
            {"dataset_id": "dataset", "project_start_date": date(2026, 1, 1)}
        )
        existing_result = self._mapping_result(
            {
                "batch_id": "batch",
                "batch_sequence": 12,
                "status": "completed",
                "scenario_seed": 42,
                "config_fingerprint": "backfill-config",
            }
        )
        tables_result = MagicMock()
        tables_result.scalars.return_value.all.return_value = ["orders"]
        connection.execute.side_effect = [
            dataset_result,
            existing_result,
            tables_result,
        ]

        with patch.object(db.StagedChangeBatch, "recover_committed") as recover:
            batch = db.start_generation_batch(
                connection,
                date(2026, 10, 12),
                scenario_seed=None,
                config_fingerprint="daily-config",
                source_code_version="image",
                allow_completed_mismatch=True,
            )

        self.assertIsNone(batch)
        recover.assert_called_once()
        self.assertEqual(recover.call_args.kwargs["batch_id"], "batch")

    def test_backfill_still_rejects_completed_batch_with_different_seed(self) -> None:
        connection = MagicMock()
        connection.execute.side_effect = [
            self._mapping_result(
                {"dataset_id": "dataset", "project_start_date": date(2026, 1, 1)}
            ),
            self._mapping_result(
                {
                    "batch_id": "batch",
                    "batch_sequence": 12,
                    "status": "completed",
                    "scenario_seed": 42,
                    "config_fingerprint": "same-config",
                }
            ),
        ]

        with (
            patch.object(db.StagedChangeBatch, "recover_committed") as recover,
            self.assertRaisesRegex(ValueError, "different seed or config"),
        ):
            db.start_generation_batch(
                connection,
                date(2026, 10, 12),
                scenario_seed=None,
                config_fingerprint="same-config",
                source_code_version="image",
            )

        recover.assert_not_called()

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
