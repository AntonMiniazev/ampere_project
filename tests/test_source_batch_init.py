"""Checks clean initialization writes a baseline manifest without row journals."""

from __future__ import annotations

import sys
import unittest
from datetime import date
from pathlib import Path
from types import ModuleType
from unittest.mock import MagicMock, patch

ROOT = Path(__file__).resolve().parents[1]
INIT_MODEL = ROOT / "docker/init_source_preparation/model"
sys.path.insert(0, str(INIT_MODEL))
sys.path.insert(0, str(ROOT))
package = ModuleType("init_source_preparation")
package.__path__ = [str(INIT_MODEL)]
sys.modules["init_source_preparation"] = package

from init_source_preparation import source_batches  # noqa: E402


class SourceBatchInitTests(unittest.TestCase):
    def test_initialization_stages_baseline_and_creates_only_compact_control_rows(self):
        connection = MagicMock()
        source_result = MagicMock()
        source_result.mappings.return_value.fetchmany.side_effect = [
            [{"id": 1, "source_row_id": "row-1"}], [],
            [],
        ]
        stager = MagicMock()
        stager.chunk_rows = 50_000
        stager.append.return_value = 1
        statements = []
        select_statements = []

        def execute(statement, *_args, **_kwargs):
            sql = str(statement)
            statements.append(sql)
            if "SELECT * FROM" in sql:
                select_statements.append(statement)
                return source_result
            return MagicMock()

        connection.execute.side_effect = execute

        with (
            patch.object(source_batches, "SOURCE_TABLES", ("orders", "payments")),
            patch.object(source_batches, "_ensure_control_schema"),
            patch.object(source_batches, "_ensure_source_row_identity"),
            patch.object(source_batches, "_new_batch", return_value=("batch-id", 3)),
            patch.object(source_batches, "StagedChangeBatch", return_value=stager) as make_stager,
        ):
            dataset_id, result = source_batches.initialize_source_dataset(
                connection,
                "source",
                date(2026, 1, 1),
                initialization_run_id="manual__run-1",
            )

        self.assertIs(result, stager)
        self.assertTrue(dataset_id)
        connection.execution_options.assert_not_called()
        self.assertTrue(select_statements[0]._execution_options["stream_results"])
        self.assertEqual(make_stager.call_args.kwargs["source_tables"], ("orders", "payments"))
        self.assertEqual(stager.append.call_count, 1)
        stager.prepare.assert_called_once_with()
        sql = "\n".join(statements).lower()
        self.assertNotIn("source_changes", sql)
        self.assertNotIn("create trigger", sql)
        self.assertIn('"pipeline".batch_table_counts', sql)
        self.assertIn('"pipeline".batches', sql)

    def test_v2_table_and_control_sql_use_only_v2_namespaces(self):
        connection = MagicMock()
        connection.execute.return_value.scalar_one.return_value = 1
        connection.execute.return_value.mappings.return_value.fetchmany.return_value = []

        with patch.object(source_batches, "StagedChangeBatch") as make_stager:
            source_batches.initialize_source_dataset(
                connection,
                "ampere_source",
                date(2026, 1, 1),
                control_schema="ampere_pipeline",
                bucket="test",
                prefix="ampere/source-batches/v1/ampere_source",
            )

        sql = "\n".join(str(call.args[0]) for call in connection.execute.call_args_list)
        self.assertIn('CREATE SCHEMA IF NOT EXISTS "ampere_pipeline"', sql)
        self.assertIn('REFERENCES "ampere_pipeline".datasets', sql)
        self.assertIn('INSERT INTO "ampere_pipeline".batch_table_counts', sql)
        self.assertIn('SELECT * FROM "ampere_source"."orders"', sql)
        self.assertNotIn('"pipeline"', sql)
        self.assertNotIn('"source"', sql)
        self.assertEqual(make_stager.call_args.kwargs["prefix"], "ampere/source-batches/v1/ampere_source")


if __name__ == "__main__":
    unittest.main()
