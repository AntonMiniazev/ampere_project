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
        self.assertIn("pipeline.batch_table_counts", sql)
        self.assertIn("pipeline.batches", sql)


if __name__ == "__main__":
    unittest.main()
