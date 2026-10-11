"""Checks that a source reset replaces stale journal payload with a baseline."""

from __future__ import annotations

import sys
import unittest
from datetime import date
from pathlib import Path
from unittest.mock import MagicMock, patch


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "docker/init_source_preparation/model"))

import source_journal  # noqa: E402


class SourceJournalResetTests(unittest.TestCase):
    def test_reset_drops_legacy_and_compact_capture_triggers(self) -> None:
        connection = MagicMock()

        source_journal.drop_source_capture_triggers(connection, "source", "clients")

        statements = [str(call.args[0]) for call in connection.execute.call_args_list]
        self.assertEqual(
            statements,
            [
                'DROP TRIGGER IF EXISTS "capture_source_change" ON "source"."clients"',
                'DROP TRIGGER IF EXISTS "capture_source_change_insert" ON "source"."clients"',
                'DROP TRIGGER IF EXISTS "capture_source_change_update" ON "source"."clients"',
                'DROP TRIGGER IF EXISTS "capture_source_change_delete" ON "source"."clients"',
            ],
        )

    def test_reset_clears_old_compact_metadata_before_writing_baseline(self) -> None:
        connection = MagicMock()
        with (
            patch.object(source_journal, "SOURCE_TABLES", ("orders", "payments")),
            patch.object(source_journal, "_ensure_control_schema"),
            patch.object(source_journal, "_ensure_source_row_identity"),
            patch.object(source_journal, "_create_compact_journal_table"),
            patch.object(source_journal, "_install_triggers"),
            patch.object(source_journal, "_create_batch", return_value=("batch-id", 1)),
            patch.object(source_journal, "_source_columns", return_value=[("id", "integer")]),
            patch.object(source_journal, "_complete_batch"),
            patch.object(source_journal.uuid, "uuid4", return_value="dataset-id"),
        ):
            source_journal.initialize_dataset_journal(
                connection, "source", date(2026, 1, 1)
            )

        statements = [str(call.args[0]) for call in connection.execute.call_args_list]
        truncate_index = next(i for i, sql in enumerate(statements) if sql.startswith("TRUNCATE TABLE"))
        baseline_index = next(
            i
            for i, sql in enumerate(statements)
            if 'INSERT INTO source_changes."orders"' in sql
        )
        self.assertLess(truncate_index, baseline_index)
        self.assertEqual(
            statements[truncate_index],
            'TRUNCATE TABLE "source_changes"."orders", "source_changes"."payments"',
        )
        self.assertNotIn("change_id_seq", statements[truncate_index])
        self.assertNotIn("pipeline.", statements[truncate_index])


if __name__ == "__main__":
    unittest.main()
