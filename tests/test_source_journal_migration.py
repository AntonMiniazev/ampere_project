"""Focused checks for non-destructive source journal migration."""

from __future__ import annotations

import sys
import unittest
from datetime import date
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "docker/init_source_preparation/model"))

import source_journal  # noqa: E402


class SourceJournalMigrationTests(unittest.TestCase):
    def test_ready_dataset_is_migrated_without_new_baseline_or_active_row_deletion(self) -> None:
        connection = MagicMock()
        connection.execute.return_value.first.return_value = SimpleNamespace(
            dataset_id="existing-dataset", source_schema="source"
        )

        with (
            patch.object(source_journal, "SOURCE_TABLES", ("orders",)),
            patch.object(source_journal, "_ensure_control_schema") as ensure_control,
            patch.object(source_journal, "_source_columns", return_value=[("id", "integer")]),
            patch.object(source_journal, "_ensure_source_row_identity") as ensure_identity,
            patch.object(source_journal, "_create_typed_journal_table") as ensure_journal,
            patch.object(source_journal, "_install_triggers") as install_triggers,
            patch.object(source_journal, "_delete_inactive_source_change_payload") as prune_inactive,
            patch.object(source_journal, "initialize_dataset_journal") as create_baseline,
        ):
            result = source_journal.migrate_existing_source_journal(
                connection, "source", date(2026, 1, 1)
            )

        self.assertEqual(result, "existing-dataset")
        ensure_control.assert_called_once_with(connection)
        ensure_identity.assert_called_once_with(connection, "source", "orders")
        ensure_journal.assert_called_once_with(connection, "source", "orders")
        install_triggers.assert_called_once_with(connection, "source")
        prune_inactive.assert_called_once_with(connection)
        create_baseline.assert_not_called()

    def test_missing_dataset_uses_existing_rows_as_a_new_baseline(self) -> None:
        connection = MagicMock()
        connection.execute.return_value.first.return_value = None
        connection.execute.return_value.scalar_one_or_none.return_value = date(2026, 1, 31)
        with (
            patch.object(source_journal, "SOURCE_TABLES", ("orders",)),
            patch.object(source_journal, "_ensure_control_schema"),
            patch.object(source_journal, "_source_columns", return_value=[("id", "integer")]),
            patch.object(source_journal, "_ensure_source_row_identity"),
            patch.object(source_journal, "_create_typed_journal_table"),
            patch.object(source_journal, "initialize_dataset_journal", return_value="new-dataset") as baseline,
        ):
            result = source_journal.migrate_existing_source_journal(
                connection,
                "source",
                date(2026, 1, 1),
                source_code_version="test-image",
            )

        self.assertEqual(result, "new-dataset")
        baseline.assert_called_once_with(
            connection,
            "source",
            date(2026, 2, 1),
            source_code_version="test-image",
        )


if __name__ == "__main__":
    unittest.main()
