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
    def test_wide_journal_migration_keeps_only_four_compact_fields(self) -> None:
        connection = MagicMock()
        connection.execute.return_value.scalars.return_value.all.return_value = [
            "id", "_batch_id", "_change_id", "_source_row_id", "_op"
        ]

        with patch.object(
            source_journal, "_source_columns", return_value=[("id", "integer")]
        ):
            source_journal._create_compact_journal_table(connection, "source", "orders")

        statements = [call.args[0].text for call in connection.execute.call_args_list]
        self.assertTrue(any("CREATE TABLE source_changes.\"compact_orders\"" in sql for sql in statements))
        self.assertTrue(any("SELECT _batch_id::uuid,_change_id,_source_row_id::uuid,_op" in sql for sql in statements))
        self.assertTrue(any('DROP TABLE source_changes."orders"' in sql for sql in statements))
        self.assertTrue(any('RENAME TO "orders"' in sql for sql in statements))
        self.assertTrue(any("(batch_id, change_id)" in sql for sql in statements))

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
            patch.object(source_journal, "_create_compact_journal_table") as ensure_journal,
            patch.object(source_journal, "_install_triggers") as install_triggers,
            patch.object(source_journal, "_delete_inactive_source_change_metadata") as prune_inactive,
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
            patch.object(source_journal, "_create_compact_journal_table"),
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
