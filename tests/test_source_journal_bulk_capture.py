"""Keep journal capture set-based for COPY and multi-row mutations."""

from __future__ import annotations

import sys
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "docker/init_source_preparation/model"))

import source_journal  # noqa: E402


class SourceJournalBulkCaptureTests(unittest.TestCase):
    def test_installs_one_statement_trigger_per_write_operation(self) -> None:
        connection = MagicMock()
        connection.execute.return_value.scalars.return_value.all.return_value = [
            "orders"
        ]

        with patch.object(source_journal, "_install_capture_function"):
            source_journal._install_triggers(connection, "source")

        statements = [call.args[0].text for call in connection.execute.call_args_list]
        inserts = [
            sql
            for sql in statements
            if "CREATE TRIGGER capture_source_change_insert" in sql
        ]
        updates = [
            sql
            for sql in statements
            if "CREATE TRIGGER capture_source_change_update" in sql
        ]
        deletes = [
            sql
            for sql in statements
            if "CREATE TRIGGER capture_source_change_delete" in sql
        ]
        self.assertEqual(len(inserts), 1)
        self.assertIn("REFERENCING NEW TABLE AS new_rows", inserts[0])
        self.assertIn("FOR EACH STATEMENT", inserts[0])
        self.assertEqual(len(updates), 1)
        self.assertIn(
            "REFERENCING OLD TABLE AS old_rows NEW TABLE AS new_rows", updates[0]
        )
        self.assertIn("FOR EACH STATEMENT", updates[0])
        self.assertEqual(len(deletes), 1)
        self.assertIn("REFERENCING OLD TABLE AS old_rows", deletes[0])
        self.assertIn("FOR EACH STATEMENT", deletes[0])

    def test_capture_function_inserts_rows_and_updates_counts_per_statement(
        self,
    ) -> None:
        connection = MagicMock()

        source_journal._install_capture_function(connection)

        function_sql = connection.execute.call_args.args[0].text
        self.assertIn("TG_LEVEL <> 'STATEMENT'", function_sql)
        self.assertIn("GET DIAGNOSTICS v_change_count = ROW_COUNT", function_sql)
        self.assertIn("change_count = change_count + v_change_count", function_sql)
        self.assertIn("JOIN old_rows o USING (source_row_id)", function_sql)
        self.assertIn("source_row_id", function_sql)
        self.assertNotIn("_before_", function_sql)
        self.assertNotIn("v_source_targets", function_sql)
        self.assertNotIn("FOR v_column", function_sql)
        self.assertNotIn("information_schema.columns", function_sql)


if __name__ == "__main__":
    unittest.main()
