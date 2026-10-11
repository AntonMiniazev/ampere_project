"""Checks bounded, multi-group PostgreSQL-to-Raw transfer selection."""

from __future__ import annotations

import sys
import unittest
from pathlib import Path
from unittest.mock import MagicMock, call, patch


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "docker/spark/iceberg_raw_etl/app"))
sys.path.insert(0, str(ROOT))

import journal_to_raw  # noqa: E402


def _transfer(size: int, first_sequence: int = 1) -> dict:
    return {"batches": [(str(i), first_sequence + i) for i in range(size)]}


class RawJournalTransferTests(unittest.TestCase):
    def test_raw_contract_uses_typed_parquet_and_manifest_visibility(self) -> None:
        spec = journal_to_raw.CONTRACT.table("raw", "order_product")
        self.assertEqual(spec.ingestion["kind"], "staged_typed_parquet_manifest")
        self.assertEqual(spec.ingestion["staging"]["file_format"], "parquet")
        self.assertEqual(spec.ingestion["staging"]["commit_marker"], "COMMITTED.json")
        self.assertEqual(spec.ingestion["staging"]["max_rows_per_chunk"], 50000)
        self.assertNotIn("source_changes", Path(journal_to_raw.__file__).read_text(encoding="utf-8"))

    def test_requested_backlog_is_processed_in_groups_no_larger_than_96(self) -> None:
        transfers = [_transfer(96, 1), _transfer(96, 97), _transfer(96, 193), _transfer(17, 289)]
        with (
            patch.object(journal_to_raw, "_complete_transfer") as complete,
            patch.object(journal_to_raw, "_load_eligible_manifests", return_value={}),
            patch.object(
                journal_to_raw,
                "reserve_transfer",
                side_effect=transfers[1:],
            ) as reserve,
        ):
            processed = journal_to_raw.transfer_backlog(
                MagicMock(),
                transfers[0],
                max_batches=305,
                batch_size=96,
            )

        self.assertEqual(processed, 305)
        self.assertEqual(complete.call_count, 4)
        self.assertEqual(reserve.call_args_list, [call(96), call(96), call(17)])

    def test_stops_when_no_more_batches_are_available(self) -> None:
        with (
            patch.object(journal_to_raw, "_complete_transfer") as complete,
            patch.object(journal_to_raw, "_load_eligible_manifests", return_value={}),
            patch.object(journal_to_raw, "reserve_transfer", return_value=None) as reserve,
        ):
            processed = journal_to_raw.transfer_backlog(
                MagicMock(),
                _transfer(31),
                max_batches=305,
                batch_size=96,
            )

        self.assertEqual(processed, 31)
        complete.assert_called_once()
        reserve.assert_called_once_with(96)

    def test_batch_group_limit_rejects_values_above_96(self) -> None:
        with self.assertRaisesRegex(ValueError, "1..96"):
            journal_to_raw.reserve_transfer(97)

    def test_started_transfer_does_not_exceed_this_runs_max_batches(self) -> None:
        transfer = _transfer(8)
        with self.assertRaisesRegex(RuntimeError, "max_batches limit"):
            journal_to_raw.transfer_backlog(
                MagicMock(), transfer, max_batches=4, batch_size=4
            )

    def test_high_water_mark_reads_indexed_transfer_batches_directly(self) -> None:
        cursor = MagicMock()
        cursor.fetchone.return_value = (128,)

        result = journal_to_raw._last_acknowledged_batch(cursor, "dataset-id")

        self.assertEqual(result, 128)
        sql, params = cursor.execute.call_args.args
        self.assertIn("MAX(batch_sequence)", sql)
        self.assertIn("pipeline.raw_transfer_batches WHERE dataset_id=%s", sql)
        self.assertNotIn("JOIN pipeline.batches", sql)
        self.assertEqual(params, ("dataset-id",))


if __name__ == "__main__":
    unittest.main()
