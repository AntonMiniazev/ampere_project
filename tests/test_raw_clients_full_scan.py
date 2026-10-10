from __future__ import annotations

import logging
import sys
import unittest
from datetime import date
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "docker/spark/iceberg_raw_etl/app"))
sys.path.insert(0, str(ROOT))

from raw.mutable_dims import build_mutable_dim_plan  # noqa: E402
from dags.utils.stream_group_config import build_raw_stream_groups  # noqa: E402


class RawClientsFullScanTests(unittest.TestCase):
    def test_clients_is_configured_for_full_current_state_extract(self) -> None:
        mutable_dims = next(
            group for group in build_raw_stream_groups() if group["group"] == "mutable_dims"
        )

        self.assertTrue(mutable_dims["table_config"]["clients"]["full_scan"])
        self.assertFalse(mutable_dims["table_config"]["costing"]["full_scan"])

    def test_full_scan_ignores_existing_watermark_state(self) -> None:
        args = SimpleNamespace(
            bucket="ampere-raw",
            source_system="postgres-pre-raw",
            schema="source",
            watermark_from="",
            watermark_to="",
        )
        state = {
            "last_updated_at": "2026-10-09",
            "last_created_at": "2026-10-09",
        }
        with (
            patch("raw.mutable_dims.state_path", return_value="s3a://raw/state.json"),
            patch("raw.mutable_dims.read_json", return_value=state),
        ):
            plan = build_mutable_dim_plan(
                spark=object(),
                logger=logging.getLogger("raw-clients-full-scan-test"),
                group_name="mutable_dims",
                group_mode="incremental",
                group_partition_key="extract_date",
                snapshot_partitioned=True,
                output_base="s3a://ampere-raw/postgres-pre-raw/source/clients",
                table="clients",
                table_meta={
                    "watermark_column": "updated_at",
                    "created_column": "registration_date",
                    "cursor_granularity": "date",
                    "full_scan": True,
                },
                group_event_col=None,
                group_lookback_days=0,
                group_watermark_col=None,
                args=args,
                run_date=date(2026, 10, 10),
            )

        self.assertTrue(plan.full_scan)
        self.assertIsNone(plan.where_clause)
        self.assertEqual(plan.dbtable, '"source"."clients"')


if __name__ == "__main__":
    unittest.main()
