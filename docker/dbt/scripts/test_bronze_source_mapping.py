from __future__ import annotations

import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch

from bronze_source_mapping import (
    build_source_mapping_rows,
    load_required_source_tables,
    validate_source_mapping,
)


class BronzeSourceMappingTests(unittest.TestCase):
    """Unit tests for UC bronze source mapping helpers."""

    def _create_source_file(self, root: Path) -> None:
        source_dir = root / "models" / "staging"
        source_dir.mkdir(parents=True, exist_ok=True)
        source_dir.joinpath("_sources.yml").write_text(
            (
                "version: 2\n"
                "sources:\n"
                "  - name: bronze\n"
                "    tables:\n"
                "      - name: stores\n"
                "      - name: orders\n"
            ),
            encoding="utf-8",
        )

    def test_load_required_source_tables(self) -> None:
        """The helper should read table names from dbt source definitions."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir)
            self._create_source_file(root)
            tables = load_required_source_tables(root, "bronze")
            self.assertEqual(tables, ["orders", "stores"])

    def test_build_source_mapping_rows(self) -> None:
        """UC payload rows should be converted into sorted mapping entries."""
        fake_payload = {
            "table_type": "EXTERNAL",
            "data_source_format": "DELTA",
            "storage_location": "s3://ampere-bronze/bronze/source/stores",
        }
        with patch("bronze_source_mapping.uc_get_json", return_value=fake_payload):
            rows = build_source_mapping_rows(
                base_uri="http://ucatalog.local",
                token="not-used",
                catalog="ampere",
                schema="bronze",
                source_name="bronze",
                table_names=["stores"],
            )
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["table_name"], "stores")
        self.assertEqual(
            rows[0]["storage_location"], "s3://ampere-bronze/bronze/source/stores"
        )

    def test_validate_source_mapping_detects_stale_and_missing(self) -> None:
        """Validation should fail when mapping is stale or does not cover all tables."""
        stale_time = (
            datetime.now(timezone.utc) - timedelta(hours=30)
        ).replace(microsecond=0).isoformat()
        rows = [
            {
                "source_name": "bronze",
                "table_name": "stores",
                "storage_location": "s3://bucket/stores",
                "generated_at_utc": stale_time,
            }
        ]
        table_to_row, failures = validate_source_mapping(
            mapping_rows=rows,
            required_tables=["stores", "orders"],
            source_name="bronze",
            max_age_hours=24,
        )
        self.assertIn("stores", table_to_row)
        self.assertTrue(any("Missing mapping rows" in item for item in failures))
        self.assertTrue(any("stale" in item for item in failures))


if __name__ == "__main__":
    unittest.main()
