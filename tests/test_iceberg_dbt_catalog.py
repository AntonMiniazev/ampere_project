"""Prove dbt v2 can attach all three Lakekeeper warehouse aliases."""

from __future__ import annotations

import importlib.util
import json
import os
import subprocess
import tempfile
import threading
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from unittest.mock import patch

import yaml


ROOT = Path(__file__).resolve().parents[1]


class CatalogHandler(BaseHTTPRequestHandler):
    """Serve the minimal OAuth and Iceberg REST config exchanges."""

    def do_POST(self) -> None:  # noqa: N802
        """Issue a test-only OAuth token."""
        if self.path != "/token":
            self.send_error(404)
            return
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(b'{"access_token":"dummy","token_type":"Bearer","expires_in":3600}')

    def do_GET(self) -> None:  # noqa: N802
        """Return an empty Iceberg catalog configuration."""
        if not self.path.startswith("/catalog/v1/config?warehouse="):
            self.send_error(404)
            return
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(b'{"defaults":{},"overrides":{}}')

    def log_message(self, *_args: object) -> None:
        """Suppress expected local HTTP request logs."""


@unittest.skipUnless(os.getenv("ICEBERG_DBT_SMOKE"), "dbt v2 runtime not requested")
class DuckDBCatalogTests(unittest.TestCase):
    """Check secret and profile compatibility with the dbt ADBC driver."""

    def test_prepare_and_attach_catalogs(self) -> None:
        """Prepare pod-local secrets and attach three mock warehouses."""
        server = ThreadingHTTPServer(("127.0.0.1", 0), CatalogHandler)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            with tempfile.TemporaryDirectory() as temp_dir:
                endpoint = f"http://127.0.0.1:{server.server_port}"
                settings = {
                    "DUCKDB_SECRET_DIRECTORY": str(Path(temp_dir) / "secrets"),
                    "DBT_PROFILES_DIR": str(Path(temp_dir) / "profiles"),
                    "DUCKDB_PATH": str(Path(temp_dir) / "ampere_work.duckdb"),
                    "DUCKDB_WORKER_THREADS": "2",
                    "DUCKDB_PRESERVE_INSERTION_ORDER": "false",
                    "MINIO_S3_ENDPOINT": "http://127.0.0.1:9000",
                    "MINIO_ACCESS_KEY": "dummy-access",
                    "MINIO_SECRET_KEY": "dummy-secret",
                    "LAKEKEEPER_CATALOG_URI": endpoint + "/catalog",
                    "LAKEKEEPER_OAUTH_URI": endpoint + "/token",
                    "LAKEKEEPER_SCOPE": "dummy-scope",
                    "LAKEKEEPER_CLIENT_ID": "dummy-client",
                    "LAKEKEEPER_CLIENT_SECRET": "dummy-secret",
                }
                module_path = ROOT / "docker/dbt_iceberg/prepare_catalog.py"
                spec = importlib.util.spec_from_file_location("prepare_catalog", module_path)
                module = importlib.util.module_from_spec(spec)
                spec.loader.exec_module(module)
                with patch.dict(os.environ, settings):
                    module.prepare()
                    profile = Path(settings["DBT_PROFILES_DIR"]) / "profiles.yml"
                    self.assertNotIn("dummy-secret", profile.read_text())
                    config = yaml.safe_load(profile.read_text())
                    workspace = config["ampere_iceberg_project"]["outputs"]["prod"]["path"]
                    self.assertEqual(Path(workspace).stem, "ampere_work")
                    duckdb_settings = config["ampere_iceberg_project"]["outputs"]["prod"]["settings"]
                    self.assertEqual(duckdb_settings["threads"], 2)
                    self.assertIs(duckdb_settings["preserve_insertion_order"], False)
                    completed = subprocess.run(
                        [
                            "dbt",
                            "debug",
                            "--project-dir",
                            str(ROOT / "dbt_iceberg"),
                            "--profiles-dir",
                            settings["DBT_PROFILES_DIR"],
                        ],
                        capture_output=True,
                        text=True,
                        timeout=30,
                        check=False,
                    )
                self.assertEqual(completed.returncode, 0, completed.stdout + completed.stderr)
                for layer in ("bronze", "silver", "gold"):
                    self.assertIn(f"iceberg_{layer}", completed.stdout)

                # Compile representative Silver and Gold models in each mode
                # to verify full_history removes both layers' date windows.
                for mode in ("daily_refresh", "full_history"):
                    target_path = Path(temp_dir) / f"target-{mode}"
                    completed = subprocess.run(
                        [
                            "dbt",
                            "compile",
                            "--project-dir",
                            str(ROOT / "dbt_iceberg"),
                            "--profiles-dir",
                            settings["DBT_PROFILES_DIR"],
                            "--target-path",
                            str(target_path),
                            "--select",
                            "stg_orders",
                            "fct_orders_sales_mart",
                            "--vars",
                            json.dumps(
                                {
                                    "silver_run_mode": mode,
                                    "gold_run_mode": mode,
                                    "silver_lookback_days": 7,
                                    "gold_lookback_days": 7,
                                }
                            ),
                        ],
                        capture_output=True,
                        text=True,
                        timeout=60,
                        check=False,
                    )
                    self.assertEqual(
                        completed.returncode,
                        0,
                        completed.stdout + completed.stderr,
                    )

                    silver_sql = (
                        target_path
                        / "compiled/ampere_iceberg_project/models/staging/stg_orders.sql"
                    ).read_text(encoding="utf-8").lower()
                    gold_sql = (
                        target_path
                        / "compiled/ampere_iceberg_project/models/gold/marts/"
                        "fct_orders_sales_mart.sql"
                    ).read_text(encoding="utf-8").lower()
                    if mode == "full_history":
                        self.assertNotIn("interval '7 day'", silver_sql)
                        self.assertNotIn("interval '7 day'", gold_sql)
                        self.assertIn("where true", silver_sql)
                        self.assertIn("where true", gold_sql)
                    else:
                        self.assertIn("interval '7 day'", silver_sql)
                        self.assertIn("interval '7 day'", gold_sql)
        finally:
            server.shutdown()
            server.server_close()


if __name__ == "__main__":
    unittest.main()
