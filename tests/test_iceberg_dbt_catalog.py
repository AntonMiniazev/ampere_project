"""Prove dbt v2 can attach all three Lakekeeper warehouse aliases."""

from __future__ import annotations

import importlib.util
import os
import subprocess
import tempfile
import threading
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from unittest.mock import patch


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
                    "DUCKDB_PATH": str(Path(temp_dir) / "work.duckdb"),
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
        finally:
            server.shutdown()
            server.server_close()


if __name__ == "__main__":
    unittest.main()
