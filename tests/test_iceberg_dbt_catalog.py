"""Prove dbt v2 can attach all three Lakekeeper warehouse aliases."""

from __future__ import annotations

import importlib.util
import json
import os
import subprocess
import sys
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
                    "DUCKDB_MAX_TEMP_DIRECTORY_SIZE": "2GB",
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
                sys.path.insert(0, str(module_path.parent))
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
                    self.assertEqual(
                        duckdb_settings["temp_directory"],
                        str(Path(temp_dir) / "duckdb_tmp"),
                    )
                    self.assertEqual(duckdb_settings["max_temp_directory_size"], "2GB")
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

                selected = subprocess.run(
                    ["dbt", "ls", "--project-dir", str(ROOT / "dbt_iceberg"),
                     "--profiles-dir", settings["DBT_PROFILES_DIR"],
                     "--select", "tag:gold", "--resource-type", "test"],
                    capture_output=True, text=True, timeout=30, check=False,
                )
                self.assertEqual(selected.returncode, 0, selected.stdout + selected.stderr)
                self.assertIn("gold_sales_margin_consistency", selected.stdout)

                # The daily run builds Silver/Gold into pod-local DuckDB files
                # before a separate publisher touches the Iceberg catalogs.
                with patch.dict(os.environ, settings | {"ICEBERG_PUBLISH_MODE": "staged"}):
                    module.prepare()
                    staged = yaml.safe_load(profile.read_text())["ampere_iceberg_project"]["outputs"]["prod"]
                    attaches = {item["alias"]: item for item in staged["attach"]}
                    self.assertEqual(attaches["iceberg_bronze"]["type"], "iceberg")
                    for layer in ("silver", "gold"):
                        attachment = attaches[f"iceberg_{layer}"]
                        self.assertNotIn("type", attachment)
                        self.assertEqual(
                            Path(attachment["path"]).name, f"staged_{layer}.duckdb"
                        )
                    completed = subprocess.run(
                        ["dbt", "debug", "--project-dir", str(ROOT / "dbt_iceberg"),
                         "--profiles-dir", settings["DBT_PROFILES_DIR"]],
                        capture_output=True, text=True, timeout=30, check=False,
                    )
                    self.assertEqual(completed.returncode, 0, completed.stdout + completed.stderr)

                    completed = subprocess.run(
                        [
                            "dbt", "build", "--project-dir", str(ROOT / "dbt_iceberg"),
                            "--profiles-dir", settings["DBT_PROFILES_DIR"],
                            "--target-path", str(Path(temp_dir) / "target-budget"),
                            "--select", "silver_budget_orders_sales", "budget_orders_sales",
                        ],
                        env=os.environ | {
                            "BUDGET_DAILY_CSV_PATH": str(
                                ROOT / "tools/budget_generation/budget_parameters_daily.csv"
                            )
                        },
                        capture_output=True, text=True, timeout=60, check=False,
                    )
                    self.assertEqual(completed.returncode, 0, completed.stdout + completed.stderr)

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
                            "stg_order_product",
                            "stg_payments",
                            "stg_order_status_history",
                            "stg_delivery_tracking",
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
                        self.assertNotIn("where", silver_sql)
                        self.assertIn("and true", gold_sql)
                    else:
                        self.assertIn("interval '7 day'", silver_sql)
                        self.assertIn("interval '7 day'", gold_sql)
                    for table in (
                        "stg_order_product", "stg_payments",
                        "stg_order_status_history", "stg_delivery_tracking",
                    ):
                        event_sql = (
                            target_path / "compiled/ampere_iceberg_project/models/staging/"
                            f"{table}.sql"
                        ).read_text(encoding="utf-8").lower()
                        if mode == "full_history":
                            self.assertIn("where true", event_sql)
                            self.assertNotIn("in (select order_id from", event_sql)
                        else:
                            self.assertIn("in (select order_id from", event_sql)

                with patch.dict(os.environ, settings | {"ICEBERG_PUBLISH_MODE": "staged"}):
                    target_path = Path(temp_dir) / "target-staged"
                    completed = subprocess.run(
                        [
                            "dbt", "compile", "--project-dir", str(ROOT / "dbt_iceberg"),
                            "--profiles-dir", settings["DBT_PROFILES_DIR"],
                            "--target-path", str(target_path),
                            "--select", "stg_order_product", "fct_orders_sales_mart",
                        ],
                        capture_output=True, text=True, timeout=60, check=False,
                    )
                    self.assertEqual(completed.returncode, 0, completed.stdout + completed.stderr)
                    staged_sql = (
                        target_path / "compiled/ampere_iceberg_project/models/gold/marts/"
                        "fct_orders_sales_mart.sql"
                    ).read_text(encoding="utf-8").lower()
                    self.assertIn("and true", staged_sql)
                    product_sql = (
                        target_path / "compiled/ampere_iceberg_project/models/staging/"
                        "stg_order_product.sql"
                    ).read_text(encoding="utf-8").lower()
                    self.assertIn("stg_orders", product_sql)
        finally:
            server.shutdown()
            server.server_close()


if __name__ == "__main__":
    unittest.main()
