from __future__ import annotations

import argparse
import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.error import HTTPError
from urllib.parse import quote
from urllib.request import Request, urlopen

import yaml


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Create a live dbt source mapping from Unity Catalog metadata."
    )
    parser.add_argument("--project-dir", default=os.getenv("DBT_PROJECT_DIR", "/app/dbt"))
    parser.add_argument("--source-file", default="models/staging/_sources.yml")
    parser.add_argument("--source-name", required=True)
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--schema", required=True)
    parser.add_argument("--uc-api-uri", default=os.getenv("UC_API_URI", "http://ucatalog.local"))
    parser.add_argument("--uc-token", default=os.getenv("UC_TOKEN", "not-used"))
    parser.add_argument("--output", required=True)
    return parser.parse_args()


def source_tables(project_dir: Path, source_file: str, source_name: str) -> list[str]:
    source_path = project_dir / source_file
    if not source_path.exists():
        raise FileNotFoundError(f"dbt source file not found: {source_path}")

    payload = yaml.safe_load(source_path.read_text(encoding="utf-8")) or {}
    for source in payload.get("sources", []):
        if isinstance(source, dict) and source.get("name") == source_name:
            tables = source.get("tables", [])
            return sorted(
                {
                    str(table.get("name")).strip()
                    for table in tables
                    if isinstance(table, dict) and str(table.get("name") or "").strip()
                }
            )
    raise RuntimeError(f"Source {source_name!r} not found in {source_path}")


def uc_get_table(base_uri: str, token: str, catalog: str, schema: str, table: str) -> dict[str, Any]:
    fq_name = quote(f"{catalog}.{schema}.{table}", safe=".")
    request = Request(
        f"{base_uri.rstrip('/')}/api/2.1/unity-catalog/tables/{fq_name}",
        headers={"Accept": "application/json"},
        method="GET",
    )
    if token and token.lower() != "not-used":
        request.add_header("Authorization", f"Bearer {token}")

    try:
        with urlopen(request, timeout=30) as response:
            return json.loads(response.read().decode("utf-8"))
    except HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(
            f"UC table metadata request failed for {catalog}.{schema}.{table}: "
            f"HTTP {exc.code}: {body}"
        ) from exc


def build_mapping(args: argparse.Namespace) -> list[dict[str, str]]:
    project_dir = Path(args.project_dir)
    generated_at = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
    rows: list[dict[str, str]] = []
    for table in source_tables(project_dir, args.source_file, args.source_name):
        payload = uc_get_table(args.uc_api_uri, args.uc_token, args.catalog, args.schema, table)
        storage_location = str(payload.get("storage_location") or "").strip()
        table_type = str(payload.get("table_type") or "").strip().upper()
        data_source_format = str(payload.get("data_source_format") or "").strip().upper()
        if not storage_location:
            raise RuntimeError(f"{args.catalog}.{args.schema}.{table} has no storage_location.")
        if table_type and table_type != "EXTERNAL":
            raise RuntimeError(
                f"{args.catalog}.{args.schema}.{table} table_type={table_type!r}; expected EXTERNAL."
            )
        if data_source_format and data_source_format != "DELTA":
            raise RuntimeError(
                f"{args.catalog}.{args.schema}.{table} data_source_format={data_source_format!r}; "
                "expected DELTA."
            )
        rows.append(
            {
                "source_name": args.source_name,
                "table_name": table,
                "uc_catalog": args.catalog,
                "uc_schema": args.schema,
                "table_type": table_type,
                "data_source_format": data_source_format,
                "storage_location": storage_location,
                "generated_at_utc": generated_at,
                "uc_api_uri": args.uc_api_uri.rstrip("/"),
            }
        )
    return rows


def main() -> None:
    args = parse_args()
    rows = build_mapping(args)
    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(rows, indent=2) + "\n", encoding="utf-8")
    print(
        "Created UC source mapping: "
        f"source={args.source_name} rows={len(rows)} output={output}"
    )


if __name__ == "__main__":
    main()
