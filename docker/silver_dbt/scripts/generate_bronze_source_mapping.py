from __future__ import annotations

import argparse
import os
from pathlib import Path

from bronze_source_mapping import (
    DEFAULT_SOURCE_NAME,
    build_source_mapping_rows,
    default_mapping_path_for_project,
    default_uc_api_uri_for_project,
    detect_default_project_dir,
    load_required_source_tables,
    normalize_uc_api_uri,
    write_source_mapping,
)


def parse_args() -> argparse.Namespace:
    """Parse CLI arguments for UC bronze source mapping generation."""
    parser = argparse.ArgumentParser(
        description=(
            "Generate dbt bronze source mapping from Unity Catalog metadata "
            "(table -> storage_location)."
        )
    )
    parser.add_argument(
        "--project-dir",
        default=str(detect_default_project_dir()),
        help="Path to dbt project root. Auto-detected when omitted.",
    )
    parser.add_argument(
        "--output",
        default=os.getenv("BRONZE_SOURCE_MAPPING_PATH", ""),
        help="Output JSON mapping artifact path. Auto-detected when omitted.",
    )
    parser.add_argument(
        "--source-name",
        default=os.getenv("BRONZE_SOURCE_NAME", DEFAULT_SOURCE_NAME),
        help="dbt source name to resolve from _sources.yml. Default: bronze",
    )
    parser.add_argument(
        "--uc-api-uri",
        default=os.getenv("UC_API_URI", ""),
        help="Unity Catalog API base URI. Auto-detected when omitted.",
    )
    parser.add_argument(
        "--uc-token",
        default=os.getenv("UC_TOKEN", "not-used"),
        help="Unity Catalog API token. Optional when UC allows anonymous access.",
    )
    parser.add_argument(
        "--catalog",
        default=os.getenv("BRONZE_UC_CATALOG", "ampere"),
        help="Unity Catalog catalog name for bronze tables.",
    )
    parser.add_argument(
        "--schema",
        default=os.getenv("BRONZE_UC_SCHEMA", "bronze"),
        help="Unity Catalog schema name for bronze tables.",
    )
    return parser.parse_args()


def main() -> None:
    """Generate and persist a UC-derived bronze source mapping artifact."""
    args = parse_args()
    project_dir = Path(args.project_dir)
    output_path = (
        Path(args.output)
        if (args.output or "").strip()
        else default_mapping_path_for_project(project_dir)
    )
    uc_api_uri = (args.uc_api_uri or "").strip() or default_uc_api_uri_for_project(project_dir)
    base_uri = normalize_uc_api_uri(uc_api_uri)
    table_names = load_required_source_tables(project_dir, args.source_name)

    rows = build_source_mapping_rows(
        base_uri=base_uri,
        token=args.uc_token,
        catalog=args.catalog,
        schema=args.schema,
        source_name=args.source_name,
        table_names=table_names,
    )
    write_source_mapping(rows, output_path)

    print(
        "Generated bronze source mapping:",
        f"tables={len(rows)}",
        f"source={args.source_name}",
        f"catalog={args.catalog}",
        f"schema={args.schema}",
        f"output={output_path}",
    )


if __name__ == "__main__":
    main()
