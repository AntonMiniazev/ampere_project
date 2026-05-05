from __future__ import annotations

import argparse
import logging
import os
from pathlib import Path
from typing import Any

from bronze_source_mapping import (
    DEFAULT_SOURCE_NAME,
    default_mapping_path_for_project,
    detect_default_project_dir,
    load_required_source_tables,
    load_source_mapping,
    validate_source_mapping,
)


LOGGER = logging.getLogger("silver.validate_bronze_sources")


def parse_args() -> argparse.Namespace:
    """Parse CLI arguments for bronze source mapping validation."""
    parser = argparse.ArgumentParser(
        description=(
            "Validate silver bronze source mapping coverage/freshness and optional "
            "DuckDB delta_scan readability."
        )
    )
    parser.add_argument(
        "--project-dir",
        default=str(detect_default_project_dir()),
        help="Path to the dbt project root. Auto-detected when omitted.",
    )
    parser.add_argument(
        "--source-name",
        default=os.getenv("BRONZE_SOURCE_NAME", DEFAULT_SOURCE_NAME),
        help="dbt source name to validate. Default: bronze",
    )
    parser.add_argument(
        "--mapping-path",
        default=os.getenv("BRONZE_SOURCE_MAPPING_PATH", ""),
        help="Generated source mapping artifact path. Auto-detected when omitted.",
    )
    parser.add_argument(
        "--max-age-hours",
        type=float,
        default=float(os.getenv("BRONZE_SOURCE_MAPPING_MAX_AGE_HOURS", "24")),
        help="Maximum allowed mapping age in hours.",
    )
    parser.add_argument(
        "--skip-delta-scan",
        action="store_true",
        help="Skip DuckDB delta_scan readability checks.",
    )
    parser.add_argument(
        "--log-level",
        default=os.getenv("SILVER_VALIDATE_LOG_LEVEL", "INFO"),
        help="Logging level (DEBUG, INFO, WARNING, ERROR). Default: INFO",
    )
    return parser.parse_args()


def setup_logging(level_name: str) -> None:
    """Configure process logging with a concise timestamped format."""
    level = getattr(logging, (level_name or "INFO").upper(), logging.INFO)
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )


def parse_bool_env(name: str, default: bool) -> bool:
    """Parse a bool-like environment variable using true/false style values."""
    raw = os.getenv(name)
    if raw is None:
        return default
    value = raw.strip().lower()
    return value in {"1", "true", "yes", "y", "on"}


def sql_literal(value: str) -> str:
    """Escape a SQL string literal for DuckDB."""
    return value.replace("'", "''")


def _read_minio_config() -> tuple[str, bool, str, str, str]:
    """Read and normalize MinIO/S3 configuration from environment."""
    endpoint = os.getenv("MINIO_S3_ENDPOINT", "minio.ampere.svc.cluster.local:9000").strip()
    use_ssl = parse_bool_env("MINIO_S3_USE_SSL", False)
    if endpoint.startswith("https://"):
        endpoint = endpoint[len("https://") :]
        use_ssl = True
    elif endpoint.startswith("http://"):
        endpoint = endpoint[len("http://") :]
        use_ssl = False

    region = os.getenv("MINIO_S3_REGION", "us-east-1").strip() or "us-east-1"
    access_key = os.getenv("MINIO_ACCESS_KEY", "").strip()
    secret_key = os.getenv("MINIO_SECRET_KEY", "").strip()
    return endpoint, use_ssl, region, access_key, secret_key


def configure_minio_environment() -> tuple[str, bool, str, str, str]:
    """Set AWS-compatible env vars before loading DuckDB extensions."""
    # Delta kernel may attempt AWS metadata lookup when creds are not explicit.
    # Disable IMDS by default to avoid long hangs in devcontainer/local runs.
    os.environ["AWS_EC2_METADATA_DISABLED"] = "true"
    # Keep metadata fallback attempts short even if a downstream library ignores the disable flag.
    os.environ["AWS_METADATA_SERVICE_NUM_ATTEMPTS"] = "1"
    os.environ["AWS_METADATA_SERVICE_TIMEOUT"] = "1"
    endpoint, use_ssl, region, access_key, secret_key = _read_minio_config()
    endpoint_with_scheme = f"{'https' if use_ssl else 'http'}://{endpoint}"

    # Delta kernel uses AWS SDK env vars; mirror MinIO config here.
    if access_key:
        os.environ["AWS_ACCESS_KEY_ID"] = access_key
    if secret_key:
        os.environ["AWS_SECRET_ACCESS_KEY"] = secret_key
    os.environ["AWS_REGION"] = region
    os.environ["AWS_DEFAULT_REGION"] = region
    os.environ["AWS_ENDPOINT_URL_S3"] = endpoint_with_scheme
    os.environ["AWS_ENDPOINT_URL"] = endpoint_with_scheme
    os.environ["AWS_S3_FORCE_PATH_STYLE"] = "true"
    if not use_ssl:
        os.environ["AWS_ALLOW_HTTP"] = "true"

    # Avoid profile-based/provider-chain detours in local/devcontainer runs.
    os.environ.pop("AWS_PROFILE", None)
    os.environ.pop("AWS_DEFAULT_PROFILE", None)
    return endpoint, use_ssl, region, access_key, secret_key


def configure_minio_settings(connection: Any) -> None:
    """Configure DuckDB S3 settings so delta_scan can read MinIO-backed tables."""
    endpoint, use_ssl, region, access_key, secret_key = _read_minio_config()
    LOGGER.info(
        "S3 config: endpoint=%s use_ssl=%s access_key_set=%s secret_key_set=%s aws_imds_disabled=%s",
        endpoint,
        use_ssl,
        bool(access_key),
        bool(secret_key),
        os.getenv("AWS_EC2_METADATA_DISABLED", ""),
    )
    LOGGER.info(
        "AWS mirror config: endpoint=%s region=%s force_path_style=%s allow_http=%s access_key_set=%s secret_key_set=%s",
        os.getenv("AWS_ENDPOINT_URL_S3", ""),
        os.getenv("AWS_REGION", ""),
        os.getenv("AWS_S3_FORCE_PATH_STYLE", ""),
        os.getenv("AWS_ALLOW_HTTP", ""),
        bool(os.getenv("AWS_ACCESS_KEY_ID")),
        bool(os.getenv("AWS_SECRET_ACCESS_KEY")),
    )

    if access_key and secret_key:
        connection.execute(
            f"""
            create or replace secret bronze_minio_s3 (
                type s3,
                provider config,
                key_id '{sql_literal(access_key)}',
                secret '{sql_literal(secret_key)}',
                region '{sql_literal(region)}',
                endpoint '{sql_literal(endpoint)}',
                use_ssl {'true' if use_ssl else 'false'},
                url_style 'path'
            )
            """
        )
        LOGGER.info(
            "Configured DuckDB S3 secret: name=bronze_minio_s3 endpoint=%s region=%s use_ssl=%s",
            endpoint,
            region,
            use_ssl,
        )
    else:
        LOGGER.warning(
            "MINIO_ACCESS_KEY/MINIO_SECRET_KEY missing; DuckDB S3 secret was not created."
        )

    connection.execute(f"set s3_region='{sql_literal(region)}'")
    connection.execute("set s3_url_style='path'")
    connection.execute(f"set s3_endpoint='{sql_literal(endpoint)}'")
    connection.execute(f"set s3_use_ssl={'true' if use_ssl else 'false'}")
    if access_key:
        connection.execute(f"set s3_access_key_id='{sql_literal(access_key)}'")
    if secret_key:
        connection.execute(f"set s3_secret_access_key='{sql_literal(secret_key)}'")


def validate_delta_scan_readability(table_to_row: dict[str, dict[str, object]]) -> list[str]:
    """Run a small delta_scan probe against each mapped source location."""
    import duckdb  # noqa: PLC0415

    failures: list[str] = []
    total = len(table_to_row)
    LOGGER.info("Starting delta_scan readability checks for %d table(s).", total)
    configure_minio_environment()
    connection = duckdb.connect(":memory:")
    for extension in ("httpfs", "aws", "delta"):
        connection.execute(f"install {extension}")
        connection.execute(f"load {extension}")
    configure_minio_settings(connection)

    for index, (table_name, row) in enumerate(sorted(table_to_row.items()), start=1):
        storage_location = str(row.get("storage_location") or "").strip()
        LOGGER.info(
            "delta_scan progress [%d/%d]: table=%s path=%s",
            index,
            total,
            table_name,
            storage_location,
        )
        try:
            connection.execute(
                f"select 1 from delta_scan('{sql_literal(storage_location)}') limit 1"
            )
            LOGGER.debug("delta_scan succeeded for table=%s", table_name)
        except Exception as exc:  # noqa: BLE001
            LOGGER.exception("delta_scan failed for table=%s", table_name)
            failures.append(
                f"{table_name}: delta_scan failed for {storage_location!r}: {exc}"
            )
    LOGGER.info("Completed delta_scan checks with %d failure(s).", len(failures))
    return failures


def main() -> None:
    """Validate that bronze source mapping is complete, fresh, and readable."""
    args = parse_args()
    setup_logging(args.log_level)
    project_dir = Path(args.project_dir)
    mapping_path = (
        Path(args.mapping_path)
        if (args.mapping_path or "").strip()
        else default_mapping_path_for_project(project_dir)
    )
    LOGGER.info(
        "Starting bronze source validation: project_dir=%s mapping_path=%s source_name=%s skip_delta_scan=%s max_age_hours=%s",
        project_dir,
        mapping_path,
        args.source_name,
        args.skip_delta_scan,
        args.max_age_hours,
    )

    required_tables = load_required_source_tables(project_dir, args.source_name)
    LOGGER.info("Loaded %d required source table(s) from dbt source config.", len(required_tables))
    mapping_rows = load_source_mapping(mapping_path)
    LOGGER.info("Loaded mapping artifact rows: %d", len(mapping_rows))
    table_to_row, failures = validate_source_mapping(
        mapping_rows=mapping_rows,
        required_tables=required_tables,
        source_name=args.source_name,
        max_age_hours=args.max_age_hours,
    )
    LOGGER.info(
        "Mapping validation result: resolved_tables=%d failure_count=%d",
        len(table_to_row),
        len(failures),
    )

    if not args.skip_delta_scan and not failures:
        failures.extend(validate_delta_scan_readability(table_to_row))
    elif args.skip_delta_scan:
        LOGGER.info("Skipping delta_scan readability checks by request.")

    if failures:
        LOGGER.error("Bronze validation failed with %d failure(s).", len(failures))
        raise SystemExit("Bronze validation failed:\n- " + "\n- ".join(failures))

    LOGGER.info("Bronze validation finished successfully.")
    print(
        "Validated bronze source mapping:",
        f"tables={len(table_to_row)}",
        f"mapping={mapping_path}",
        f"delta_scan={'skipped' if args.skip_delta_scan else 'ok'}",
    )


if __name__ == "__main__":
    main()
