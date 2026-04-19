"""Extract source tables to raw landing (parquet + manifest + success marker)."""

from __future__ import annotations

import logging
import uuid
from datetime import date, datetime, timezone

from pyspark.sql import SparkSession

from etl_utils import (
    configure_s3,
    get_env,
    get_minio_connection_settings,
    get_postgres_connection_settings,
    sanitize_run_id,
    set_spark_log_level,
    setup_logging,
    table_base_path,
)
from raw.common import (
    add_ingest_columns,
    apply_group_shuffle,
    build_manifest_base,
    hash_schema_json,
)
from raw.dispatcher import build_table_plan, write_table
from etl_parser import parse_raw_args, resolve_raw_groups

APP_NAME = "source-to-raw-etl"


def main() -> None:
    """Extract source tables to raw landing parquet with manifests and state."""
    # Step 1: Initialize logging and parse CLI inputs for the run.
    # This makes sure run_date, mode, and partition settings are resolved consistently.
    # The expected outcome is a stable run_id and validated arguments before work begins.
    setup_logging()
    logger = logging.getLogger(APP_NAME)

    args = parse_raw_args()
    run_date_str = args.run_date or get_env("RUN_DATE") or date.today().isoformat()
    run_date = date.fromisoformat(run_date_str)
    run_id = sanitize_run_id(args.run_id or str(uuid.uuid4()))

    # Step 2: Resolve per-run configuration and credentials.
    # This prepares event and watermark settings and verifies DB and MinIO secrets.
    # The expected outcome is non-empty credentials and normalized inputs.
    event_date_column = args.event_date_column.strip() or None
    watermark_column = args.watermark_column.strip() or None
    lookback_days = max(args.lookback_days, 0)
    jdbc_fetchsize = max(args.jdbc_fetchsize, 1)

    pg_settings = get_postgres_connection_settings()
    minio_settings = get_minio_connection_settings()

    # Step 3: Build the table list and optional per-table config.
    # This allows a single job to handle multiple tables with overrides.
    # The expected outcome is a non-empty table list before starting Spark.
    groups, using_groups_config = resolve_raw_groups(
        args,
        event_date_column=event_date_column,
        watermark_column=watermark_column,
        lookback_days=lookback_days,
    )

    if using_groups_config:
        group_names = ",".join([g.get("group", "group") for g in groups])
        logger.info(
            "Starting ETL for %s groups (%s) run_date=%s",
            len(groups),
            group_names,
            run_date_str,
        )
    else:
        logger.info(
            "Starting ETL for %s (run_date=%s, mode=%s)",
            ",".join(groups[0]["tables"]),
            run_date_str,
            args.mode,
        )
    logger.info(
        "Postgres target host=%s port=%s db=%s user=%s",
        pg_settings.host,
        pg_settings.port,
        pg_settings.database,
        pg_settings.user,
    )
    logger.info("JDBC fetchsize=%s", jdbc_fetchsize)
    logger.info("MinIO endpoint=%s bucket=%s", minio_settings.endpoint, args.bucket)

    # Step 4: Initialize Spark, configure S3, and build the JDBC URL.
    # This sets the execution context used by every table extract.
    # The expected outcome is a ready SparkSession and valid JDBC target.
    spark = SparkSession.builder.appName(args.app_name).getOrCreate()
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    configure_s3(
        spark,
        minio_settings.endpoint,
        minio_settings.access_key,
        minio_settings.secret_key,
    )
    set_spark_log_level(spark)
    ingest_ts_utc = datetime.now(timezone.utc).isoformat()

    for group in groups:
        # Step 5: Resolve per-group settings and apply shuffle overrides.
        # This sets the execution strategy before per-table reads begin.
        # The expected outcome is a configured group context for table processing.
        group_name = group.get("group", "group")
        group_tables = group.get("tables", [])
        if not group_tables:
            logger.info("No tables configured for group %s", group_name)
            continue
        apply_group_shuffle(spark, group_name, group.get("shuffle_partitions"))

        for table in group_tables:
            # Step 6: Resolve per-table settings and detect initial-load conditions.
            # This decides how we filter incremental loads and whether to pull full history.
            # The expected outcome is a clear per-table extraction strategy before reading.
            output_base = table_base_path(
                args.bucket, args.output_prefix, args.schema, table
            )
            plan = build_table_plan(
                spark=spark,
                logger=logger,
                group=group,
                group_name=group_name,
                table=table,
                args=args,
                run_date=run_date,
                output_base=output_base,
            )

            # Step 7: Build the query filter and read source data via JDBC.
            # This ensures we only read the required slice when incrementing.
            # The expected outcome is a DataFrame covering the intended window.
            if plan.bootstrap_full_extract:
                logger.info(
                    "Bootstrap extract for %s: no state watermark; extracting full table.",
                    table,
                )

            logger.info("Reading %s via JDBC", plan.dbtable)
            df = (
                spark.read.format("jdbc")
                .option("url", pg_settings.jdbc_url)
                .option("dbtable", plan.dbtable)
                .option("user", pg_settings.user)
                .option("password", pg_settings.password)
                .option("driver", "org.postgresql.Driver")
                .option("fetchsize", str(jdbc_fetchsize))
                .load()
            )

            # Step 8: Add technical columns and construct manifest metadata.
            # This makes raw batches traceable and consistent for downstream validation.
            # The expected outcome is a DataFrame with lineage fields and a manifest base.
            df, base_columns = add_ingest_columns(
                df,
                run_id=run_id,
                source_system=args.source_system,
                table=table,
                mode=plan.group_mode,
            )
            schema_hash = hash_schema_json(df.schema.json())

            manifest_base = build_manifest_base(
                source_system=args.source_system,
                schema=args.schema,
                table=table,
                run_id=run_id,
                ingest_ts_utc=ingest_ts_utc,
                group_mode=plan.group_mode,
                schema_hash=schema_hash,
                image=args.image,
                app_name=args.app_name,
                dbtable=plan.dbtable,
                git_sha=get_env("GIT_SHA", "unknown") or "unknown",
            )

            # Step 9: Write output batches based on the partition strategy.
            # This emits parquet, manifest, and _SUCCESS markers in the expected paths.
            # The expected outcome is a complete batch per partition and updated state.
            write_table(
                spark=spark,
                logger=logger,
                plan=plan,
                df=df,
                run_date=run_date,
                run_date_str=run_date_str,
                run_id=run_id,
                manifest_base=manifest_base,
                base_columns=base_columns,
                args=args,
                table=table,
                ingest_ts_utc=ingest_ts_utc,
            )

            logger.info("ETL completed for %s.%s", args.schema, table)

    spark.stop()


if __name__ == "__main__":
    main()
