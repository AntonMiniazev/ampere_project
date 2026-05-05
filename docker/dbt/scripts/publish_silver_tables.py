from __future__ import annotations

import argparse
import gc
import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import boto3
import duckdb
import pyarrow as pa
from botocore.exceptions import ClientError
from deltalake import write_deltalake
from register_silver_uc_tables import (
    comparable_column,
    get_uc_table,
    normalize_location,
    uc_column_type,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Publish dbt layer tables from the local DuckDB file to MinIO."
    )
    parser.add_argument(
        "--layer",
        default=os.getenv("PUBLISH_LAYER", "silver"),
        choices=["silver", "gold"],
        help="Layer tag and defaults used for publish discovery and UC validation.",
    )
    parser.add_argument(
        "--duckdb-path",
        default=os.getenv("DUCKDB_PATH", "/app/artifacts/ampere.duckdb"),
        help="DuckDB database file produced by dbt.",
    )
    parser.add_argument(
        "--manifest-path",
        default="/app/dbt/target/manifest.json",
        help="dbt manifest used to discover publish-tagged models.",
    )
    parser.add_argument(
        "--external-root",
        default="",
        help="S3 root for durable table exports. Defaults by layer when omitted.",
    )
    parser.add_argument(
        "--run-mode",
        default="",
        choices=["daily_refresh", "full_rebuild"],
        help="Run mode used to choose full or differential publish behavior.",
    )
    parser.add_argument(
        "--local-manifest-output",
        default="",
        help="Local copy of the publish manifest for downstream runtime steps.",
    )
    return parser.parse_args()


PARTITIONED_PUBLISH_MODELS = {
    "silver": {
        "fact_orders": "order_date",
        "fact_order_product": "order_date",
        "fact_payments": "payment_date",
        "fact_order_status_history": "status_datetime",
        "fact_delivery_tracking": "status_datetime",
    },
    "gold": {
        "dim_costing": "order_date",
        "fct_orders_sales": "order_date",
        "fct_deliveries": "status_datetime",
        "fct_order_margin": "order_date",
    },
}
PUBLISH_PARTITION_COLUMNS = {
    "silver": "_silver_partition_date",
    "gold": "_gold_partition_date",
}
BRONZE_LINEAGE_COLUMNS = {
    "_bronze_last_run_id",
    "_bronze_last_apply_ts",
    "_bronze_last_manifest_path",
}


def default_external_root(layer: str) -> str:
    if layer == "gold":
        return os.getenv("GOLD_EXTERNAL_ROOT", "s3://ampere-gold/gold")
    return os.getenv("SILVER_EXTERNAL_ROOT", "s3://ampere-silver/silver")


def default_run_mode(layer: str) -> str:
    env_name = "GOLD_RUN_MODE" if layer == "gold" else "SILVER_RUN_MODE"
    return os.getenv(env_name, "daily_refresh")


def default_publish_manifest_path(layer: str) -> str:
    if layer == "gold":
        return os.getenv(
            "GOLD_PUBLISH_MANIFEST_PATH",
            "/app/artifacts/gold_publish_manifest.json",
        )
    return os.getenv(
        "SILVER_PUBLISH_MANIFEST_PATH",
        "/app/artifacts/silver_publish_manifest.json",
    )


def split_s3_uri(uri: str) -> tuple[str, str]:
    if not uri.startswith("s3://"):
        raise ValueError(f"Unsupported S3 URI: {uri}")
    bucket, _, prefix = uri[5:].partition("/")
    if not bucket:
        raise ValueError(f"S3 URI does not contain a bucket: {uri}")
    return bucket, prefix.strip("/")


def endpoint_url() -> str:
    raw_endpoint = os.getenv("MINIO_S3_ENDPOINT", "minio.ampere.svc.cluster.local:9000")
    if raw_endpoint.startswith(("http://", "https://")):
        return raw_endpoint
    return f"http://{raw_endpoint}"


def s3_client() -> Any:
    endpoint = endpoint_url()
    return boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=os.getenv("MINIO_ACCESS_KEY"),
        aws_secret_access_key=os.getenv("MINIO_SECRET_KEY"),
        region_name=os.getenv("MINIO_S3_REGION", "us-east-1"),
        use_ssl=endpoint.startswith("https://"),
    )


def delta_storage_options() -> dict[str, str]:
    endpoint = endpoint_url()
    options = {
        "AWS_ENDPOINT_URL": endpoint,
        "AWS_ACCESS_KEY_ID": os.getenv("MINIO_ACCESS_KEY", ""),
        "AWS_SECRET_ACCESS_KEY": os.getenv("MINIO_SECRET_KEY", ""),
        "AWS_REGION": os.getenv("MINIO_S3_REGION", "us-east-1"),
        "AWS_S3_FORCE_PATH_STYLE": "true",
        "AWS_S3_ALLOW_UNSAFE_RENAME": "true",
    }
    if endpoint.startswith("http://"):
        options["AWS_ALLOW_HTTP"] = "true"
    return options


def ensure_bucket(client: Any, bucket: str) -> None:
    try:
        client.head_bucket(Bucket=bucket)
    except ClientError as exc:
        error_code = exc.response.get("Error", {}).get("Code")
        if error_code not in {"404", "NoSuchBucket"}:
            raise
        client.create_bucket(Bucket=bucket)


def prefix_has_delta_log(client: Any, bucket: str, table_prefix: str) -> bool:
    response = client.list_objects_v2(
        Bucket=bucket,
        Prefix=f"{table_prefix.rstrip('/')}/_delta_log/",
        MaxKeys=1,
    )
    return bool(response.get("Contents"))


def delete_prefix(client: Any, bucket: str, prefix: str) -> None:
    paginator = client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        objects = [{"Key": item["Key"]} for item in page.get("Contents", [])]
        if objects:
            client.delete_objects(Bucket=bucket, Delete={"Objects": objects})


def clean_legacy_non_delta_prefix(client: Any, bucket: str, table_prefix: str) -> None:
    if prefix_has_delta_log(client, bucket, table_prefix):
        return
    delete_prefix(client, bucket, table_prefix.rstrip("/") + "/")


def publish_models(manifest_path: Path, layer: str) -> list[dict[str, str]]:
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    published: list[dict[str, str]] = []
    for node in manifest.get("nodes", {}).values():
        if node.get("resource_type") != "model":
            continue
        tags = node.get("tags", [])
        if "publish" not in tags or layer not in tags:
            continue
        relation_name = node.get("relation_name")
        model_name = node.get("name")
        table_name = node.get("alias") or model_name
        if relation_name and model_name and table_name:
            published.append(
                {
                    "model_name": model_name,
                    "table_name": table_name,
                    "relation_name": relation_name,
                }
            )
    return sorted(published, key=lambda item: item["table_name"])


def write_publish_manifest(
    client: Any,
    bucket: str,
    root_prefix: str,
    rows: list[dict[str, Any]],
    local_manifest_output: Path,
) -> None:
    body = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "tables": rows,
    }
    local_manifest_output.parent.mkdir(parents=True, exist_ok=True)
    local_manifest_output.write_text(json.dumps(body, indent=2), encoding="utf-8")
    key = "/".join(part for part in [root_prefix, "_publish_manifest.json"] if part)
    client.put_object(
        Bucket=bucket,
        Key=key,
        Body=json.dumps(body, indent=2).encode("utf-8"),
        ContentType="application/json",
    )


def partition_values(
    connection: duckdb.DuckDBPyConnection,
    relation_name: str,
    partition_column: str,
) -> list[str]:
    rows = connection.execute(
        f"""
        select distinct cast(cast({partition_column} as date) as varchar) as partition_value
        from {relation_name}
        where {partition_column} is not null
        order by partition_value
        """
    ).fetchall()
    return [row[0] for row in rows]


def publish_columns(
    connection: duckdb.DuckDBPyConnection,
    relation_name: str,
) -> list[str]:
    """Return relation columns excluding Bronze-only lineage metadata."""
    cursor = connection.execute(f"select * from {relation_name} limit 0")
    return [
        item[0]
        for item in cursor.description
        if item[0] not in BRONZE_LINEAGE_COLUMNS
    ]


def select_column_sql(
    connection: duckdb.DuckDBPyConnection,
    relation_name: str,
) -> str:
    """Return SQL projection for published columns only."""
    return ",\n                ".join(
        f'"{column}"' for column in publish_columns(connection, relation_name)
    )


def relation_as_arrow(
    connection: duckdb.DuckDBPyConnection,
    relation_name: str,
    publish_partition_column: str,
    partition_column: str | None = None,
    where_clause: str | None = None,
) -> pa.Table:
    where_sql = f"where {where_clause}" if where_clause else ""
    select_columns = select_column_sql(connection, relation_name)
    if partition_column:
        return connection.sql(
            f"""
            select
                {select_columns},
                cast({partition_column} as date) as {publish_partition_column}
            from {relation_name}
            {where_sql}
            """
        ).to_arrow_table()
    return connection.sql(
        f"select {select_columns} from {relation_name} {where_sql}"
    ).to_arrow_table()


def relation_uc_columns(
    connection: duckdb.DuckDBPyConnection,
    relation_name: str,
    partition_column: str | None,
    publish_partition_column: str,
) -> list[dict[str, Any]]:
    """Build UC-comparable columns from the zero-row publish projection."""
    select_columns = select_column_sql(connection, relation_name)
    if partition_column:
        query = f"""
            select
                {select_columns},
                cast({partition_column} as date) as {publish_partition_column}
            from {relation_name}
            limit 0
        """
    else:
        query = f"select {select_columns} from {relation_name} limit 0"
    schema = connection.sql(query).to_arrow_table().schema
    columns = []
    for position, field in enumerate(schema, start=1):
        type_name, type_text, type_json = uc_column_type(field.type)
        columns.append(
            {
                "name": field.name,
                "type_name": type_name,
                "type_text": type_text,
                "type_json": type_json,
                "position": position,
                "nullable": bool(field.nullable),
            }
        )
    return columns


def validate_publish_contract(
    connection: duckdb.DuckDBPyConnection,
    layer: str,
    table_name: str,
    relation_name: str,
    target_uri: str,
    partition_column: str | None,
    publish_partition_column: str,
) -> None:
    """Validate the planned publish schema against deployed UC metadata."""
    validation_env = "RUN_GOLD_UC_REGISTRATION" if layer == "gold" else "RUN_SILVER_UC_REGISTRATION"
    if os.getenv(validation_env, "true").strip().lower() != "true":
        return
    uc_api_uri = os.getenv("UC_API_URI", "").strip()
    if not uc_api_uri:
        raise RuntimeError(f"UC_API_URI is required for {layer} publish contract validation.")
    catalog = os.getenv(f"{layer.upper()}_UC_CATALOG", os.getenv("BRONZE_UC_CATALOG", "ampere"))
    schema = os.getenv(f"{layer.upper()}_UC_SCHEMA", layer)
    uc_payload = get_uc_table(uc_api_uri, catalog, schema, table_name)

    errors: list[str] = []
    if str(uc_payload.get("table_type") or "").upper() != "EXTERNAL":
        errors.append(f"table_type={uc_payload.get('table_type')!r}, expected EXTERNAL")
    if str(uc_payload.get("data_source_format") or "").upper() != "DELTA":
        errors.append(
            "data_source_format="
            f"{uc_payload.get('data_source_format')!r}, expected DELTA"
        )
    if normalize_location(str(uc_payload.get("storage_location") or "")) != normalize_location(
        target_uri
    ):
        errors.append(
            "storage_location="
            f"{uc_payload.get('storage_location')!r}, expected {target_uri!r}"
        )

    uc_columns = [
        comparable_column(column, include_nullable=False)
        for column in sorted(
            uc_payload.get("columns") or [],
            key=lambda item: int(item.get("position") or 0),
        )
    ]
    planned_columns = [
        comparable_column(column, include_nullable=False)
        for column in relation_uc_columns(
            connection,
            relation_name,
            partition_column,
            publish_partition_column,
        )
    ]
    if uc_columns != planned_columns:
        errors.append(
            "column contract mismatch: "
            f"uc={uc_columns!r} planned_publish={planned_columns!r}"
        )
    if errors:
        raise RuntimeError(
            f"{layer.capitalize()} publish contract validation failed for {table_name}: "
            + "; ".join(errors)
        )
    print(f"UC publish contract valid: {catalog}.{schema}.{table_name}")


def partition_predicate(
    publish_partition_column: str,
    partition_values_to_replace: list[str],
) -> str | None:
    if not partition_values_to_replace:
        return None
    values = ", ".join(f"'{value}'" for value in partition_values_to_replace)
    return f"{publish_partition_column} in ({values})"


def write_partitioned_delta(
    connection: duckdb.DuckDBPyConnection,
    target_uri: str,
    relation_name: str,
    partition_column: str,
    publish_partition_column: str,
    partition_value: str,
    storage_options: dict[str, str],
    mode: str,
    schema_mode: str | None = None,
    predicate: str | None = None,
) -> None:
    """Write one date partition from DuckDB to Delta to cap Arrow memory peaks."""
    arrow_table = relation_as_arrow(
        connection,
        relation_name,
        publish_partition_column,
        partition_column,
        where_clause=f"cast({partition_column} as date) = date '{partition_value}'",
    )
    try:
        write_deltalake(
            target_uri,
            arrow_table,
            mode=mode,
            partition_by=[publish_partition_column],
            schema_mode=schema_mode,
            storage_options=storage_options,
            predicate=predicate,
        )
    finally:
        del arrow_table
        gc.collect()


def publish_partitioned_model(
    connection: duckdb.DuckDBPyConnection,
    client: Any,
    bucket: str,
    table_prefix: str,
    storage_options: dict[str, str],
    target_uri: str,
    layer: str,
    model_name: str,
    table_name: str,
    relation_name: str,
    partition_column: str,
    publish_partition_column: str,
    run_mode: str,
) -> dict[str, Any]:
    row_count = connection.execute(f"select count(*) from {relation_name}").fetchone()[0]
    date_partitions = partition_values(connection, relation_name, partition_column)
    validate_publish_contract(
        connection,
        layer,
        table_name,
        relation_name,
        target_uri,
        partition_column,
        publish_partition_column,
    )
    if not date_partitions:
        print(f"Skipped {table_name}: no rows to publish")
        return {
            "model_name": model_name,
            "table_name": table_name,
            "relation_name": relation_name,
            "row_count": row_count,
            "data_uri": target_uri,
            "publish_mode": "delta_partitioned",
            "partition_column": partition_column,
            "publish_partition_column": publish_partition_column,
            "partition_values": [],
        }

    if run_mode == "full_rebuild":
        clean_legacy_non_delta_prefix(client, bucket, table_prefix)
    elif not prefix_has_delta_log(client, bucket, table_prefix):
        raise RuntimeError(
            f"{table_name} is not a Delta table yet. Run the {layer} full rebuild "
            "before daily partition refresh."
        )

    for index, partition_value in enumerate(date_partitions, start=1):
        if run_mode == "full_rebuild":
            mode = "overwrite" if index == 1 else "append"
            schema_mode = "overwrite" if index == 1 else None
            predicate = None
        else:
            mode = "overwrite"
            schema_mode = None
            predicate = partition_predicate(publish_partition_column, [partition_value])

        write_partitioned_delta(
            connection,
            target_uri,
            relation_name,
            partition_column,
            publish_partition_column,
            partition_value,
            storage_options,
            mode,
            schema_mode,
            predicate,
        )
    print(
        f"Published {table_name}: rows={row_count} partitions={len(date_partitions)} uri={target_uri}"
    )
    return {
        "model_name": model_name,
        "table_name": table_name,
        "relation_name": relation_name,
        "row_count": row_count,
        "data_uri": target_uri,
        "publish_mode": "delta_partitioned",
        "partition_column": partition_column,
        "publish_partition_column": publish_partition_column,
        "partition_values": date_partitions,
    }


def publish_replacement_model(
    connection: duckdb.DuckDBPyConnection,
    client: Any,
    bucket: str,
    table_prefix: str,
    storage_options: dict[str, str],
    target_uri: str,
    layer: str,
    model_name: str,
    table_name: str,
    relation_name: str,
    publish_partition_column: str,
) -> dict[str, Any]:
    row_count = connection.execute(f"select count(*) from {relation_name}").fetchone()[0]
    validate_publish_contract(
        connection,
        layer,
        table_name,
        relation_name,
        target_uri,
        None,
        publish_partition_column,
    )
    clean_legacy_non_delta_prefix(client, bucket, table_prefix)
    arrow_table = relation_as_arrow(connection, relation_name, publish_partition_column)
    try:
        write_deltalake(
            target_uri,
            arrow_table,
            mode="overwrite",
            schema_mode="overwrite",
            storage_options=storage_options,
        )
    finally:
        del arrow_table
        gc.collect()
    print(f"Published {table_name}: rows={row_count} uri={target_uri}")
    return {
        "model_name": model_name,
        "table_name": table_name,
        "relation_name": relation_name,
        "row_count": row_count,
        "data_uri": target_uri,
        "publish_mode": "delta_replacement",
    }


def table_prefix_for(root_prefix: str, table_name: str) -> str:
    """Return the object-storage prefix used for one published table."""
    return "/".join(part for part in [root_prefix, table_name] if part)


def choose_effective_run_mode(
    *,
    requested_run_mode: str,
    layer: str,
    models: list[dict[str, str]],
    partitioned_models: dict[str, str],
    client: Any,
    bucket: str,
    root_prefix: str,
) -> str:
    """Promote first daily publish to full rebuild when partitioned Delta outputs are absent."""
    if requested_run_mode != "daily_refresh":
        return requested_run_mode

    missing_delta_tables: list[str] = []
    for model in models:
        table_name = model["table_name"]
        if table_name not in partitioned_models:
            continue
        table_prefix = table_prefix_for(root_prefix, table_name)
        if not prefix_has_delta_log(client, bucket, table_prefix):
            missing_delta_tables.append(table_name)

    if not missing_delta_tables:
        return requested_run_mode

    print(
        f"{layer} daily publish bootstrap: missing Delta log for "
        f"{', '.join(sorted(missing_delta_tables))}. "
        "Using full_rebuild publish mode for this run."
    )
    return "full_rebuild"


def main() -> None:
    args = parse_args()
    layer = args.layer
    external_root = args.external_root or default_external_root(layer)
    run_mode = args.run_mode or default_run_mode(layer)
    local_manifest_output = args.local_manifest_output or default_publish_manifest_path(layer)
    partitioned_models = PARTITIONED_PUBLISH_MODELS[layer]
    publish_partition_column = PUBLISH_PARTITION_COLUMNS[layer]
    manifest_path = Path(args.manifest_path)
    bucket, root_prefix = split_s3_uri(external_root)
    client = s3_client()
    ensure_bucket(client, bucket)
    storage_options = delta_storage_options()

    models = publish_models(manifest_path, layer)
    if not models:
        raise SystemExit(f"No {layer} publish-tagged dbt models were found in manifest.")
    effective_run_mode = choose_effective_run_mode(
        requested_run_mode=run_mode,
        layer=layer,
        models=models,
        partitioned_models=partitioned_models,
        client=client,
        bucket=bucket,
        root_prefix=root_prefix,
    )

    connection = duckdb.connect(args.duckdb_path, read_only=False)

    published_rows: list[dict[str, Any]] = []
    for model in models:
        model_name = model["model_name"]
        table_name = model["table_name"]
        table_prefix = table_prefix_for(root_prefix, table_name)
        target_uri = f"s3://{bucket}/{table_prefix}"
        relation_name = model["relation_name"]
        partition_column = partitioned_models.get(table_name)
        if partition_column:
            published_rows.append(
                publish_partitioned_model(
                    connection,
                    client,
                    bucket,
                    table_prefix,
                    storage_options,
                    target_uri,
                    layer,
                    model_name,
                    table_name,
                    relation_name,
                    partition_column,
                    publish_partition_column,
                    effective_run_mode,
                )
            )
        else:
            published_rows.append(
                publish_replacement_model(
                    connection,
                    client,
                    bucket,
                    table_prefix,
                    storage_options,
                    target_uri,
                    layer,
                    model_name,
                    table_name,
                    relation_name,
                    publish_partition_column,
                )
            )

    write_publish_manifest(
        client,
        bucket,
        root_prefix,
        published_rows,
        Path(local_manifest_output),
    )
    print(f"Published {len(published_rows)} {layer} table(s) to {external_root}")


if __name__ == "__main__":
    main()
