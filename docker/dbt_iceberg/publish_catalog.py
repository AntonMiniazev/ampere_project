"""Publish a validated daily dbt slice without replacing Iceberg history."""

from __future__ import annotations

import json
import os
from pathlib import Path

import duckdb


# Silver facts have dbt uniqueness tests on these keys. Gold keys represent
# their corresponding business grain; verify each staged slice before MERGE.
MERGE_KEYS = {
    "silver": {
        "fact_orders": ("order_id",),
        "fact_order_product": ("fact_order_product_key",),
        "fact_payments": ("fact_payments_key",),
        "fact_order_status_history": ("fact_order_status_history_key",),
        "fact_delivery_tracking": ("fact_delivery_tracking_key",),
    },
    "gold": {
        "dim_costing": ("order_id", "order_date", "product_id"),
        "dim_delivery_cost": ("order_id",),
        "fct_orders_sales": ("order_id",),
        "fct_deliveries": ("order_id", "status_datetime"),
        "fct_order_margin": ("order_id",),
        "fct_order_product": ("order_id", "order_date", "product_id"),
    },
}
EXPECTED_MODEL_COUNT = {"silver": 17, "gold": 11}


def identifier(value: str) -> str:
    """Quote a catalog or column identifier."""
    return '"' + value.replace('"', '""') + '"'


def literal(value: str) -> str:
    """Quote a DuckDB string value."""
    return "'" + value.replace("'", "''") + "'"


def publish_models(manifest_path: Path, layer: str) -> list[str]:
    """Find all publish-tagged models for a layer in the completed dbt build."""
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    tables = [
        node.get("alias") or node["name"]
        for node in manifest.get("nodes", {}).values()
        if node.get("resource_type") == "model"
        and {layer, "publish"}.issubset(set(node.get("tags", [])))
    ]
    if len(tables) != EXPECTED_MODEL_COUNT[layer] or len(set(tables)) != len(tables):
        raise RuntimeError(
            f"Expected {EXPECTED_MODEL_COUNT[layer]} unique {layer} publish models; "
            f"found {len(tables)}. Refusing a partial publication."
        )
    return sorted(tables)


def assert_unique_keys(
    con: duckdb.DuckDBPyConnection, source: str, keys: tuple[str, ...]
) -> None:
    """Reject null or duplicate source keys before changing a published table."""
    null_predicate = " OR ".join(f"{identifier(key)} IS NULL" for key in keys)
    if con.execute(f"SELECT 1 FROM {source} WHERE {null_predicate} LIMIT 1").fetchone():
        raise ValueError(f"Staged relation {source} has null merge keys: {keys}")
    key_sql = ", ".join(identifier(key) for key in keys)
    duplicate = con.execute(
        f"SELECT 1 FROM {source} GROUP BY {key_sql} HAVING count(*) > 1 LIMIT 1"
    ).fetchone()
    if duplicate:
        raise ValueError(f"Staged relation {source} has duplicate merge keys: {keys}")


def validate_table(
    con: duckdb.DuckDBPyConnection, layer: str, table: str, run_mode: str
) -> int:
    """Check a complete staged table before any Iceberg table is changed."""
    source = ".".join(map(identifier, (f"staged_{layer}", layer, table)))
    target = ".".join(map(identifier, (f"publish_{layer}", layer, table)))
    row_count = con.execute(f"SELECT count(*) FROM {source}").fetchone()[0]
    if run_mode == "full_history" or table not in MERGE_KEYS[layer]:
        if row_count == 0:
            raise ValueError(f"Refusing to replace {target} with an empty table")
    else:
        # Fail instead of bootstrapping a partial-history table on the first
        # daily run. The manual full rebuild creates the complete baseline.
        con.execute(f"SELECT 1 FROM {target} LIMIT 0")
        if row_count:
            assert_unique_keys(con, source, MERGE_KEYS[layer][table])
    return row_count


def publish_table(
    con: duckdb.DuckDBPyConnection,
    layer: str,
    table: str,
    run_mode: str,
    row_count: int | None = None,
) -> None:
    """Replace full models or upsert one daily fact slice in one Iceberg commit."""
    if row_count is None:
        row_count = validate_table(con, layer, table, run_mode)
    source = ".".join(map(identifier, (f"staged_{layer}", layer, table)))
    target = ".".join(map(identifier, (f"publish_{layer}", layer, table)))
    if run_mode == "full_history" or table not in MERGE_KEYS[layer]:
        con.execute(f"CREATE OR REPLACE TABLE {target} AS SELECT * FROM {source}")
        action = "replaced"
    elif row_count == 0:
        action = "unchanged (empty daily slice)"
    else:
        keys = MERGE_KEYS[layer][table]
        predicate = " AND ".join(
            f"target.{identifier(key)} = source.{identifier(key)}" for key in keys
        )
        con.execute(
            f"MERGE INTO {target} AS target USING {source} AS source "
            f"ON {predicate} WHEN MATCHED THEN UPDATE "
            "WHEN NOT MATCHED THEN INSERT BY NAME"
        )
        action = "merged"
    print(f"{layer}.{table}: {action} {row_count} staged rows", flush=True)


def publish() -> None:
    """Attach local dbt outputs and Lakekeeper warehouses, then publish."""
    workspace = Path(os.getenv("DUCKDB_PATH", "/app/artifacts/ampere_work.duckdb"))
    secret_dir = Path(os.getenv("DUCKDB_SECRET_DIRECTORY", "/app/secret_store"))
    manifest = Path(os.getenv("DBT_PROJECT_DIR", "/app/dbt_iceberg")) / "target/manifest.json"
    models = {layer: publish_models(manifest, layer) for layer in ("silver", "gold")}
    settings = {
        "secret_directory": str(secret_dir),
        "memory_limit": os.getenv("DUCKDB_MEMORY_LIMIT", "7GB"),
    }
    ca_file = os.getenv("DUCKDB_CA_CERT_FILE", "").strip()
    if ca_file:
        settings["ca_cert_file"] = ca_file
        settings["enable_server_cert_verification"] = True
    con = duckdb.connect(str(workspace), config=settings)
    try:
        con.execute("LOAD iceberg")
        con.execute("LOAD httpfs")
        for layer in ("silver", "gold"):
            local_path = workspace.parent / f"staged_{layer}.duckdb"
            con.execute(
                f"ATTACH {literal(str(local_path))} AS {identifier(f'staged_{layer}')}"
            )
            warehouse = os.getenv(f"ICEBERG_{layer.upper()}_WAREHOUSE", layer)
            con.execute(
                f"ATTACH {literal(warehouse)} AS {identifier(f'publish_{layer}')} "
                "(TYPE iceberg)"
            )
        run_modes = {}
        row_counts = {}
        for layer in ("silver", "gold"):
            run_mode = os.getenv(f"{layer.upper()}_RUN_MODE", "daily_refresh")
            if run_mode not in {"daily_refresh", "full_history"}:
                raise ValueError(f"Unsupported {layer} run mode: {run_mode}")
            run_modes[layer] = run_mode
            for table in models[layer]:
                row_counts[layer, table] = validate_table(con, layer, table, run_mode)
        for layer in ("silver", "gold"):
            for table in models[layer]:
                publish_table(
                    con, layer, table, run_modes[layer], row_counts[layer, table]
                )
    finally:
        con.close()


if __name__ == "__main__":
    publish()
