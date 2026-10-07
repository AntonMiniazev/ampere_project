"""Publish a validated daily dbt slice without replacing Iceberg history."""

from __future__ import annotations

import json
import os
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from time import monotonic

import duckdb

from duckdb_runtime import runtime_settings


# Every published model needs a stable business grain. Complete staged models
# also delete keys missing from the new snapshot; daily fact slices never do.
MERGE_KEYS = {
    "silver": {
        "budget_orders_sales": ("budget_name", "budget_date", "store_id"),
        "dim_assortment": ("assortment_key",),
        "dim_clients": ("client_id",),
        "dim_costing": ("costing_key",),
        "dim_delivery_costing": ("delivery_costing_key",),
        "dim_delivery_resource": ("delivery_resource_id",),
        "dim_delivery_type": ("delivery_type_id",),
        "dim_order_statuses": ("order_status_id",),
        "dim_product_categories": ("category_id",),
        "dim_products": ("product_id",),
        "dim_stores": ("store_id",),
        "dim_zones": ("zone_id",),
        "fact_orders": ("order_id",),
        "fact_order_product": ("fact_order_product_key",),
        "fact_payments": ("fact_payments_key",),
        "fact_order_status_history": ("fact_order_status_history_key",),
        "fact_delivery_tracking": ("fact_delivery_tracking_key",),
    },
    "gold": {
        "budget_orders_sales": ("budget_name", "budget_date", "store_id"),
        "dim_clients": ("client_id",),
        "dim_costing": ("order_id", "order_date", "product_id"),
        "dim_delivery_cost": ("order_id",),
        "dim_products": ("product_id",),
        "dim_resource": ("courier_id",),
        "dim_stores": ("store_id", "zone_name"),
        "fct_orders_sales": ("order_id",),
        "fct_deliveries": ("order_id", "status_datetime"),
        "fct_order_margin": ("order_id",),
        "fct_order_product": ("order_id", "order_date", "product_id"),
    },
}
COMPLETE_MODELS = {
    "silver": frozenset(
        name for name in MERGE_KEYS["silver"] if not name.startswith("fact_")
    ),
    "gold": frozenset(
        {
            "budget_orders_sales",
            "dim_clients",
            "dim_products",
            "dim_resource",
            "dim_stores",
        }
    ),
}
NULLABLE_MERGE_KEYS = {("gold", "dim_stores"): frozenset({"zone_name"})}
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
    if set(tables) != set(MERGE_KEYS[layer]):
        raise RuntimeError(
            f"{layer} publish models do not match configured merge keys. "
            f"Missing keys: {sorted(set(tables) - set(MERGE_KEYS[layer]))}; "
            f"unexpected keys: {sorted(set(MERGE_KEYS[layer]) - set(tables))}"
        )
    return sorted(tables)


def assert_unique_keys(
    con: duckdb.DuckDBPyConnection,
    source: str,
    keys: tuple[str, ...],
    nullable_keys: frozenset[str] = frozenset(),
) -> None:
    """Reject null or duplicate source keys before changing a published table."""
    required_keys = [key for key in keys if key not in nullable_keys]
    null_predicate = " OR ".join(f"{identifier(key)} IS NULL" for key in required_keys)
    if (
        null_predicate
        and con.execute(
            f"SELECT 1 FROM {source} WHERE {null_predicate} LIMIT 1"
        ).fetchone()
    ):
        raise ValueError(f"Staged relation {source} has null merge keys: {keys}")
    key_sql = ", ".join(identifier(key) for key in keys)
    duplicate = con.execute(
        f"SELECT 1 FROM {source} GROUP BY {key_sql} HAVING count(*) > 1 LIMIT 1"
    ).fetchone()
    if duplicate:
        raise ValueError(f"Staged relation {source} has duplicate merge keys: {keys}")


def is_complete_source(layer: str, table: str, run_mode: str) -> bool:
    """Identify a complete source snapshot, which may safely remove missing keys."""
    return run_mode == "full_history" or table in COMPLETE_MODELS[layer]


def target_exists(con: duckdb.DuckDBPyConnection, target: str) -> bool:
    """Check whether a published table exists without modifying its catalog."""
    try:
        con.execute(f"SELECT 1 FROM {target} LIMIT 0")
    except duckdb.CatalogException:
        return False
    return True


def validate_table(
    con: duckdb.DuckDBPyConnection, layer: str, table: str, run_mode: str
) -> int:
    """Check a complete staged table before any Iceberg table is changed."""
    source = ".".join(map(identifier, (f"staged_{layer}", layer, table)))
    target = ".".join(map(identifier, (f"publish_{layer}", layer, table)))
    row_count = con.execute(f"SELECT count(*) FROM {source}").fetchone()[0]
    complete_source = is_complete_source(layer, table, run_mode)
    if complete_source:
        if row_count == 0:
            raise ValueError(f"Refusing to synchronize {target} from an empty table")
    if run_mode == "daily_refresh" and not target_exists(con, target):
        raise ValueError(
            f"Published table {target} is missing; run a full rebuild first"
        )
    keys = MERGE_KEYS[layer][table]
    nullable_keys = NULLABLE_MERGE_KEYS.get((layer, table), frozenset())
    if row_count:
        assert_unique_keys(con, source, keys, nullable_keys)
    if complete_source and run_mode == "daily_refresh":
        assert_unique_keys(con, target, keys, nullable_keys)
    return row_count


def publish_table(
    con: duckdb.DuckDBPyConnection,
    layer: str,
    table: str,
    run_mode: str,
    row_count: int | None = None,
) -> None:
    """Synchronize a complete snapshot or upsert a daily fact slice."""
    started = monotonic()
    if row_count is None:
        row_count = validate_table(con, layer, table, run_mode)
    source = ".".join(map(identifier, (f"staged_{layer}", layer, table)))
    target = ".".join(map(identifier, (f"publish_{layer}", layer, table)))
    complete_source = is_complete_source(layer, table, run_mode)
    if complete_source and not target_exists(con, target):
        con.execute(f"CREATE TABLE {target} AS SELECT * FROM {source}")
        action = "created"
    elif row_count == 0:
        action = "unchanged (empty daily slice)"
    else:
        keys = MERGE_KEYS[layer][table]
        predicate = " AND ".join(
            f"target.{identifier(key)} IS NOT DISTINCT FROM source.{identifier(key)}"
            for key in keys
        )
        columns = [
            description[0]
            for description in con.execute(f"SELECT * FROM {source} LIMIT 0").description
        ]
        key_names = {key.casefold() for key in keys}
        changed_columns = [
            column for column in columns if column.casefold() not in key_names
        ]
        # Replaying a daily slice can match millions of unchanged rows. Iceberg
        # updates write positional deletes, so only update changed values.
        changed_predicate = " OR ".join(
            f"target.{identifier(column)} IS DISTINCT FROM "
            f"source.{identifier(column)}"
            for column in changed_columns
        )
        matched_action = (
            f"WHEN MATCHED AND ({changed_predicate}) THEN UPDATE "
            if changed_predicate
            else ""
        )
        statement = (
            f"MERGE INTO {target} AS target USING {source} AS source "
            f"ON {predicate} {matched_action}"
            "WHEN NOT MATCHED THEN INSERT BY NAME"
        )
        con.execute(statement)
        if complete_source:
            # DuckDB-Iceberg 1.5.6 accepts the upsert MERGE and DELETE, but
            # rejects a MERGE containing update, insert, and delete actions.
            # Upsert first so a failed cleanup never leaves the target absent.
            con.execute(
                f"DELETE FROM {target} AS target "
                f"WHERE NOT EXISTS (SELECT 1 FROM {source} AS source "
                f"WHERE {predicate})"
            )
        action = "synchronized" if complete_source else "merged"
    print(
        f"{layer}.{table}: {action} {row_count} staged rows "
        f"in {monotonic() - started:.1f}s",
        flush=True,
    )


def attach_catalogs(
    con: duckdb.DuckDBPyConnection, workspace: Path, layers: tuple[str, ...]
) -> None:
    """Attach read-only staged files and their separate published warehouses."""
    con.execute("LOAD iceberg")
    con.execute("LOAD httpfs")
    for layer in layers:
        local_path = workspace.parent / f"staged_{layer}.duckdb"
        con.execute(
            f"ATTACH {literal(str(local_path))} AS {identifier(f'staged_{layer}')} "
            "(READ_ONLY)"
        )
        warehouse = os.getenv(f"ICEBERG_{layer.upper()}_WAREHOUSE", layer)
        con.execute(
            f"ATTACH {literal(warehouse)} AS {identifier(f'publish_{layer}')} "
            "(TYPE iceberg)"
        )


def publish_layer(
    workspace: Path,
    settings: dict[str, object],
    layer: str,
    tables: list[str],
    run_mode: str,
    row_counts: dict[tuple[str, str], int],
) -> None:
    """Publish one warehouse on its own DuckDB connection."""
    con = duckdb.connect(":memory:", config=settings)
    try:
        attach_catalogs(con, workspace, (layer,))
        for table in tables:
            publish_table(con, layer, table, run_mode, row_counts[layer, table])
    finally:
        con.close()


def publish() -> None:
    """Attach local dbt outputs and Lakekeeper warehouses, then publish."""
    workspace = Path(os.getenv("DUCKDB_PATH", "/app/artifacts/ampere_work.duckdb"))
    manifest = (
        Path(os.getenv("DBT_PROJECT_DIR", "/app/dbt_iceberg")) / "target/manifest.json"
    )
    models = {layer: publish_models(manifest, layer) for layer in ("silver", "gold")}
    settings = runtime_settings(workspace)
    parallel_layers = int(os.getenv("ICEBERG_PUBLISH_PARALLEL_LAYERS", "1"))
    if parallel_layers not in (1, 2):
        raise ValueError("ICEBERG_PUBLISH_PARALLEL_LAYERS must be 1 or 2")
    started = monotonic()
    con = duckdb.connect(":memory:", config=settings)
    try:
        attach_catalogs(con, workspace, ("silver", "gold"))
        run_modes = {}
        row_counts = {}
        for layer in ("silver", "gold"):
            run_mode = os.getenv(f"{layer.upper()}_RUN_MODE", "daily_refresh")
            if run_mode not in {"daily_refresh", "full_history"}:
                raise ValueError(f"Unsupported {layer} run mode: {run_mode}")
            run_modes[layer] = run_mode
            for table in models[layer]:
                row_counts[layer, table] = validate_table(con, layer, table, run_mode)
    finally:
        con.close()
    print(f"Validated all staged tables in {monotonic() - started:.1f}s", flush=True)

    publish_started = monotonic()
    if parallel_layers == 1:
        for layer in ("silver", "gold"):
            publish_layer(
                workspace, settings, layer, models[layer], run_modes[layer], row_counts
            )
    else:
        # The warehouses are distinct; each worker writes one catalog, and
        # the pod succeeds only after both have completed. Per-table Iceberg
        # commits remain independent, so a retry must finish partial work.
        with ThreadPoolExecutor(max_workers=2) as executor:
            futures = [
                executor.submit(
                    publish_layer, workspace, settings, layer, models[layer],
                    run_modes[layer], row_counts,
                )
                for layer in ("silver", "gold")
            ]
            for future in futures:
                future.result()
    print(f"Published both layers in {monotonic() - publish_started:.1f}s", flush=True)


if __name__ == "__main__":
    publish()
