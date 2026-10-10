"""Validate and publish one contract-v3 dbt layer to Lakekeeper."""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
from time import monotonic

import duckdb

from duckdb_runtime import runtime_settings
from tools.contracts.ampere_contract import load_contract


FULL_REBUILD_FACT_BATCHES = {
    "fact_delivery_tracking": 3,
    "fact_order_product": 6,
    "fact_order_status_history": 3,
}
DAILY_FACT_BATCHES = {
    "fact_delivery_tracking": 3,
    "fact_order_product": 3,
}
CONTRACT = load_contract()


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
    expected_tables = {table.name for table in CONTRACT.layer_tables(layer)}
    if len(set(tables)) != len(tables):
        raise RuntimeError(
            f"Duplicate {layer} publish models found; refusing publication"
        )
    if set(tables) != expected_tables:
        raise RuntimeError(
            f"{layer} models do not match contract v{CONTRACT.version}. "
            f"Missing: {sorted(expected_tables - set(tables))}; "
            f"unexpected: {sorted(set(tables) - expected_tables)}"
        )
    missing_publication = {
        table for table in expected_tables
        if not CONTRACT.table(layer, table).publication
    }
    if missing_publication:
        raise RuntimeError(
            f"Missing publication contract for {layer}: {sorted(missing_publication)}"
        )
    return sorted(tables)


def _normalized_type(value: str) -> str:
    normalized = value.strip().lower().replace(" ", "")
    return {"varchar": "string", "text": "string", "integer": "int", "timestamp": "timestamp_ntz"}.get(normalized, normalized)


def _assert_contract_schema(con: duckdb.DuckDBPyConnection, relation: str, layer: str, table: str) -> None:
    spec = CONTRACT.table(layer, table)
    actual = [(row[0], _normalized_type(row[1])) for row in con.execute(f"DESCRIBE {relation}").fetchall()]
    expected = [(column["name"], _normalized_type(column["type_text"])) for column in spec.columns]
    if actual != expected:
        raise ValueError(f"Contract v{CONTRACT.version} schema mismatch for {relation}: expected {expected}, observed {actual}")


def _assert_contract_iceberg_table(con: duckdb.DuckDBPyConnection, relation: str, layer: str, table: str) -> None:
    spec = CONTRACT.table(layer, table)
    from tools.contracts.duckdb_conformance import validate_duckdb_table

    validate_duckdb_table(con, relation, spec)


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
    spec = CONTRACT.table(layer, table)
    try:
        completeness = spec.publication["source_completeness"][run_mode]
    except KeyError as exc:
        raise ValueError(f"No source-completeness policy for {layer}.{table} in {run_mode}") from exc
    return completeness == "full_snapshot"


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
    if not target_exists(con, target):
        raise ValueError(
            f"Published table {target} is missing; run ampere__iceberg__catalog__init first"
        )
    row_count = con.execute(f"SELECT count(*) FROM {source}").fetchone()[0]
    complete_source = is_complete_source(layer, table, run_mode)
    if complete_source:
        if row_count == 0:
            raise ValueError(f"Refusing to synchronize {target} from an empty table")
    keys = CONTRACT.table(layer, table).publication["merge_keys"]
    if row_count:
        assert_unique_keys(con, source, keys)
    if complete_source and run_mode == "daily_refresh":
        assert_unique_keys(con, target, keys)
    return row_count


def merge_upsert(
    con: duckdb.DuckDBPyConnection,
    source: str,
    target: str,
    keys: tuple[str, ...],
    source_relation: str | None = None,
) -> str:
    """Upsert changed rows and return the key predicate for source/target joins."""
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
    con.execute(
        f"MERGE INTO {target} AS target "
        f"USING {source_relation or source} AS source "
        f"ON {predicate} {matched_action}"
        "WHEN NOT MATCHED THEN INSERT BY NAME"
    )
    return predicate


def publish_fact_in_batches(
    con: duckdb.DuckDBPyConnection,
    table: str,
    source: str,
    target: str,
    keys: tuple[str, ...],
    batches: int,
    row_count: int,
    *,
    remove_missing: bool,
) -> None:
    """Bound large fact merges by order ID, cleaning only complete sources."""
    if batches < 1:
        raise ValueError("Fact publication requires at least one batch")
    if con.execute(
        f"SELECT 1 FROM {source} WHERE order_id IS NULL LIMIT 1"
    ).fetchone():
        raise ValueError(f"Staged relation {source} has null order_id")

    existing_target = target_exists(con, target)
    if not existing_target:
        raise ValueError(f"Published table {target} is missing; run catalog initialization first")
    if remove_missing:
        bounds = con.execute(
            f"SELECT min(order_id), max(order_id) FROM ("
            f"SELECT order_id FROM {source} UNION ALL "
            f"SELECT order_id FROM {target})"
        ).fetchone()
    else:
        # Daily fact sources are partial. Their ranges bound the merge work;
        # target-only history must remain untouched.
        bounds = con.execute(
            f"SELECT min(order_id), max(order_id) FROM {source}"
        ).fetchone()

    lower, maximum = bounds
    span = maximum - lower + 1
    ranges = [
        (lower + span * index // batches,
         lower + span * (index + 1) // batches)
        for index in range(batches)
    ]
    counts = con.execute(
        "SELECT " + ", ".join(
            f"count(*) FILTER (WHERE order_id >= {start} AND order_id < {stop})"
            for start, stop in ranges
        ) + f" FROM {source}"
    ).fetchone()
    if sum(counts) != row_count:
        raise ValueError(f"Batch ranges do not cover staged {source}: {counts}")
    for index, ((start, stop), batch_count) in enumerate(zip(ranges, counts)):
        if start == stop:
            continue
        started = monotonic()
        selected = (
            f"(SELECT * FROM {source} WHERE order_id >= {start} "
            f"AND order_id < {stop})"
        )
        predicate = merge_upsert(con, source, target, keys, selected)
        if existing_target and remove_missing:
            # A full-source DELETE inside one batch would erase the other two.
            # The union bounds also include stale target IDs outside staging.
            con.execute(
                f"DELETE FROM {target} AS target "
                f"WHERE target.order_id >= {start} AND target.order_id < {stop} "
                f"AND NOT EXISTS (SELECT 1 FROM {selected} AS source "
                f"WHERE {predicate})"
            )
        print(
            f"silver.{table}: part {index + 1}/{batches} "
            f"order_id=[{start},{stop}) staged_rows={batch_count} "
            f"published in {monotonic() - started:.1f}s",
            flush=True,
        )
    if existing_target and remove_missing:
        con.execute(f"DELETE FROM {target} WHERE order_id IS NULL")


def publish_full_history_fact_in_batches(
    con: duckdb.DuckDBPyConnection,
    table: str,
    source: str,
    target: str,
    keys: tuple[str, ...],
    batches: int,
    row_count: int,
) -> None:
    """Keep the full-history helper API for complete fact synchronization."""
    publish_fact_in_batches(
        con, table, source, target, keys, batches, row_count, remove_missing=True
    )


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
    if layer == "silver" and run_mode == "full_history" and table in FULL_REBUILD_FACT_BATCHES:
        publish_fact_in_batches(
            con, table, source, target,
            CONTRACT.table(layer, table).publication["merge_keys"],
            FULL_REBUILD_FACT_BATCHES[table], row_count,
            remove_missing=True,
        )
        action = "synchronized in batches"
    elif (
        layer == "silver"
        and run_mode == "daily_refresh"
        and table in DAILY_FACT_BATCHES
        and row_count > 0
    ):
        publish_fact_in_batches(
            con, table, source, target,
            CONTRACT.table(layer, table).publication["merge_keys"],
            DAILY_FACT_BATCHES[table], row_count,
            remove_missing=False,
        )
        action = "merged in batches"
    elif row_count == 0:
        action = "unchanged (empty daily slice)"
    else:
        keys = CONTRACT.table(layer, table).publication["merge_keys"]
        # Replays leave unchanged Iceberg rows alone instead of writing
        # positional deletes for every match.
        predicate = merge_upsert(con, source, target, keys)
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
    con.execute("SET ignore_target_file_size_for_partitioned_tables = true")
    for layer in layers:
        partitioned_target_tables = [
            table.name
            for table in CONTRACT.layer_tables(layer)
            if table.partition_spec and table.write.get("target_file_size_bytes") is not None
        ]
        if partitioned_target_tables:
            print(
                f"DuckDB Iceberg defers target-file sizing for {layer} partitioned tables "
                f"{partitioned_target_tables} to contract-driven Spark compaction",
                flush=True,
            )
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


def publish(layer: str) -> None:
    """Validate and publish only the selected layer's staged outputs."""
    if layer not in {"silver", "gold"}:
        raise ValueError("Publisher layer must be silver or gold")
    workspace = Path(os.getenv("DUCKDB_PATH", "/app/artifacts/ampere_work.duckdb"))
    manifest = (
        Path(os.getenv("DBT_PROJECT_DIR", "/app/dbt_iceberg")) / "target/manifest.json"
    )
    models = publish_models(manifest, layer)
    settings = runtime_settings(workspace)
    started = monotonic()
    con = duckdb.connect(":memory:", config=settings)
    try:
        attach_catalogs(con, workspace, (layer,))
        run_mode = os.getenv("ICEBERG_RUN_MODE", "daily_refresh")
        if run_mode not in {"daily_refresh", "full_history"}:
            raise ValueError(f"Unsupported run mode: {run_mode}")
        row_counts = {}
        for table in models:
            source = ".".join(map(identifier, (f"staged_{layer}", layer, table)))
            target = ".".join(map(identifier, (f"publish_{layer}", layer, table)))
            if not target_exists(con, target):
                raise ValueError(f"Published table {target} is missing; run catalog initialization first")
            _assert_contract_schema(con, source, layer, table)
            _assert_contract_iceberg_table(con, target, layer, table)
            row_counts[table] = validate_table(con, layer, table, run_mode)
    finally:
        con.close()
    print(f"Validated {len(models)} staged {layer} tables in {monotonic() - started:.1f}s", flush=True)

    publish_started = monotonic()
    con = duckdb.connect(":memory:", config=settings)
    try:
        attach_catalogs(con, workspace, (layer,))
        for table in models:
            publish_table(con, layer, table, run_mode, row_counts[table])
    finally:
        con.close()
    print(f"Published {layer} in {monotonic() - publish_started:.1f}s", flush=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--layer", required=True, choices=("silver", "gold"))
    publish(parser.parse_args().layer)
