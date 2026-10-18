"""Validate and publish one contract-v3 dbt layer to Lakekeeper."""

from __future__ import annotations

import argparse
import json
import os
import re
from datetime import date, timedelta
import json
from pathlib import Path
from time import monotonic

import duckdb

from duckdb_runtime import runtime_settings
from tools.contracts.ampere_contract import load_contract


FULL_REBUILD_FACT_BATCHES = {
    "fact_delivery_tracking": 3,
    # Keep the full-history MERGE working set below the DuckDB memory cap.
    "fact_order_product": 50,
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


def is_affected_month_source(layer: str, table: str, run_mode: str) -> bool:
    """Return whether a Gold daily source is complete only for selected months."""
    return (
        layer == "gold"
        and run_mode == "daily_refresh"
        and CONTRACT.table(layer, table).publication["source_completeness"][run_mode]
        == "affected_month_snapshot"
    )


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
    affected_month_source = is_affected_month_source(layer, table, run_mode)
    if complete_source:
        if row_count == 0:
            raise ValueError(f"Refusing to synchronize {target} from an empty table")
    keys = CONTRACT.table(layer, table).publication["merge_keys"]
    if row_count:
        assert_unique_keys(con, source, keys)
    if complete_source and run_mode == "daily_refresh":
        assert_unique_keys(con, target, keys)
    if affected_month_source and row_count:
        assert_unique_keys(con, source, keys)
    return row_count


def merge_upsert(
    con: duckdb.DuckDBPyConnection,
    source: str,
    target: str,
    keys: tuple[str, ...],
    source_relation: str | None = None,
    target_predicate: str | None = None,
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
        column for column in columns
        if column.casefold() not in key_names
        and column.casefold() not in {"_silver_build_ts", "_silver_run_id"}
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
    target_condition = f" AND ({target_predicate})" if target_predicate else ""
    con.execute(
        f"MERGE INTO {target} AS target "
        f"USING {source_relation or source} AS source "
        f"ON ({predicate}){target_condition} {matched_action}"
        "WHEN NOT MATCHED THEN INSERT BY NAME"
    )
    return predicate


def affected_order_relation(layer: str) -> str:
    return ".".join(
        map(identifier, ("ampere_work", "silver_control", "affected_order_ids"))
    )


def _period_bounds(value, grain: str) -> tuple[str, str]:
    start = value.date() if hasattr(value, "date") else value
    if grain == "week":
        stop = start + timedelta(days=7)
    elif grain == "month":
        stop = date(start.year + (start.month == 12), 1 if start.month == 12 else start.month + 1, 1)
    else:
        raise ValueError(f"Unsupported date partition grain: {grain}")
    return start.isoformat(), stop.isoformat()


def _explain_partition_filter(con, target: str, column: str, start: str, stop: str) -> None:
    plan = "\n".join(
        str(row[-1])
        for row in con.execute(
            f"EXPLAIN SELECT {identifier(column)} FROM {target} "
            f"WHERE {identifier(column)} >= DATE '{start}' "
            f"AND {identifier(column)} < DATE '{stop}'"
        ).fetchall()
    )
    has_partition_filter = (
        column.casefold() in plan.casefold() and "filter" in plan.casefold()
    )
    if not has_partition_filter:
        estimate = re.search(r"~\s*([\d,]+)\s+rows?", plan, re.IGNORECASE)
        estimated_rows = (
            int(estimate.group(1).replace(",", "")) if estimate else None
        )
        # DuckDB can fold a selective predicate away on a tiny table. Avoid
        # running COUNT(*) on an Iceberg target just to classify that plan.
        if estimated_rows is None or estimated_rows > 1_000:
            raise RuntimeError(
                f"DuckDB EXPLAIN did not retain the {column} partition filter "
                f"for {target} (estimated rows={estimated_rows}): {plan}"
            )
        print(
            f"DuckDB EXPLAIN optimized {column} filter on small target "
            f"(estimated rows={estimated_rows}); no large scan to bound",
            flush=True,
        )
        return
    print(
        f"Validated {column} pruning predicate for {target}: [{start},{stop})",
        flush=True,
    )


def publish_partitioned_order_fact(
    con,
    table: str,
    source: str,
    target: str,
    keys: tuple[str, ...],
    row_count: int,
    *,
    partition_column: str,
    partition_grain: str,
    daily: bool,
) -> None:
    """Publish order facts in week slices aligned with date partition pruning."""
    if not target_exists(con, target):
        raise ValueError(f"Published table {target} is missing; run catalog initialization first")
    if con.execute(f"SELECT 1 FROM {source} WHERE order_id IS NULL LIMIT 1").fetchone():
        raise ValueError(f"Staged relation {source} has null order_id")
    changed_ids = affected_order_relation("silver")
    if daily:
        where_changed = f"order_id IN (SELECT order_id FROM {changed_ids})"
    else:
        where_changed = "true"
    bounds_rows = con.execute(
        f"SELECT DISTINCT date_trunc('{partition_grain}', {identifier(partition_column)})::date "
        f"FROM (SELECT {identifier(partition_column)} FROM {target} WHERE {where_changed} "
        f"UNION ALL SELECT {identifier(partition_column)} FROM {source} WHERE {where_changed}) "
        f"WHERE {identifier(partition_column)} IS NOT NULL ORDER BY 1"
    ).fetchall()
    for index, (week_start,) in enumerate(bounds_rows, 1):
        start, stop = _period_bounds(week_start, partition_grain)
        _explain_partition_filter(con, target, partition_column, start, stop)
        target_filter = (
            f"target.{identifier(partition_column)} >= DATE '{start}' AND "
            f"target.{identifier(partition_column)} < DATE '{stop}'"
        )
        selected_predicate = (
            f"{identifier(partition_column)} >= DATE '{start}' AND "
            f"{identifier(partition_column)} < DATE '{stop}'"
        )
        if daily:
            selected_predicate += f" AND order_id IN (SELECT order_id FROM {changed_ids})"
        selected = f"(SELECT * FROM {source} WHERE {selected_predicate})"
        count = int(con.execute(f"SELECT count(*) FROM {selected}").fetchone()[0])
        if count:
            predicate = merge_upsert(
                con, source, target, keys, selected, target_predicate=target_filter
            )
        else:
            predicate = " AND ".join(
                f"target.{identifier(key)} IS NOT DISTINCT FROM source.{identifier(key)}"
                for key in keys
            )
        delete_filter = target_filter
        if daily:
            delete_filter += f" AND target.order_id IN (SELECT order_id FROM {changed_ids})"
        con.execute(
            f"DELETE FROM {target} AS target WHERE {delete_filter} "
            f"AND NOT EXISTS (SELECT 1 FROM {selected} AS source WHERE {predicate})"
        )
        print(
            f"silver.{table}: slice {index}/{len(bounds_rows)} "
            f"{start}..{stop} staged_rows={count}",
            flush=True,
        )


def publish_changed_order_fact(
    con, table: str, source: str, target: str, keys: tuple[str, ...], row_count: int
) -> None:
    """Merge a complete changed-order slice and remove stale rows for those orders."""
    changed_ids = affected_order_relation("silver")
    selected = f"(SELECT * FROM {source} WHERE order_id IN (SELECT order_id FROM {changed_ids}))"
    count = int(con.execute(f"SELECT count(*) FROM {selected}").fetchone()[0])
    predicate = None
    if count:
        predicate = merge_upsert(con, source, target, keys, selected)
    if target_exists(con, target):
        if predicate is None:
            predicate = " AND ".join(
                f"target.{identifier(key)} IS NOT DISTINCT FROM source.{identifier(key)}"
                for key in keys
            )
        con.execute(
            f"DELETE FROM {target} AS target "
            f"WHERE target.order_id IN (SELECT order_id FROM {changed_ids}) "
            f"AND NOT EXISTS (SELECT 1 FROM {selected} AS source WHERE {predicate})"
        )
    print(f"silver.{table}: merged complete changed-order slice {count} rows", flush=True)


def affected_months_for_silver(con) -> set:
    """Capture old and new month grains before Silver tables are mutated."""
    changed_ids = affected_order_relation("silver")
    months = set()
    for table, date_column in (
        ("fact_orders", "order_date"),
        ("fact_order_product", "order_date"),
        ("fact_delivery_tracking", "status_datetime"),
    ):
        source = ".".join(map(identifier, ("staged_silver", "silver", table)))
        target = ".".join(map(identifier, ("publish_silver", "silver", table)))
        for relation in (source, target):
            rows = con.execute(
                f"SELECT DISTINCT date_trunc('month', {identifier(date_column)})::date "
                f"FROM {relation} WHERE order_id IN (SELECT order_id FROM {changed_ids}) "
                f"AND {identifier(date_column)} IS NOT NULL"
            ).fetchall()
            months.update(row[0].replace(day=1) for row in rows)
    history = ".".join(map(identifier, ("staged_silver", "silver", "dim_client_preference_history")))
    lower = int(os.getenv("SILVER_DELTA_FROM_SEQUENCE", "0"))
    upper = int(os.getenv("SILVER_DELTA_TO_SEQUENCE", "0"))
    changed_effective = con.execute(
        f"SELECT DISTINCT date_trunc('month', effective_from)::date FROM {history} "
        "WHERE source_batch_sequence > ? AND source_batch_sequence <= ?",
        [lower, upper],
    ).fetchall()
    if changed_effective:
        all_months = set()
        # A preference correction affects every later month with client orders,
        # including orders outside today's changed-order slice. Read the
        # published history and staged corrections so the Gold boundary is
        # complete before Silver mutates its tables.
        for relation in (
            ".".join(map(identifier, ("publish_silver", "silver", "fact_orders"))),
            ".".join(map(identifier, ("staged_silver", "silver", "fact_orders"))),
        ):
            all_months.update(
                row[0].replace(day=1)
                for row in con.execute(
                    f"SELECT DISTINCT date_trunc('month', order_date)::date FROM {relation} "
                    "WHERE order_date IS NOT NULL"
                ).fetchall()
            )
        starts = [row[0].replace(day=1) for row in changed_effective]
        months.update(month for month in all_months if any(month >= start for start in starts))
    return months


def affected_month_values() -> list:
    return sorted(
        {
            date_value
            for value in os.getenv("GOLD_AFFECTED_MONTHS", "").split(",")
            if value
            for date_value in [date.fromisoformat(value)]
        }
    )


def publish_gold_months(
    con, table: str, source: str, target: str, keys: tuple[str, ...], row_count: int
) -> None:
    """Synchronize only complete Gold month slices, including explicitly empty months."""
    months = affected_month_values()
    if not months:
        print(f"gold.{table}: unchanged (no new affected months)", flush=True)
        return
    for month in months:
        month_sql = month.isoformat()
        target_filter = f"target.month = DATE '{month_sql}'"
        selected = f"(SELECT * FROM {source} WHERE month = DATE '{month_sql}')"
        count = int(con.execute(f"SELECT count(*) FROM {selected}").fetchone()[0])
        predicate = None
        if count:
            predicate = merge_upsert(
                con, source, target, keys, selected, target_predicate=target_filter
            )
        if predicate is None:
            predicate = " AND ".join(
                f"target.{identifier(key)} IS NOT DISTINCT FROM source.{identifier(key)}"
                for key in keys
            )
        con.execute(
            f"DELETE FROM {target} AS target WHERE {target_filter} "
            f"AND NOT EXISTS (SELECT 1 FROM {selected} AS source WHERE {predicate})"
        )
        print(f"gold.{table}: synchronized month={month_sql} rows={count}", flush=True)


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
    spec = CONTRACT.table(layer, table)
    keys = spec.publication["merge_keys"]
    column_names = {column["name"] for column in spec.columns}
    if layer == "gold" and run_mode == "daily_refresh":
        publish_gold_months(con, table, source, target, keys, row_count)
        action = "synchronized affected months"
    elif layer == "silver" and run_mode == "full_history" and table == "fact_order_product":
        publish_partitioned_order_fact(
            con,
            table,
            source,
            target,
            keys,
            row_count,
            partition_column="order_date",
            partition_grain="week",
            daily=False,
        )
        action = "synchronized partition-pruned history"
    elif layer == "silver" and run_mode == "full_history" and table in FULL_REBUILD_FACT_BATCHES:
        publish_fact_in_batches(
            con, table, source, target,
            keys,
            FULL_REBUILD_FACT_BATCHES[table], row_count,
            remove_missing=True,
        )
        action = "synchronized in batches"
    elif layer == "silver" and run_mode == "daily_refresh" and "order_id" in column_names:
        if table == "fact_order_product":
            publish_partitioned_order_fact(
                con,
                table,
                source,
                target,
                keys,
                row_count,
                partition_column="order_date",
                partition_grain="week",
                daily=True,
            )
        elif table == "fact_delivery_tracking":
            publish_partitioned_order_fact(
                con,
                table,
                source,
                target,
                keys,
                row_count,
                partition_column="status_datetime",
                partition_grain="month",
                daily=True,
            )
        else:
            publish_changed_order_fact(con, table, source, target, keys, row_count)
        action = "merged complete changed-order slice"
    elif row_count == 0:
        action = "unchanged (empty source slice)"
    else:
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
    con.execute(
        f"ATTACH {literal(str(workspace))} AS ampere_work (READ_ONLY)"
    )
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
    silver_affected_months = set()
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
        if layer == "silver":
            silver_affected_months = affected_months_for_silver(con)
            print(
                f"Prepared Silver affected-month set: {sorted(silver_affected_months)}",
                flush=True,
            )
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
    from incremental_state import record_gold_publication, record_silver_publication

    if layer == "silver":
        record_silver_publication(silver_affected_months)
    else:
        record_gold_publication()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--layer", required=True, choices=("silver", "gold"))
    publish(parser.parse_args().layer)
