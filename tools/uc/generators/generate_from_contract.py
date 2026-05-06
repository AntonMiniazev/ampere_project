"""Generate derived DAG metadata artifacts from canonical contracts."""

from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

from tools.py_utils.paths import project_root
from tools.py_utils.uc_contracts import iter_layer_tables, load_contract

DEFAULT_CONTRACT = "tools/uc/contracts/ampere_tables.json"


def write_json(path: Path, payload: Any) -> None:
    """Write stable, diff-friendly JSON."""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(payload, indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )


def dag_table_definitions(contract: dict[str, Any]) -> dict[str, Any]:
    """Build `dags/config/table_definitions.json` from canonical table metadata."""
    definitions: dict[str, Any] = {
        "events": {},
        "facts": {},
        "mutable_dims": {},
        "snapshots": [],
    }
    for table in iter_layer_tables(contract, "bronze"):
        runtime = table.get("stream_group")
        if not runtime:
            continue
        group = runtime["group"]
        table_name = table["table_name"]
        if group == "snapshots":
            definitions["snapshots"].append((int(runtime.get("order", 0)), table_name))
            continue
        values = {
            key: value
            for key, value in runtime.items()
            if key not in {"group", "group_config", "group_order", "order"}
        }
        definitions[group][table_name] = {
            "__order": int(runtime.get("order", 0)),
            **values,
        }

    definitions["snapshots"] = [
        table_name
        for _, table_name in sorted(definitions["snapshots"], key=lambda item: item[0])
    ]
    for group in ("events", "facts", "mutable_dims"):
        ordered_items = sorted(
            definitions[group].items(),
            key=lambda item: item[1].pop("__order"),
        )
        definitions[group] = dict(ordered_items)
    return definitions


def dag_stream_groups(contract: dict[str, Any]) -> list[dict[str, Any]]:
    """Build `dags/config/stream_groups.json` from bronze table stream metadata."""
    groups: dict[str, dict[str, Any]] = {}
    order: dict[str, int] = {}
    for table in iter_layer_tables(contract, "bronze"):
        runtime = table.get("stream_group")
        if not runtime:
            continue
        group = runtime["group"]
        groups.setdefault(
            group,
            {
                "group": group,
                **runtime.get("group_config", {}),
            },
        )
        order.setdefault(group, int(runtime.get("group_order", len(order))))
    return [
        groups[group]
        for group, _ in sorted(order.items(), key=lambda item: item[1])
    ]


def main() -> None:
    """Generate all derived files from the canonical contract."""
    root = project_root(__file__)
    contract = load_contract(root / DEFAULT_CONTRACT)
    write_json(
        root / "dags" / "config" / "table_definitions.json",
        dag_table_definitions(contract),
    )
    write_json(
        root / "dags" / "config" / "stream_groups.json",
        dag_stream_groups(contract),
    )
    print(f"Generated artifacts from {DEFAULT_CONTRACT}")


if __name__ == "__main__":
    main()
