from __future__ import annotations

import json
from pathlib import Path

CONFIG_DIR = Path(__file__).resolve().parent / "config"
TABLES_PATH = CONFIG_DIR / "table_definitions.json"
STREAM_GROUPS_PATH = CONFIG_DIR / "stream_groups.json"


def _load_json(path: Path):
    return json.loads(path.read_text())


def load_table_definitions() -> dict:
    return _load_json(TABLES_PATH)


def load_stream_groups(event_lookback_days: int) -> list[dict]:
    groups = _load_json(STREAM_GROUPS_PATH)
    for group in groups:
        if group.get("group") == "events":
            group["lookback_days"] = event_lookback_days
    return groups


def build_raw_stream_groups(event_lookback_days: int) -> list[dict]:
    tables = load_table_definitions()
    groups = load_stream_groups(event_lookback_days)
    stream_groups = []
    for group in groups:
        name = group["group"]
        if name == "snapshots":
            tables_list = tables["snapshots"]
            table_config = {}
        else:
            table_map = tables[name]
            tables_list = list(table_map.keys())
            if name == "mutable_dims":
                table_config = {
                    table: {
                        "watermark_column": table_map[table].get("watermark_column", "")
                    }
                    for table in tables_list
                }
            else:
                table_config = {
                    table: {
                        "event_date_column": table_map[table].get("event_date_column", "")
                    }
                    for table in tables_list
                }
        stream_groups.append(
            {
                **group,
                "tables": tables_list,
                "table_config": table_config,
                "event_date_column": "",
                "watermark_column": "",
            }
        )
    return stream_groups


def build_bronze_stream_groups(event_lookback_days: int) -> list[dict]:
    tables = load_table_definitions()
    groups = load_stream_groups(event_lookback_days)
    stream_groups = []
    for group in groups:
        name = group["group"]
        if name == "snapshots":
            tables_list = tables["snapshots"]
            table_config = {}
        else:
            table_map = tables[name]
            tables_list = list(table_map.keys())
            table_config = {
                table: {"merge_keys": table_map[table].get("merge_keys", [])}
                for table in tables_list
            }
        stream_groups.append(
            {
                **group,
                "tables": tables_list,
                "table_config": table_config,
                "event_date_column": "",
            }
        )
    return stream_groups
