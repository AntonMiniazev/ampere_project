from __future__ import annotations

import json
from pathlib import Path

CONFIG_DIR = Path(__file__).resolve().parents[1] / "config"
TABLES_PATH = CONFIG_DIR / "table_definitions.json"
STREAM_GROUPS_PATH = CONFIG_DIR / "stream_groups.json"


def _load_json(path: Path):
    """Load a small configuration JSON file from the DAG config directory.

    The pipeline keeps table and group definitions outside the DAG code so they
    are easier to review and change. This helper gives the DAG utilities one
    obvious place where that JSON parsing happens.
    """
    return json.loads(path.read_text())


def load_table_definitions() -> dict:
    """Load the per-table metadata used by raw and bronze orchestration.

    This data describes which tables belong to which groups and which technical
    fields matter for them, such as merge keys, watermarks, and event-date
    columns. In practice it is the declarative map that keeps the DAGs from
    hardcoding table logic inline.
    """
    return _load_json(TABLES_PATH)


def load_stream_groups(event_lookback_days: int) -> list[dict]:
    """Load the logical execution groups and inject the current event lookback.

    The events group has a configurable lookback window that Airflow can control
    through variables. Updating that value at load time keeps the JSON mostly
    static while still allowing the DAG to tune how much recent history is
    rescanned on each run.
    """
    groups = _load_json(STREAM_GROUPS_PATH)
    for group in groups:
        if group.get("group") == "events":
            group["lookback_days"] = event_lookback_days
    return groups


def build_raw_stream_groups(event_lookback_days: int) -> list[dict]:
    """Combine table definitions and group settings into raw-landing run groups.

    Raw extraction needs to know more than just table names: mutable dimensions
    need watermark fields and event/fact tables need event-date semantics. This
    helper prepares the exact structure the raw DAG passes into its Spark
    application template.
    """
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
                        "watermark_column": table_map[table].get("watermark_column", ""),
                        "created_column": table_map[table].get("created_column", ""),
                    }
                    for table in tables_list
                }
            else:
                table_config = {
                    table: {
                        "event_date_column": table_map[table].get("event_date_column", ""),
                        "lookback_days": table_map[table].get("lookback_days", None),
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
    """Combine table definitions and group settings into bronze run groups.

    Bronze processing has different needs from raw landing: it cares about merge
    keys and lookback behavior rather than JDBC watermark columns. This helper
    reshapes the same declarative config into the form expected by the bronze
    Spark application.
    """
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
                table: {
                    "merge_keys": table_map[table].get("merge_keys", []),
                    "lookback_days": table_map[table].get("lookback_days", None),
                }
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
