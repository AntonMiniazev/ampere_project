from __future__ import annotations

import json
import os
import sys
import urllib.error
import urllib.parse
import urllib.request
import ast
from pathlib import Path
from typing import Any

try:
    import yaml
except (
    ImportError
) as exc:  # pragma: no cover - exercised only in incomplete environments.
    raise SystemExit(
        "PyYAML is required. Install it with `pip install pyyaml` and rerun the generator."
    ) from exc


ROOT = Path(__file__).resolve().parents[2]
DATAFLOW_PATH = ROOT / "docs" / "dataflow" / "dataflow.yml"
DIAGRAMS_DIR = ROOT / "docs" / "dataflow" / "diagrams"
GENERATED_DIR = ROOT / "docs" / "dataflow" / "generated"
TABLE_DEFINITIONS_PATH = ROOT / "dags" / "config" / "table_definitions.json"
STREAM_GROUPS_PATH = ROOT / "dags" / "config" / "stream_groups.json"
DATA_CONTRACTS_DIR = ROOT / "docs" / "data_contracts"
DEFAULT_UC_CONTRACT_LAYERS = ("bronze", "silver", "gold")
UC_INVENTORY_MD = GENERATED_DIR / "uc_table_inventory.md"
DAGS_DIR = ROOT / "dags" / "my_dags"


def node_id(value: str, prefix: str = "N") -> str:
    """Build a deterministic Mermaid node id."""
    return prefix + "_" + "".join(ch if ch.isalnum() else "_" for ch in value.upper())


def load_yaml(path: Path) -> dict[str, Any]:
    """Load the project dataflow YAML and enforce an object root."""
    if not path.exists():
        raise FileNotFoundError(f"Missing dataflow config: {path}")
    parsed = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(parsed, dict):
        raise ValueError(f"{path} must contain a YAML mapping at the root.")
    return parsed


def validate_dataflow(config: dict[str, Any]) -> None:
    """Fail fast when required top-level dataflow sections are absent or malformed."""
    for key in ("project", "layers", "flows"):
        if key not in config:
            raise ValueError(f"dataflow.yml is missing required section: {key}")
    if not isinstance(config["layers"], dict) or not config["layers"]:
        raise ValueError("dataflow.yml section `layers` must be a non-empty mapping.")
    if not isinstance(config["flows"], list) or not config["flows"]:
        raise ValueError("dataflow.yml section `flows` must be a non-empty list.")
    known_layers = set(config["layers"])
    for index, flow in enumerate(config["flows"], start=1):
        if not isinstance(flow, dict):
            raise ValueError(f"flows[{index}] must be a mapping.")
        for key in ("from", "to", "engine"):
            if not flow.get(key):
                raise ValueError(f"flows[{index}] is missing required field: {key}")
        if flow["from"] not in known_layers:
            raise ValueError(
                f"flows[{index}] references unknown source layer: {flow['from']}"
            )
        if flow["to"] not in known_layers:
            raise ValueError(
                f"flows[{index}] references unknown target layer: {flow['to']}"
            )


def as_list(value: Any) -> list[str]:
    """Normalize optional scalar/list YAML fields into a display-ready list."""
    if value is None:
        return []
    if isinstance(value, list):
        return [str(item) for item in value if str(item).strip()]
    if str(value).strip():
        return [str(value)]
    return []


def markdown_escape(value: str) -> str:
    """Escape Markdown table separators in generated cells."""
    return value.replace("|", "\\|").replace("\n", " ")


def mermaid_text(value: Any) -> str:
    """Escape text for Mermaid quoted labels."""
    return str(value).replace('"', "'").replace("\n", " ").strip()


def layer_node_id(layer_name: str) -> str:
    """Build a deterministic Mermaid node id for a layer name."""
    return node_id(layer_name, "L")


def write_mermaid_markdown(
    path: Path,
    *,
    title: str,
    description: list[str],
    mermaid: str,
) -> None:
    """Write a GitHub-renderable Mermaid diagram page."""
    lines = [f"# {title}", "", *description, "", "```mermaid", mermaid.rstrip(), "```", ""]
    write_text(path, "\n".join(lines))


def write_project_dataflow(config: dict[str, Any]) -> None:
    """Render the high-level layer transition Mermaid flowchart."""
    layers: dict[str, dict[str, Any]] = config["layers"]
    lines = [
        "flowchart LR",
    ]
    for name, layer in layers.items():
        detail_parts = []
        engines = as_list(layer.get("engine"))
        storage = as_list(layer.get("storage"))
        catalog = str(layer.get("catalog") or "").strip()
        if engines:
            detail_parts.append(", ".join(engines))
        if storage:
            detail_parts.append(", ".join(storage))
        if catalog:
            detail_parts.append(catalog)
        label = mermaid_text(layer.get("title", name))
        diagram_note = str(layer.get("diagram_note") or "").strip()
        if diagram_note:
            label += "<br/>Logic: " + mermaid_text(diagram_note)
        if detail_parts:
            label += "<br/>" + "<br/>".join(mermaid_text(part) for part in detail_parts)
        lines.append(f'    {layer_node_id(name)}["{label}"]')

    lines.append("")
    for flow in config["flows"]:
        from_id = layer_node_id(str(flow["from"]))
        to_id = layer_node_id(str(flow["to"]))
        engine = mermaid_text(flow["engine"])
        lines.append(f'    {from_id} -->|"{engine}"| {to_id}')

    mermaid = "\n".join(lines) + "\n"
    write_text(DIAGRAMS_DIR / "project_dataflow.mmd", mermaid)
    write_mermaid_markdown(
        GENERATED_DIR / "project_dataflow.md",
        title="Project dataflow",
        description=[
            "This flowchart shows the high-level layer transitions, the main logic inside each layer, and the engine used between layers.",
        ],
        mermaid=mermaid,
    )


def write_layer_responsibilities(config: dict[str, Any]) -> None:
    """Render a Markdown responsibilities table from dataflow.yml."""
    lines = [
        "# Layer responsibilities",
        "",
        "| Layer | Status | Engine | Storage | Catalog | Responsibility |",
        "|---|---|---|---|---|---|",
    ]
    for name, layer in config["layers"].items():
        title = markdown_escape(str(layer.get("title") or name))
        status = markdown_escape(str(layer.get("status") or "-"))
        engine = markdown_escape(", ".join(as_list(layer.get("engine"))) or "-")
        storage = markdown_escape(", ".join(as_list(layer.get("storage"))) or "-")
        catalog_values = [
            str(layer.get("catalog") or "").strip(),
            str(layer.get("catalog_schema") or "").strip(),
        ]
        catalog = markdown_escape(
            " / ".join(value for value in catalog_values if value) or "-"
        )
        responsibility = markdown_escape(str(layer.get("responsibility") or "-"))
        lines.append(
            f"| {title} | {status} | {engine} | {storage} | {catalog} | {responsibility} |"
        )

    write_text(GENERATED_DIR / "layer_responsibilities.md", "\n".join(lines) + "\n")


def load_json(path: Path) -> Any:
    """Read a checked-in JSON config file."""
    return json.loads(path.read_text(encoding="utf-8"))


def table_list_for_group(group_name: str, definitions: dict[str, Any]) -> list[str]:
    """Return deterministic table names for one raw/bronze stream group."""
    if group_name == "snapshots":
        return sorted(str(item) for item in definitions.get("snapshots", []))
    group_definition = definitions.get(group_name, {})
    if isinstance(group_definition, dict):
        return sorted(str(item) for item in group_definition)
    return []


def group_behavior(group_name: str, group_config: dict[str, Any]) -> tuple[str, str]:
    """Translate current stream group config into raw and bronze behavior labels."""
    partition_key = str(group_config.get("partition_key") or "-")
    mode = str(group_config.get("mode") or "-")
    lookback_days = int(group_config.get("lookback_days") or 0)
    if group_name == "snapshots":
        return (
            f"Raw partition: {partition_key}",
            "Bronze behavior: partition replacement",
        )
    if group_name == "mutable_dims":
        return "Raw partition: extract_date + watermark", "Bronze behavior: merge"
    if group_name == "events":
        return (
            f"Raw partition: {partition_key} + {lookback_days}-day lookback",
            "Bronze behavior: merge",
        )
    return f"Raw partition: {partition_key}", f"Bronze behavior: {mode}"


def write_table_groups() -> None:
    """Render table group movement across the full source-to-gold pipeline."""
    if TABLE_DEFINITIONS_PATH.exists() and STREAM_GROUPS_PATH.exists():
        definitions = load_json(TABLE_DEFINITIONS_PATH)
        stream_groups = load_json(STREAM_GROUPS_PATH)
    else:
        definitions = {
            "snapshots": [
                "stores",
                "zones",
                "product_categories",
                "order_statuses",
                "delivery_type",
                "delivery_costing",
                "assortment",
            ],
            "mutable_dims": {
                "clients": {},
                "delivery_resource": {},
                "products": {},
                "costing": {},
            },
            "facts": {
                "orders": {},
                "order_product": {},
                "payments": {},
            },
            "events": {
                "order_status_history": {},
                "delivery_tracking": {},
            },
        }
        stream_groups = [
            {
                "group": "snapshots",
                "mode": "snapshot",
                "partition_key": "snapshot_date",
                "lookback_days": 0,
            },
            {
                "group": "mutable_dims",
                "mode": "incremental",
                "partition_key": "extract_date",
                "lookback_days": 0,
            },
            {
                "group": "facts",
                "mode": "incremental",
                "partition_key": "event_date",
                "lookback_days": 0,
            },
            {
                "group": "events",
                "mode": "incremental",
                "partition_key": "event_date",
                "lookback_days": 2,
            },
        ]

    lines = [
        "flowchart LR",
        '    PG["PostgreSQL source"]',
    ]
    silver_labels = {
        "snapshots": "Silver snapshot dimensions<br/>Reference lookups and denormalized labels",
        "mutable_dims": "Silver mutable dimensions<br/>Current row plus valid_from / valid_to history",
        "facts": "Silver facts<br/>Orders, order lines, payments, and reusable rollups",
        "events": "Silver event facts<br/>Status and delivery event history with late-arrival tolerance",
    }
    gold_labels = {
        "snapshots": "Gold reference lookups<br/>Stores and delivery tariff context",
        "mutable_dims": "Gold dimensions and cost helpers<br/>Client/product/resource lookup and order-date product cost",
        "facts": "Gold sales and margin facts<br/>Order sales, product cost, delivery cost, gross profit",
        "events": "Gold delivery facts<br/>Delivery status timeline for BI consumption",
    }
    for group_config in stream_groups:
        group_name = str(group_config.get("group") or "").strip()
        if not group_name:
            continue
        node_prefix = node_id(group_name, "G")
        tables = table_list_for_group(group_name, definitions)
        table_label = ", ".join(tables) if tables else "No tables configured"
        raw_label, bronze_label = group_behavior(group_name, group_config)
        title = group_name.replace("_", " ").title()
        lines.extend(
            [
                f'    {node_prefix}["{mermaid_text(title)}<br/>{mermaid_text(table_label)}"]',
                f'    {node_prefix}_RAW["{mermaid_text(raw_label)}"]',
                f'    {node_prefix}_BRONZE["{mermaid_text(bronze_label)}"]',
                f'    {node_prefix}_SILVER["{mermaid_text(silver_labels.get(group_name, "Silver models"))}"]',
                f'    {node_prefix}_GOLD["{mermaid_text(gold_labels.get(group_name, "Gold marts"))}"]',
                f"    PG --> {node_prefix} --> {node_prefix}_RAW --> {node_prefix}_BRONZE --> {node_prefix}_SILVER --> {node_prefix}_GOLD",
            ]
        )

    mermaid = "\n".join(lines) + "\n"
    write_text(DIAGRAMS_DIR / "table_groups.mmd", mermaid)
    write_mermaid_markdown(
        GENERATED_DIR / "table_groups.md",
        title="Table movement",
        description=[
            "This flowchart shows how source table groups move through Raw, Bronze, Silver, and Gold.",
        ],
        mermaid=mermaid,
    )


def literal_string(node: ast.AST | None) -> str | None:
    """Return a string literal from a small AST expression when available."""
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        return node.value
    return None


def literal_bool(node: ast.AST | None) -> bool | None:
    """Return a boolean literal from a small AST expression when available."""
    if isinstance(node, ast.Constant) and isinstance(node.value, bool):
        return node.value
    return None


def schedule_label(node: ast.AST | None) -> str:
    """Return a display label for a DAG schedule AST expression."""
    if node is None:
        return "unknown"
    if isinstance(node, ast.Constant) and node.value is None:
        return "manual / triggered"
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        return node.value
    return "dynamic"


def trigger_rule_label(node: ast.AST | None) -> str:
    """Return a readable trigger rule label from an AST expression."""
    if node is None:
        return "all_success"
    if isinstance(node, ast.Attribute):
        return node.attr
    if isinstance(node, ast.Constant):
        return str(node.value)
    return "dynamic"


def keyword(call: ast.Call, name: str) -> ast.AST | None:
    """Return a keyword value from an AST call."""
    for item in call.keywords:
        if item.arg == name:
            return item.value
    return None


def parse_airflow_dag_file(path: Path) -> dict[str, Any]:
    """Extract DAG metadata and cross-DAG triggers from one DAG Python file."""
    tree = ast.parse(path.read_text(encoding="utf-8"))
    constants: dict[str, str] = {}
    for node in ast.walk(tree):
        if isinstance(node, ast.Assign):
            value = literal_string(node.value)
            if value is None:
                continue
            for target in node.targets:
                if isinstance(target, ast.Name):
                    constants[target.id] = value

    dag_id = constants.get("DAG_ID") or path.stem
    schedule = "unknown"
    tags: list[str] = []
    triggers: list[dict[str, Any]] = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        func_name = getattr(node.func, "id", "") or getattr(node.func, "attr", "")
        if func_name == "DAG":
            dag_id_kw = keyword(node, "dag_id")
            if isinstance(dag_id_kw, ast.Name) and dag_id_kw.id in constants:
                dag_id = constants[dag_id_kw.id]
            elif literal_string(dag_id_kw):
                dag_id = literal_string(dag_id_kw) or dag_id
            schedule = schedule_label(keyword(node, "schedule"))
            tags_node = keyword(node, "tags")
            if isinstance(tags_node, ast.List):
                tags = [
                    item.value
                    for item in tags_node.elts
                    if isinstance(item, ast.Constant) and isinstance(item.value, str)
                ]
        if func_name == "TriggerDagRunOperator":
            triggers.append(
                {
                    "task_id": literal_string(keyword(node, "task_id")) or "trigger",
                    "target": literal_string(keyword(node, "trigger_dag_id")) or "unknown",
                    "wait_for_completion": literal_bool(keyword(node, "wait_for_completion")),
                    "reset_dag_run": literal_bool(keyword(node, "reset_dag_run")),
                    "trigger_rule": trigger_rule_label(keyword(node, "trigger_rule")),
                }
            )
    return {
        "dag_id": dag_id,
        "file": str(path.relative_to(ROOT)).replace("\\", "/"),
        "schedule": schedule,
        "tags": tags,
        "triggers": triggers,
    }


def trigger_condition(source: str, target: str, trigger: dict[str, Any]) -> str:
    """Describe cross-DAG trigger conditions for generated docs."""
    trigger_rule = str(trigger.get("trigger_rule") or "all_success")
    wait = trigger.get("wait_for_completion")
    parts: list[str] = []
    if trigger_rule != "all_success":
        parts.append(f"trigger_rule={trigger_rule}")
    else:
        parts.append("upstream success")
    if wait is True:
        parts.append("waits for completion")
    elif wait is False:
        parts.append("does not wait")
    if source == "ampere__silver_gold__dbt_duckdb__daily" and target == "ampere__housekeeping__bronze_delta_cleanup__weekly":
        parts.append("after Silver/Gold dbt reaches terminal state")
    if target == "ampere__housekeeping__bronze_delta_cleanup__weekly":
        parts.append("cleanup runs on Sunday or bronze_optimization=true, otherwise skips")
    return "; ".join(parts)


def dag_short_label(dag_id: str) -> str:
    """Return a compact DAG label for Mermaid nodes."""
    label = dag_id.removeprefix("ampere__")
    return mermaid_text(label.replace("__", "<br/>"))


def write_airflow_dag_orchestration() -> None:
    """Generate Airflow DAG dependency documentation and Mermaid flowchart."""
    dag_files = sorted(path for path in DAGS_DIR.glob("ampere__*.py") if path.is_file())
    dags = [parse_airflow_dag_file(path) for path in dag_files]
    dag_by_id = {dag["dag_id"]: dag for dag in dags}

    lines = [
        "flowchart TD",
        '    START(["Scheduled daily start<br/>04:15"])',
    ]
    for dag in dags:
        schedule = mermaid_text(dag["schedule"])
        label = f"{dag_short_label(dag['dag_id'])}<br/>schedule: {schedule}"
        lines.append(f'    {node_id(dag["dag_id"], "D")}["{label}"]')
    lines.append("")
    daily_id = "ampere__pre_raw__generators__daily"
    if daily_id in dag_by_id:
        lines.append(f"    START --> {node_id(daily_id, 'D')}")
    for dag in dags:
        source_id = dag["dag_id"]
        for trigger in dag["triggers"]:
            target = str(trigger["target"])
            label = mermaid_text(trigger_condition(source_id, target, trigger))
            lines.append(
                f'    {node_id(source_id, "D")} -->|"{label}"| {node_id(target, "D")}'
            )

    lines.extend(
        [
            "",
            f'    {node_id("ampere__pre_raw__generators__init", "D")}:::manual',
            f'    {node_id("ampere__silver__dbt_duckdb__full_rebuild", "D")}:::manual',
            f'    {node_id("ampere__gold__dbt_duckdb__full_rebuild", "D")}:::manual',
            f'    {node_id("ampere__gold__refresh_from_silver__adhoc", "D")}:::manual',
            "    classDef manual fill:#dbeafe,stroke:#2563eb,color:#111827,stroke-dasharray: 4 3",
        ]
    )
    mermaid = "\n".join(lines) + "\n"
    write_text(DIAGRAMS_DIR / "airflow_dags.mmd", mermaid)

    md_lines = [
        "# Airflow DAG orchestration",
        "",
        "This page is generated from `dags/my_dags/*.py`. It shows the normal daily chain, manual recovery entrypoints, and the trigger conditions that matter operationally.",
        "",
        "Daily work is intentionally narrow: the scheduled generator starts the chain, Raw and Bronze run as Spark jobs, and the combined Silver/Gold dbt job publishes analytical tables. Manual DAGs are kept outside the daily path so rebuilds and ad hoc Gold refreshes are explicit recovery actions.",
        "",
        "The housekeeping DAG is triggered after the Silver/Gold dbt pod reaches a terminal state. It still skips work unless the run is Sunday or the Airflow variable `bronze_optimization=true` is set.",
        "",
        "```mermaid",
        mermaid.rstrip(),
        "```",
        "",
        "## DAG Inventory",
        "",
        "| DAG | Schedule | Tags | Source file |",
        "|---|---|---|---|",
    ]
    for dag in sorted(dags, key=lambda item: item["dag_id"]):
        md_lines.append(
            f"| `{markdown_escape(dag['dag_id'])}` | {markdown_escape(dag['schedule'])} | "
            f"{markdown_escape(', '.join(dag['tags']) or '-')} | `{markdown_escape(dag['file'])}` |"
        )

    md_lines.extend(
        [
            "",
            "## Cross-DAG Triggers",
            "",
            "| Source DAG | Target DAG | Task | Condition |",
            "|---|---|---|---|",
        ]
    )
    for dag in sorted(dags, key=lambda item: item["dag_id"]):
        for trigger in dag["triggers"]:
            condition = trigger_condition(dag["dag_id"], str(trigger["target"]), trigger)
            md_lines.append(
                f"| `{markdown_escape(dag['dag_id'])}` | `{markdown_escape(str(trigger['target']))}` | "
                f"`{markdown_escape(str(trigger['task_id']))}` | {markdown_escape(condition)} |"
            )
    write_text(GENERATED_DIR / "airflow_dag_orchestration.md", "\n".join(md_lines) + "\n")


def uc_base_url() -> str:
    """Resolve the UC API base URL from project-standard environment variables."""
    return (
        (os.getenv("UC_ENDPOINT") or os.getenv("UC_API_URI") or "").strip().rstrip("/")
    )


def uc_token() -> str:
    """Read an optional UC bearer token without hardcoding credentials."""
    return (os.getenv("UC_TOKEN") or "").strip()


def uc_timeout_seconds() -> int:
    """Read the UC request timeout and keep invalid values from crashing startup."""
    raw = (os.getenv("UC_TIMEOUT_SECONDS") or "10").strip()
    try:
        return max(int(raw), 1)
    except ValueError:
        return 10


def uc_strict_mode() -> bool:
    """Return whether UC extraction failures should fail the docs generator."""
    return (os.getenv("UC_DOCS_STRICT") or "").strip().lower() in {"1", "true", "yes"}


def uc_get_json(base_url: str, path: str, query: dict[str, str]) -> dict[str, Any]:
    """Execute a Unity Catalog OSS GET request and parse the JSON response."""
    encoded_query = urllib.parse.urlencode(query)
    url = f"{base_url}{path}"
    if encoded_query:
        url = f"{url}?{encoded_query}"
    request = urllib.request.Request(
        url, headers={"Accept": "application/json"}, method="GET"
    )
    token = uc_token()
    if token and token.lower() != "not-used":
        request.add_header("Authorization", f"Bearer {token}")
    try:
        with urllib.request.urlopen(request, timeout=uc_timeout_seconds()) as response:
            return json.loads(response.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="ignore")
        raise RuntimeError(
            f"UC request failed ({exc.code}) for {url}: {detail}"
        ) from exc
    except urllib.error.URLError as exc:
        raise RuntimeError(f"UC request failed for {url}: {exc}") from exc
    except TimeoutError as exc:
        raise RuntimeError(f"UC request timed out for {url}") from exc


def fetch_uc_tables_for_schema(
    base_url: str, catalog: str, schema: str
) -> list[dict[str, Any]]:
    """Fetch one UC schema table listing with pagination support."""
    rows: list[dict[str, Any]] = []
    page_token = ""
    while True:
        query = {"catalog_name": catalog, "schema_name": schema}
        if page_token:
            query["page_token"] = page_token
        payload = uc_get_json(base_url, "/api/2.1/unity-catalog/tables", query)
        table_rows = payload.get("tables") or []
        if not isinstance(table_rows, list):
            raise RuntimeError(
                f"UC table listing for {catalog}.{schema} did not return a table list."
            )
        rows.extend(row for row in table_rows if isinstance(row, dict))
        page_token = str(payload.get("next_page_token") or "").strip()
        if not page_token:
            break
    return rows


def normalize_uc_inventory_row(
    catalog: str, schema: str, row: dict[str, Any]
) -> dict[str, str]:
    """Normalize one UC table payload into the generated inventory shape."""
    return {
        "catalog": str(row.get("catalog_name") or catalog),
        "schema": str(row.get("schema_name") or schema),
        "table": str(row.get("name") or row.get("table_name") or ""),
        "layer": schema,
        "storage_location": str(row.get("storage_location") or "-"),
        "comment": str(row.get("comment") or "-"),
    }


def write_uc_inventory() -> None:
    """Generate a UC table inventory from contracts, live UC, or a placeholder."""
    contract_rows = load_contract_inventory_rows()
    if contract_rows:
        write_uc_inventory_rows(
            rows=contract_rows,
            source_label="docs/data_contracts/*.json",
            extracted_at=latest_contract_extracted_at(),
        )
        return

    preserve_without_source = uc_inventory_mode() == "preserve"

    base_url = uc_base_url()
    catalog = (os.getenv("UC_CATALOG") or "ampere").strip()
    schemas = [
        schema.strip()
        for schema in (os.getenv("UC_SCHEMAS") or "bronze,silver,gold").split(",")
        if schema.strip()
    ]
    if not base_url:
        if preserve_without_source and UC_INVENTORY_MD.exists():
            return
        write_uc_inventory_placeholder("UC connection settings were not provided.")
        return

    inventory: list[dict[str, str]] = []
    try:
        for schema in sorted(schemas):
            rows = fetch_uc_tables_for_schema(base_url, catalog, schema)
            inventory.extend(
                normalize_uc_inventory_row(catalog, schema, row) for row in rows
            )
    except Exception as exc:
        if uc_strict_mode():
            raise
        if preserve_without_source and UC_INVENTORY_MD.exists():
            return
        write_uc_inventory_placeholder(str(exc))
        return
    write_uc_inventory_rows(
        rows=inventory,
        source_label=base_url,
        extracted_at="-",
    )


def load_contract_inventory_rows() -> list[dict[str, str]]:
    """Load compact per-layer data contracts and return inventory rows."""
    rows: list[dict[str, str]] = []
    for layer in DEFAULT_UC_CONTRACT_LAYERS:
        contract_path = DATA_CONTRACTS_DIR / f"{layer}.json"
        if not contract_path.exists():
            continue
        contract = json.loads(contract_path.read_text(encoding="utf-8"))
        if not isinstance(contract, dict):
            raise ValueError(f"Data contract must be a JSON object: {contract_path}")
        tables = contract.get("tables")
        if not isinstance(tables, list):
            raise ValueError(
                f"Data contract is missing list field `tables`: {contract_path}"
            )
        catalog = str(contract.get("catalog") or "")
        schema = str(contract.get("schema") or layer)
        rows.extend(
            normalize_uc_inventory_row(catalog, schema, table)
            for table in tables
            if isinstance(table, dict)
        )
    return rows


def latest_contract_extracted_at() -> str:
    """Return a compact summary of extraction timestamps from data contracts."""
    timestamps: list[str] = []
    for layer in DEFAULT_UC_CONTRACT_LAYERS:
        contract_path = DATA_CONTRACTS_DIR / f"{layer}.json"
        if not contract_path.exists():
            continue
        contract = json.loads(contract_path.read_text(encoding="utf-8"))
        if isinstance(contract, dict):
            timestamp = str(contract.get("extracted_at_utc") or "").strip()
            if timestamp:
                timestamps.append(timestamp)
    return max(timestamps) if timestamps else "-"


def write_uc_inventory_rows(
    *,
    rows: list[dict[str, str]],
    source_label: str,
    extracted_at: str,
) -> None:
    """Render normalized UC inventory rows to Markdown."""
    inventory = sorted(
        [row for row in rows if row["table"]],
        key=lambda row: (row["catalog"], row["schema"], row["table"]),
    )

    lines = [
        "# Unity Catalog table inventory",
        "",
        f"Source: `{source_label}`",
        f"Extracted at UTC: `{extracted_at}`",
        "",
        "| Catalog | Schema | Table | Layer | Storage location | Comment / Description |",
        "|---|---|---|---|---|---|",
    ]
    for row in inventory:
        lines.append(
            "| "
            + " | ".join(
                markdown_escape(row[key])
                for key in (
                    "catalog",
                    "schema",
                    "table",
                    "layer",
                    "storage_location",
                    "comment",
                )
            )
            + " |"
        )
    write_text(UC_INVENTORY_MD, "\n".join(lines) + "\n")


def uc_inventory_mode() -> str:
    """Choose how the generator behaves when no local or live UC source exists."""
    return (os.getenv("UC_INVENTORY_MODE") or "placeholder").strip().lower()


def write_uc_inventory_placeholder(reason: str) -> None:
    """Write a deterministic placeholder when UC metadata is unavailable."""
    placeholder = "\n".join(
        [
            "# Unity Catalog table inventory",
            "",
            "Unity Catalog metadata extraction was skipped.",
            "",
            f"Reason: {reason}",
            "",
            "Set `UC_ENDPOINT` or `UC_API_URI`, optionally `UC_TOKEN`, and `UC_CATALOG` to generate this inventory from Unity Catalog OSS.",
            "Set `UC_DOCS_STRICT=true` when a missing or unreachable UC endpoint should fail the generator.",
            "",
        ]
    )
    write_text(UC_INVENTORY_MD, placeholder)


def write_text(path: Path, content: str) -> None:
    """Write generated UTF-8 text after ensuring the parent directory exists."""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def main() -> None:
    """Generate deterministic repository-level dataflow documentation artifacts."""
    config = load_yaml(DATAFLOW_PATH)
    validate_dataflow(config)
    write_project_dataflow(config)
    write_layer_responsibilities(config)
    write_table_groups()
    write_airflow_dag_orchestration()
    write_uc_inventory()


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(f"generate_dataflow_docs failed: {exc}", file=sys.stderr)
        raise
