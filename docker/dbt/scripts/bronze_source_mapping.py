from __future__ import annotations

import json
import os
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import yaml

DEFAULT_SOURCE_FILE = Path("models") / "staging" / "_sources.yml"
DEFAULT_SOURCE_NAME = "bronze"
DEFAULT_UC_API_URI_LOCAL = "http://ucatalog.local"
DEFAULT_UC_API_URI_CLUSTER = (
    "http://unity-catalog-unitycatalog-server.unity-catalog.svc.cluster.local:8080"
)


def utc_now_iso() -> str:
    """Return the current UTC timestamp in ISO-8601 format."""
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def detect_default_project_dir() -> Path:
    """Detect dbt project dir for local/devcontainer and container runtimes."""
    candidates: list[Path] = []
    env_project_dir = (os.getenv("DBT_PROJECT_DIR") or "").strip()
    if env_project_dir:
        candidates.append(Path(env_project_dir))
    candidates.append(Path.cwd() / "dbt")
    candidates.append(Path("/app/dbt"))

    for candidate in candidates:
        if (candidate / "dbt_project.yml").exists():
            return candidate
    return candidates[0]


def default_mapping_path_for_project(project_dir: Path) -> Path:
    """Choose default mapping artifact path based on the runtime project layout."""
    if project_dir == Path("/app/dbt") or project_dir.parent == Path("/app"):
        return Path("/app/artifacts/bronze_source_mapping.json")
    return project_dir / ".dbt_local" / "bronze_source_mapping.json"


def default_uc_api_uri_for_project(project_dir: Path) -> str:
    """Choose default UC API URI for local/devcontainer versus cluster runtime."""
    env_uri = (os.getenv("UC_API_URI") or "").strip()
    if env_uri:
        return env_uri
    if project_dir == Path("/app/dbt") or project_dir.parent == Path("/app"):
        return DEFAULT_UC_API_URI_CLUSTER
    return DEFAULT_UC_API_URI_LOCAL


def normalize_uc_api_uri(value: str) -> str:
    """Normalize a Unity Catalog API base URI by trimming trailing slashes."""
    normalized = (value or "").strip().rstrip("/")
    if not normalized:
        raise ValueError("UC API URI is empty.")
    return normalized


def parse_iso_utc(value: str) -> datetime:
    """Parse an ISO-8601 datetime into a timezone-aware UTC datetime."""
    raw = (value or "").strip()
    if not raw:
        raise ValueError("Empty timestamp value.")
    if raw.endswith("Z"):
        raw = raw[:-1] + "+00:00"
    parsed = datetime.fromisoformat(raw)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def resolve_source_file(project_dir: Path) -> Path:
    """Return the expected dbt bronze source-definition file path."""
    return project_dir / DEFAULT_SOURCE_FILE


def load_required_source_tables(project_dir: Path, source_name: str) -> list[str]:
    """Load and return required table names for a dbt source definition."""
    source_file = resolve_source_file(project_dir)
    if not source_file.exists():
        raise FileNotFoundError(f"dbt source file not found: {source_file}")

    config = yaml.safe_load(source_file.read_text(encoding="utf-8")) or {}
    sources = config.get("sources", [])
    if not isinstance(sources, list):
        raise ValueError(f"Invalid sources format in {source_file}.")

    selected = None
    for source in sources:
        if isinstance(source, dict) and source.get("name") == source_name:
            selected = source
            break
    if selected is None:
        raise ValueError(f"Source '{source_name}' not found in {source_file}.")

    table_names: list[str] = []
    for table in selected.get("tables", []):
        if not isinstance(table, dict):
            continue
        name = str(table.get("name") or "").strip()
        if name:
            table_names.append(name)
    if not table_names:
        raise ValueError(
            f"Source '{source_name}' has no tables configured in {source_file}."
        )
    return sorted(set(table_names))


def uc_get_json(base_uri: str, path: str, token: str, timeout_seconds: int = 20) -> dict[str, Any]:
    """Execute a UC API GET request and return the parsed JSON payload."""
    request = urllib.request.Request(
        f"{base_uri}{path}",
        headers={"Accept": "application/json"},
        method="GET",
    )
    bearer = (token or "").strip()
    if bearer and bearer.lower() != "not-used":
        request.add_header("Authorization", f"Bearer {bearer}")

    try:
        with urllib.request.urlopen(request, timeout=timeout_seconds) as response:
            body = response.read().decode("utf-8")
    except urllib.error.HTTPError as exc:
        details = exc.read().decode("utf-8", errors="ignore")
        raise RuntimeError(
            f"UC API request failed ({exc.code}) for {base_uri}{path}: {details}"
        ) from exc
    except urllib.error.URLError as exc:
        raise RuntimeError(f"UC API request failed for {base_uri}{path}: {exc}") from exc

    try:
        parsed = json.loads(body)
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"Invalid JSON response from UC API for {base_uri}{path}.") from exc
    if not isinstance(parsed, dict):
        raise RuntimeError(
            f"Unexpected UC payload type for {base_uri}{path}: {type(parsed).__name__}."
        )
    return parsed


def uc_table_path(catalog: str, schema: str, table: str) -> str:
    """Build the UC API path for a single fully qualified table name."""
    fq_name = f"{catalog}.{schema}.{table}"
    encoded = urllib.parse.quote(fq_name, safe=".")
    return f"/api/2.1/unity-catalog/tables/{encoded}"


def build_source_mapping_rows(
    *,
    base_uri: str,
    token: str,
    catalog: str,
    schema: str,
    source_name: str,
    table_names: list[str],
    require_external: bool = True,
    require_delta: bool = True,
) -> list[dict[str, str]]:
    """Fetch UC metadata for required tables and build mapping rows."""
    generated_at = utc_now_iso()
    rows: list[dict[str, str]] = []
    failures: list[str] = []

    for table in table_names:
        payload = uc_get_json(
            base_uri=base_uri,
            path=uc_table_path(catalog, schema, table),
            token=token,
        )
        table_type = str(payload.get("table_type") or "").strip().upper()
        data_source_format = str(payload.get("data_source_format") or "").strip().upper()
        storage_location = str(payload.get("storage_location") or "").strip()

        if not storage_location:
            failures.append(
                f"{catalog}.{schema}.{table}: storage_location is missing in UC metadata."
            )
            continue
        if require_external and table_type and table_type != "EXTERNAL":
            failures.append(
                f"{catalog}.{schema}.{table}: expected table_type=EXTERNAL, got {table_type!r}."
            )
            continue
        if require_delta and data_source_format and data_source_format != "DELTA":
            failures.append(
                f"{catalog}.{schema}.{table}: expected data_source_format=DELTA, got {data_source_format!r}."
            )
            continue

        rows.append(
            {
                "source_name": source_name,
                "table_name": table,
                "uc_catalog": catalog,
                "uc_schema": schema,
                "table_type": table_type,
                "data_source_format": data_source_format,
                "storage_location": storage_location,
                "generated_at_utc": generated_at,
                "uc_api_uri": base_uri,
            }
        )

    if failures:
        joined = "\n- ".join(failures)
        raise RuntimeError(f"Failed to build UC bronze source mapping:\n- {joined}")

    return sorted(rows, key=lambda item: item["table_name"])


def write_source_mapping(rows: list[dict[str, str]], output_path: Path) -> None:
    """Persist mapping rows as a JSON array artifact."""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(rows, indent=2) + "\n", encoding="utf-8")


def load_source_mapping(path: Path) -> list[dict[str, Any]]:
    """Load and validate a persisted source mapping artifact."""
    if not path.exists():
        raise FileNotFoundError(f"Source mapping file not found: {path}")
    parsed = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(parsed, list):
        raise ValueError(f"Invalid mapping payload in {path}: expected JSON array.")
    return parsed


def validate_source_mapping(
    *,
    mapping_rows: list[dict[str, Any]],
    required_tables: list[str],
    source_name: str,
    max_age_hours: float,
) -> tuple[dict[str, dict[str, Any]], list[str]]:
    """Validate mapping structure, coverage, and freshness."""
    failures: list[str] = []
    relevant_rows: dict[str, dict[str, Any]] = {}
    generated_times: list[datetime] = []

    for row in mapping_rows:
        if not isinstance(row, dict):
            failures.append("Encountered non-object row in mapping artifact.")
            continue
        if str(row.get("source_name") or "") != source_name:
            continue

        table_name = str(row.get("table_name") or "").strip()
        storage_location = str(row.get("storage_location") or "").strip()
        generated_at_raw = str(row.get("generated_at_utc") or "").strip()

        if not table_name:
            failures.append("Row has empty table_name.")
            continue
        if table_name in relevant_rows:
            failures.append(f"Duplicate mapping row for table '{table_name}'.")
            continue
        if not storage_location:
            failures.append(f"Table '{table_name}' has empty storage_location.")
            continue
        if not generated_at_raw:
            failures.append(f"Table '{table_name}' has empty generated_at_utc.")
            continue

        try:
            generated_at = parse_iso_utc(generated_at_raw)
        except ValueError as exc:
            failures.append(f"Table '{table_name}' has invalid generated_at_utc: {exc}")
            continue

        generated_times.append(generated_at)
        relevant_rows[table_name] = row

    missing = [table for table in required_tables if table not in relevant_rows]
    if missing:
        failures.append("Missing mapping rows for required tables: " + ", ".join(sorted(missing)))

    if generated_times:
        oldest = min(generated_times)
        age = datetime.now(timezone.utc) - oldest
        if age > timedelta(hours=max_age_hours):
            failures.append(
                "Source mapping is stale: oldest generated_at_utc="
                f"{oldest.isoformat()} exceeds max_age_hours={max_age_hours}."
            )
    else:
        failures.append(
            f"No mapping rows found for source '{source_name}'."
        )

    return relevant_rows, failures
