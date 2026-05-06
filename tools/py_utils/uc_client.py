"""Unity Catalog API client for operations on UC schemas and tables."""

from __future__ import annotations

import json
import shlex
from dataclasses import dataclass
from typing import Any

from .ssh_connection import SSHConnection, SSHConfig


@dataclass
class UCConfig:
    """Unity Catalog configuration.

    Attributes:
        catalog: Catalog name (e.g., 'ampere')
        namespace: Kubernetes namespace (e.g., 'unity-catalog')
        service_name: UC service name (e.g., 'unity-catalog-unitycatalog-server')
        service_port: UC service port (default: 8080)
        ssh_config: SSH configuration for reaching cluster
    """

    catalog: str
    namespace: str
    service_name: str
    service_port: int
    ssh_config: SSHConfig


class UCClient:
    """UC API client for managing schemas and tables."""

    def __init__(self, config: UCConfig) -> None:
        """Initialize UC client.

        Args:
            config: UC configuration
        """
        self.config = config
        self.ssh = SSHConnection(config.ssh_config)
        self.base_url = self._resolve_base_url()

    def _resolve_base_url(self) -> str:
        """Resolve UC service endpoint IP.

        Prefers direct endpoint pod IP to avoid occasional ClusterIP routing issues.
        Falls back to ClusterIP if endpoint not available.

        Returns:
            Base URL for UC API

        Raises:
            RuntimeError: If UC endpoint cannot be resolved
        """
        # Try to get endpoint pod IP first
        ep_cmd = (
            f"kubectl -n {shlex.quote(self.config.namespace)} "
            f"get endpoints {shlex.quote(self.config.service_name)} "
            "--no-headers | awk '{print $2}' | cut -d: -f1"
        )
        try:
            endpoint_ip = self.ssh.run(ep_cmd).strip()
            if endpoint_ip:
                return f"http://{endpoint_ip}:{self.config.service_port}"
        except Exception:  # noqa: BLE001
            pass

        # Fall back to ClusterIP
        svc_cmd = (
            f"kubectl -n {shlex.quote(self.config.namespace)} "
            f"get svc {shlex.quote(self.config.service_name)} --no-headers "
            "| awk '{print $3}'"
        )
        cluster_ip = self.ssh.run(svc_cmd).strip()
        if not cluster_ip:
            raise RuntimeError("Could not resolve UC endpoint IP or service ClusterIP.")
        return f"http://{cluster_ip}:{self.config.service_port}"

    def _uc_request(
        self,
        method: str,
        path: str,
        query: dict[str, Any] | None = None,
        payload: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Execute UC API request via remote curl.

        Args:
            method: HTTP method (GET, POST, etc.)
            path: API path (e.g., '/api/2.1/unity-catalog/schemas')
            query: Optional query parameters
            payload: Optional JSON request body

        Returns:
            JSON response as dictionary

        Raises:
            RuntimeError: If request fails or returns error
        """
        url = f"{self.base_url}{path}"
        cmd = f"curl -sS --connect-timeout 10 --max-time 60 -X {method} {shlex.quote(url)}"
        if query:
            cmd += " -G"
            for key, value in query.items():
                cmd += f" --data-urlencode {shlex.quote(f'{key}={value}')}"

        if payload is not None:
            payload_json = json.dumps(payload, separators=(",", ":"))
            cmd = (
                cmd
                + " -H 'Content-Type: application/json' --data-raw "
                + shlex.quote(payload_json)
            )

        raw = self.ssh.run(cmd).strip()
        if not raw:
            return {}
        try:
            data = json.loads(raw)
            if isinstance(data, dict) and data.get("error_code"):
                raise RuntimeError(
                    f"UC API error {data.get('error_code')}: {data.get('message', data)}"
                )
            return data
        except json.JSONDecodeError as exc:
            if method.upper() == "DELETE" and raw.upper() in {"200 OK", "OK"}:
                return {"status": raw}
            raise RuntimeError(
                f"Non-JSON response from UC API at {url}: {raw}"
            ) from exc

    def list_schemas(self) -> list[str]:
        """List all schemas in catalog.

        Returns:
            List of schema names
        """
        resp = self._uc_request(
            "GET",
            "/api/2.1/unity-catalog/schemas",
            query={"catalog_name": self.config.catalog},
        )
        return [item["name"] for item in resp.get("schemas", [])]

    def create_schema(
        self, schema_name: str, comment: str | None = None
    ) -> dict[str, Any]:
        """Create schema in catalog.

        Args:
            schema_name: Name of schema to create
            comment: Optional schema description

        Returns:
            API response
        """
        payload = {"name": schema_name, "catalog_name": self.config.catalog}
        if comment:
            payload["comment"] = comment
        return self._uc_request(
            "POST",
            "/api/2.1/unity-catalog/schemas",
            payload=payload,
        )

    def schema_exists(self, schema_name: str) -> bool:
        """Check if schema exists in catalog.

        Args:
            schema_name: Name of schema to check

        Returns:
            True if schema exists, False otherwise
        """
        return schema_name in self.list_schemas()

    def list_tables(self, schema_name: str) -> list[dict[str, Any]]:
        """List all tables in schema.

        Args:
            schema_name: Schema name

        Returns:
            List of table metadata dictionaries
        """
        resp = self._uc_request(
            "GET",
            "/api/2.1/unity-catalog/tables",
            query={"catalog_name": self.config.catalog, "schema_name": schema_name},
        )
        return resp.get("tables", [])

    def get_table(self, schema_name: str, table_name: str) -> dict[str, Any]:
        """Get table metadata.

        Args:
            schema_name: Schema name
            table_name: Table name

        Returns:
            Table metadata dictionary
        """
        return self._uc_request(
            "GET",
            f"/api/2.1/unity-catalog/tables/{self.config.catalog}.{schema_name}.{table_name}",
        )

    def create_table(
        self,
        schema_name: str,
        table_name: str,
        storage_location: str,
        columns: list[dict[str, Any]],
        comment: str | None = None,
        data_source_format: str = "DELTA",
        properties: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Create external table in UC.

        Args:
            schema_name: Schema name
            table_name: Table name
            storage_location: Full path to table data (s3://bucket/path)
            columns: List of column definitions with name, type, nullable, comment
            comment: Optional table description
            data_source_format: UC data source format, such as DELTA or PARQUET

        Returns:
            API response
        """
        payload = {
            "name": table_name,
            "catalog_name": self.config.catalog,
            "schema_name": schema_name,
            "table_type": "EXTERNAL",
            "data_source_format": data_source_format,
            "storage_location": storage_location,
            "columns": columns,
        }
        if comment:
            payload["comment"] = comment
        if properties:
            payload["properties"] = properties
        return self._uc_request(
            "POST",
            "/api/2.1/unity-catalog/tables",
            payload=payload,
        )

    def list_volumes(self, schema_name: str) -> list[dict[str, Any]]:
        """List all volumes in schema.

        Args:
            schema_name: Schema name

        Returns:
            List of volume metadata dictionaries
        """
        resp = self._uc_request(
            "GET",
            "/api/2.1/unity-catalog/volumes",
            query={"catalog_name": self.config.catalog, "schema_name": schema_name},
        )
        return resp.get("volumes", [])

    def list_catalogs(self) -> list[str]:
        """List all catalogs.

        Returns:
            List of catalog names
        """
        resp = self._uc_request("GET", "/api/2.1/unity-catalog/catalogs")
        return [item["name"] for item in resp.get("catalogs", [])]

    def create_catalog(self, comment: str | None = None) -> dict[str, Any]:
        """Create the configured catalog in Unity Catalog."""
        payload = {"name": self.config.catalog}
        if comment:
            payload["comment"] = comment
        return self._uc_request(
            "POST",
            "/api/2.1/unity-catalog/catalogs",
            payload=payload,
        )

    def delete_table(self, schema_name: str, table_name: str) -> dict[str, Any]:
        """Delete a UC table metadata entry without deleting object storage data."""
        return self._uc_request(
            "DELETE",
            f"/api/2.1/unity-catalog/tables/{self.config.catalog}.{schema_name}.{table_name}",
        )
