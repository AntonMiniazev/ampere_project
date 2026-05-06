"""Central `.env` loading for local Ampere tools."""

from __future__ import annotations

import os
from pathlib import Path

from .paths import project_root


def _parse_env_file(env_path: Path) -> dict[str, str]:
    """Parse simple KEY=VALUE lines from a `.env` file."""
    values: dict[str, str] = {}
    if not env_path.exists():
        return values
    for raw_line in env_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        values[key.strip()] = value.strip().strip('"').strip("'")
    return values


def load_project_env(
    start: str | Path | None = None,
    env_file: str = ".env",
    override: bool = False,
) -> dict[str, str]:
    """Load root `.env` values into `os.environ`.

    Args:
        start: Path used to find the repository root.
        env_file: Environment file name, relative to the repository root.
        override: Replace process environment values when true.

    Returns:
        Parsed key/value pairs from the env file.
    """
    env_values = _parse_env_file(project_root(start) / env_file)
    for key, value in env_values.items():
        if override:
            os.environ[key] = value
        else:
            os.environ.setdefault(key, value)
    return env_values


def require_env(name: str, start: str | Path | None = None) -> str:
    """Load project `.env` and return a required environment value."""
    load_project_env(start)
    value = os.environ.get(name)
    if value is None or value == "":
        raise RuntimeError(f"Required environment variable is not set: {name}")
    return value


def bool_env(name: str, default: bool = False, start: str | Path | None = None) -> bool:
    """Load project `.env` and return a boolean environment value."""
    load_project_env(start)
    value = os.environ.get(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "y", "on"}
