"""Project path helpers for local tools and notebooks."""

from __future__ import annotations

from pathlib import Path


def project_root(start: str | Path | None = None) -> Path:
    """Find the repository root from a script, notebook, or current directory."""
    base = Path(start) if start is not None else Path.cwd()
    resolved = base.resolve()
    if resolved.is_file():
        resolved = resolved.parent
    for candidate in [resolved, *resolved.parents]:
        if (candidate / "pyproject.toml").exists() or (candidate / ".git").exists():
            return candidate
    raise FileNotFoundError(f"Could not find project root from {base}")


def resolve_project_path(path: str | Path, start: str | Path | None = None) -> Path:
    """Resolve a repository-relative path to an absolute path."""
    path_obj = Path(path)
    if path_obj.is_absolute():
        return path_obj
    return project_root(start) / path_obj
