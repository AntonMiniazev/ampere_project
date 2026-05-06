# Project Tools

Reusable project scripts live here.

- `docs/` regenerates external dataflow documentation from tracked contracts.
- `py_utils/` contains shared local-only helpers for scripts and notebooks.
- `uc/` contains canonical metadata contracts, Unity Catalog registration, generated DAG config tooling, and data contract extraction utilities.
- `uc_catalog_rebuild_and_docs.ipynb` runs the full local UC catalog rebuild and documentation refresh sequence.

Local checks, one-off analysis, and logs belong in `_internal/`.
