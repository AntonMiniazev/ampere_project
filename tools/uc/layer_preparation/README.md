# Unity Catalog Layer Preparation

This folder contains local-only notebooks for preparing Unity Catalog metadata from the canonical JSON contract.

Source of truth:
- `tools/uc/contracts/ampere_tables.json`

Local runtime config:
- `tools/uc/config/uc_runtime_config.json`

Execution outputs:
- `tools/uc/reports/uc_<layer>_input.json`
- `tools/uc/reports/uc_<layer>_report.json`

Notebook workflow:
1. Open `create_uc_layer.ipynb`.
2. In the first code cell, set `LAYER` to `bronze`, `silver`, or `gold`.
3. Run all cells.
4. Review the report under `tools/uc/reports/`.

Inspection workflow:
1. Open `check_uc_layer.ipynb`.
2. In the first code cell, set `LAYER`.
3. Run all cells to list current UC metadata for the selected layer.

The notebooks contain their workflow steps directly and use shared local helpers
from `tools/py_utils/` for paths, `.env`, UC config, contract loading, and UC API
operations.
