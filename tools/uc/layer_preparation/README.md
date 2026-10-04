# Unity Catalog Layer Preparation

This folder contains local-only notebooks for preparing Unity Catalog metadata from the canonical JSON contract.

Source of truth:
- `tools/uc/contracts/ampere_tables.json`

UC `ColumnInfo.type_json` must contain a complete Spark field JSON object
(`name`, `type`, `nullable`, `metadata`). The `bronze.assortment` contract entry
uses this format as the UC 0.6.0 pilot; the other existing contract entries still
need conversion before a full layer metadata refresh. The pilot changed only its
UC metadata, not its external Delta storage path.

Local runtime config:
- `tools/uc/config/uc_runtime_config.json`

Execution outputs:
- `tools/uc/reports/uc_<layer>_input.json`
- `tools/uc/reports/uc_<layer>_report.json`

Layer creation notebook:
- `create_uc_layer.ipynb` prepares the selected `bronze`, `silver`, or `gold` layer from the canonical contract.
- The first code cell contains the layer selection and local execution options.
- Reports are written under `tools/uc/reports/`.

Layer inspection notebook:
- `check_uc_layer.ipynb` lists current UC metadata for the selected layer.
- The first code cell contains the layer selection.

The notebooks contain their workflow steps directly and use shared local helpers
from `tools/py_utils/` for paths, `.env`, UC config, contract loading, and UC API
operations.
