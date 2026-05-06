# Data Contract Extraction

This folder contains the local notebook used to extract compact public data contracts from live Unity Catalog state.

The notebook contains its workflow steps directly and uses shared local helpers
from `tools/py_utils/` for paths, `.env`, UC config, contract loading, and UC API
operations.

Output:
- `docs/data_contracts/bronze.json`
- `docs/data_contracts/silver.json`
- `docs/data_contracts/gold.json`

The generated contracts are consumed by the documentation workflow.
