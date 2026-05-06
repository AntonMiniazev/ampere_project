# Canonical Contracts

`ampere_tables.json` is the canonical metadata/table-schema contract for local project tooling.

Derived artifacts are generated from this file:
- DAG runtime config under `dags/config/`
- UC layer preparation payloads written as execution reports under `tools/uc/reports/`
- future metadata artifacts that need the same table definitions

The contract shape is:

```text
catalog -> layers -> tables -> columns
```

Bronze stream-group runtime metadata lives on each bronze table and is used to
regenerate `dags/config/table_definitions.json` and
`dags/config/stream_groups.json`.

Regenerate derived artifacts after editing the contract:

```powershell
py -3 tools\uc\generators\generate_from_contract.py
```
