# Source bootstrap

This subfolder contains the `init_source_preparation` package and its container
image. The bootstrap creates 16 PostgreSQL source tables, loads seed dictionaries,
generates starter data, and publishes a typed baseline to MinIO. The legacy
dataset uses `source` and `pipeline`. The generator v2 dataset uses
`ampere_source`, `ampere_pipeline`, and a dedicated MinIO staging bucket.

| Component | Responsibility |
| --- | --- |
| [model/__main__.py](model/__main__.py), [model/logging_utils.py](model/logging_utils.py) | Package entry point and logging. |
| [model/config.py](model/config.py), [model/bootstrap_defaults.yaml](model/bootstrap_defaults.yaml) | Validated seed policy and its defaults. |
| [model/settings.py](model/settings.py) | Validated runtime settings and target selection. |
| [model/data_source_initialization.py](model/data_source_initialization.py) | Orchestrates schema reset, seeding, baseline staging, and retry recovery. |
| [model/ddl_init.py](model/ddl_init.py), [model/db.py](model/db.py) | Source table and index definitions, PostgreSQL connections, and bulk loading. |
| [model/pre_raw_contracts.yaml](model/pre_raw_contracts.yaml) | Reference descriptions of pre-Raw tables and columns. |
| [model/dictionaries/](model/dictionaries/), [model/generators/](model/generators/) | Packaged CSV dictionaries and generators for clients, assortment, and delivery resources. |
| [model/source_batches.py](model/source_batches.py) | Source row identities, dataset and batch metadata, and baseline staging through the shared [staging writer](../../tools/source_batch_staging.py). |
| [pyproject.toml](pyproject.toml), [uv.lock](uv.lock), [Dockerfile](Dockerfile) | Package metadata, locked dependencies, and container image definition. |
