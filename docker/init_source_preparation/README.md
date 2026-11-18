# Source bootstrap

The bootstrap creates the 16 PostgreSQL source tables, seeds dictionaries and starter
data, and publishes a typed baseline to MinIO. Both targets use the same schema
definitions, seed policy, and batch protocol.

| Target setting | PostgreSQL schemas in `ampere_db` | Effective MinIO prefix |
| --- | --- | --- |
| `SCHEMA_INIT=source` (default) | `source`, `pipeline` | `MINIO_STAGING_PREFIX` |
| `SCHEMA_INIT=ampere_source` | `ampere_source`, `ampere_pipeline` | `MINIO_STAGING_PREFIX/ampere_source` |

Both targets require `SOURCE_INIT_MODE=reset`. The legacy path also removes the
obsolete `source_changes` schema. The generator v2 path uses its own advisory
lock and rejects a reset if other database objects depend on its tables.
The active Raw workflow discovers batches through the legacy ledger and reads
objects under `MINIO_STAGING_PREFIX/datasets/`.

## Run locally

From the repository root on the Windows development machine, use Python 3.14
and the bootstrap project's locked environment. The repository root supplies the
shared staging code and contract. Put disposable database and MinIO connection
settings in an ignored file such as `.local/bootstrap.env`, with
`PGHOST`, `PGPORT`, `PGUSER`, `PGPASSWORD`, `MINIO_S3_ENDPOINT`,
`MINIO_ACCESS_KEY`, `MINIO_SECRET_KEY`, and an existing
`MINIO_STAGING_BUCKET`. In VS Code Git Bash, run:

```bash
SCHEMA_INIT=ampere_source SOURCE_INIT_MODE=reset \
  uv run --project docker/init_source_preparation --locked \
  --env-file .local/bootstrap.env python -m init_source_preparation
```

The validated [YAML defaults](model/bootstrap_defaults.yaml) seed 20,000 clients
and 700 delivery resources. Override them with `N_OF_INIT_CLIENTS` and
`N_DELIVERY_RESOURCE`. `PROJECT_START_DATE` defaults to the current date and
accepts a date such as `2026-01-01`. Process environment variables take
precedence over the dotenv file.

`PGDATABASE` defaults to `ampere_db`, `PGHOST` to `postgres-service`, and
`PGPORT` to `5432`. A PostgreSQL `DATABASE_URL` takes precedence over
individual connection settings for the fields it supplies. The legacy
`pguser`, `pgpass`, and `MINIO_ENDPOINT` aliases are supported. MinIO defaults
are bucket `ampere-iceberg-raw`, prefix `ampere/source-batches/v1`, and region
`us-east-1`. `AMPERE_STAGING_CHUNK_ROWS` and
`AMPERE_STAGING_TARGET_FILE_BYTES` configure Parquet chunk size and target file
size. `AMPERE_SOURCE_CODE_VERSION` records provenance in the ledger.

Parquet files and the baseline manifest are prepared before the PostgreSQL
transaction commits. The bootstrap publishes `COMMITTED.json` after commit.
To recover marker publication on a local retry, use the same unique
`AMPERE_AIRFLOW_RUN_ID` on both attempts with the same target and staging
settings. A fresh reset needs a fresh run ID.

## Run in Airflow

The manual DAG `ampere__pre_raw__generators__init` uses the image selected by
`ampere_release_version`. After the refactored image is available, set Airflow
Variable `ampere_bootstrap_schema` to `ampere_source` for generator v2.
The variable is read when the task renders. An unset variable selects
`source`. Supply `{"source_init_mode": "reset"}` in the DAG run configuration.
The run configuration also accepts `project_start_date`. The DAG forwards its
run ID for retry recovery and continues to use the existing Kubernetes Secrets.

Build the same image locally from the repository root:

```bash
docker build -f docker/init_source_preparation/Dockerfile -t init-source-preparation:local .
```
