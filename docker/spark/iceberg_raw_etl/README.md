# Spark Raw and Bronze runtime

This image contains one Spark 4.1 application for PostgreSQL journal-to-Raw
transfer and one for accepted Raw-to-Bronze application. It bundles the
contract, PostgreSQL JDBC driver, Iceberg runtime, S3A libraries, and Psycopg
for the small PostgreSQL control transactions. Driver and executors use the
same MinIO and Lakekeeper credentials from Kubernetes Secrets.

## PostgreSQL journal to Raw

The source initializer creates a dataset ID, a baseline batch, a typed
`source_changes.<table>` table for each source table, and row triggers. Daily
generation is one transaction per business date. The trigger rejects untagged
source writes; the controlled SQL entrypoints are
`pipeline.begin_manual_source_batch(...)` and
`pipeline.complete_manual_source_batch()` within the same transaction. A
deferred constraint prevents a started source batch from committing.

`journal_to_raw.py` freezes up to 32 completed batches, including the baseline,
and records a deterministic transfer ID from dataset, sequence interval, and
contract version. One Spark application processes tables sequentially with one
JDBC reader at a time. Per-table counts come from the transactional source
control records and are checked against the bounded JDBC transfer. Raw tables
use change identity `(dataset_id, batch_id, change_id)`. The Iceberg MERGE
skips already committed keys after a crash; PostgreSQL marks the table complete
only after the Iceberg commit and records its snapshot ID. A transfer becomes
visible to Bronze only after all Raw tables, including zero-change tables, are
acknowledged.

## Raw to Bronze

`iceberg_bronze.journal_apply` consumes the next complete Raw transfer. It
reduces each changed source row to the last operation by batch sequence and
change ID, then matches Bronze on `(dataset_id, source_row_id)`. Version guards
reject stale updates and deletes. A delete removes a matching row and never
inserts a tombstone as active data. Per-table PostgreSQL acknowledgments make a
retry after an Iceberg commit idempotent. The full-rebuild DAG reduces all
accepted Raw history at one frozen boundary and replaces each Bronze table in
one Iceberg commit.

Bronze's v4 schema adds dataset ID, stable source-row ID, source batch sequence,
and source change ID to its prior business and operational columns. Catalog init
adds only these four nullable columns in place on existing Bronze tables,
preserving existing rows and Iceberg table identities. Other contract conflicts
remain validation errors.

## Placement and rollout

All generator, Raw, and Bronze compute is pinned to
`ampere-k8s-node4`. Current defaults request 3 CPUs and 4 GiB for generation;
Raw runs one application with at most two executors; Bronze uses at most three.
Raw JDBC extraction has no partition-column fan-out. The Raw and Bronze DAGs
share the one-slot `iceberg_pipeline_mutation` Airflow pool.

This implementation is prepared locally and has not been released or deployed.
Deployment requires the Lakekeeper `raw` warehouse backed by MinIO storage,
contract v4 catalog tables, the existing PostgreSQL/MinIO/Lakekeeper secrets,
and the Airflow mutation pool. The init DAG defaults to a destructive source
reset; `source_init_mode=journal_migrate` installs the journal on existing rows
without truncation, using them as one baseline if the dataset is new. New
generation starts after the latest existing order date. See
`.local/current_steps.md` for the cutover sequence.
