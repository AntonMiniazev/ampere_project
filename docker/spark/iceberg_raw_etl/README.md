# Spark Raw and Bronze runtime

This image contains one Spark 4.1 application for PostgreSQL journal-to-Raw
transfer and one for accepted Raw-to-Bronze application. It bundles the
contract, PostgreSQL JDBC driver, Iceberg runtime, S3A libraries, and Psycopg
for the small PostgreSQL control transactions. Driver and executors use the
same MinIO and Lakekeeper credentials from Kubernetes Secrets.

## PostgreSQL journal to Raw

The source initializer creates a dataset ID, a baseline batch, a typed
`source_changes.<table>` table for each source table, and statement-level
transition-table triggers. Daily generation uses one transaction per business
date; historical backfill commits one calendar month at a time. Each trigger
captures all affected rows set-wise and rejects untagged source writes. The
controlled SQL entrypoints are
`pipeline.begin_manual_source_batch(...)` and
`pipeline.complete_manual_source_batch()` within the same transaction. A
deferred constraint prevents a started source batch from committing.

`journal_to_raw.py` reads completed batches in ascending sequence and records a
deterministic transfer ID from dataset, sequence interval, and contract
version. `--max-batches` limits the total batches handled by one Spark
application (default 32); `--batch-size` limits each sequential transfer group
(default and maximum 96). A historical run can therefore process multiple
groups of at most 96 batches in one Airflow run. For the January-October 2026
range, baseline plus 304 daily batches is 305 total; use
`{"max_batches":305,"batch_size":96}` to process four bounded groups. Daily
triggers keep the default total of 32 and normally handle only the new daily
batch.

The PostgreSQL transfer ledger records completed batch sequences. An index on
`(dataset_id, batch_sequence)` lets each run read the high-water mark directly,
then select only later batches; it does not join or rescan the full batch
history. One Spark application processes tables sequentially with one JDBC
reader at a time. Per-table counts come from transactional source control
records and are checked against the bounded JDBC transfer. Raw tables use
change identity `(dataset_id, batch_id, change_id)`. The Iceberg MERGE skips
already committed keys after a crash; PostgreSQL marks each table complete
only after its Iceberg commit and records the snapshot ID. A transfer becomes
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
one Iceberg commit. Bronze lineage maps Raw `_batch_sequence` and `_change_id`
to `_source_batch_sequence` and `_source_change_id`; these values are required
for checkpoint-bounded reads and version-guarded updates. After deploying a
writer that corrects lineage values, run the full-rebuild DAG once to replace
rows written with null lineage; a new deterministic rebuild identity ensures
old per-table acknowledgments do not skip the repair, while retries of the
corrected rebuild remain idempotent. The daily version guard cannot repair
those rows. The journal writer configures timestamped Python INFO logs and
Spark WARN logs, with per-table start, source-row validation, completion,
failure, retry-skip, and elapsed-time messages in the Spark driver output.

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
contract v5 catalog tables, compact PostgreSQL source-change metadata, and the existing MinIO/Lakekeeper secrets,
and the Airflow mutation pool. The init DAG defaults to a destructive source
reset; `source_init_mode=journal_migrate` installs the journal on existing rows
without truncation, using them as one baseline if the dataset is new. New
generation starts after the latest existing order date. See
`.local/current_steps.md` for the cutover sequence.
