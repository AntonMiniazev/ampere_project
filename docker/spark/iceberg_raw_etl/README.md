# Spark Raw and Bronze runtime

This image contains the Spark applications for MinIO staged-Parquet-to-Raw transfer and accepted Raw-to-Bronze application. It bundles the canonical contract, Iceberg runtime, S3A libraries, and Psycopg for compact PostgreSQL control and acknowledgment transactions. Drivers and executors use existing MinIO and Lakekeeper credentials from Kubernetes Secrets.

## MinIO staged batches to Raw

The source initializer creates a dataset ID, compact batch controls, and a typed baseline in immutable MinIO Parquet. Generator mutation helpers capture exact inserted, updated, and deleted row images at mutation time; PostgreSQL does not retain a row-event journal. Daily generation commits one business date, while historical backfill commits calendar months and retains separate daily batch records. Data files and a manifest are uploaded before PostgreSQL commit; `COMMITTED.json` is published only after commit. Raw requires both that marker and completed PostgreSQL control rows, so uncommitted uploads are not eligible.

`journal_to_raw.py` validates completed manifests and referenced object sizes/checksum metadata in ascending sequence, then reads Parquet in bounded file and byte groups. It records a deterministic transfer ID from dataset, sequence interval, and contract version. `--max-batches` limits the total logical batches handled by one Spark application (default 32); `--batch-size` limits each sequential transfer group (maximum 96). A historical range can process multiple groups in one DAG run without collecting event rows on the driver. Daily triggers keep the default total of 32.

Per-table counts come from transactional source control records and are checked against staged manifests and Spark row counts. Raw tables use change identity `(dataset_id, batch_id, change_id)`. Iceberg MERGE skips already committed keys after a crash; PostgreSQL marks each table complete only after its Iceberg commit. A transfer becomes visible to Bronze only after all 16 tables, including zero-change tables, are acknowledged. Staged Parquet payloads are eligible for cleanup only after the Raw transfer and every table acknowledgment have completed and the 7–14 day retention period has elapsed. Orphan cleanup is separate.

## Raw to Bronze

`iceberg_bronze.journal_apply` consumes complete Raw transfers. Incremental application processes source batches in order and uses version-guarded upserts and deletes. Full rebuild freezes the dataset and Raw high-water boundary, selects rows only from acknowledged Raw transfers, and reduces each mutable table to its latest operation over the complete history ordered by batch sequence and change ID. Deletes are filtered only after latest-state selection. `order_product` is insert-only in the generator and has a unique source-row ID, so it skips the unnecessary deduplication window. Rebuild uses stable hash buckets and a per-table shuffle count capped at 256 (about 100,000 raw rows per partition), writes prepared rows to a unique temporary Parquet path, then publishes each table with one Iceberg overwrite commit. A retry redoes any unacknowledged table from the frozen boundary; the table acknowledgment is recorded only after its overwrite succeeds. The two sequential Spark stages remain snapshots/mutable dimensions, then facts/events, each capped at two executors with the existing resource profiles. The final checkpoint is written only after all 16 tables and both stages complete.

Bronze lineage maps Raw `_batch_sequence` and `_change_id` to `_source_batch_sequence` and `_source_change_id`. A new deterministic rebuild identity prevents acknowledgments from the prior merge-based algorithm from skipping tables. Timestamped Python INFO logs report table progress; Spark logs are set to WARN.

## Placement and rollout

Generator, Raw, and Bronze compute remains pinned to `ampere-k8s-node4`. Existing resource requests and limits are unchanged. The Raw app retains its configured executor cap; Bronze keeps its two sequential application profiles. Raw and Bronze use the one-slot Airflow pool `iceberg_pipeline_mutation`.

The current contract defines 16 Raw, 17 Bronze, 17 Silver, and 8 Gold tables. Catalog initialization creates or validates contract tables. The manual `ampere__iceberg__bronze__catalog_reset` DAG drops and recreates only Bronze tables from the contract; it uses Lakekeeper's managed soft-delete behavior, so physical file cleanup follows warehouse retention. See `.local/current_steps.md` for the active release and full-rebuild sequence.
