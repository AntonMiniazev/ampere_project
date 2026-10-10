# Spark Iceberg runtime

This Spark image runs PostgreSQL-to-Raw extraction and Raw-to-Bronze daily and
full-history application. Catalog initialization and housekeeping use the
separate lightweight Spark Connect client image. This image includes
`tools/contracts/ampere_tables.json` and the shared v3 resolver from the same
repository revision as its application code.

`ampere__iceberg__catalog__init` creates the Bronze, Silver, and Gold
namespaces and all 42 empty tables from contract v3. It is idempotent for
matching tables and fails when an existing table conflicts. Bronze jobs
validate the table schema, partition specification, and relevant properties
before applying data. They do not create tables.

The daily Bronze DAG selects validated Raw batches using the registry and
configured lookbacks. The full-rebuild DAG scans every validated Raw manifest
and replays it into the freshly initialized tables. Snapshot partitions,
mutable dimensions, facts, and events keep their existing write semantics.
Raw extraction partition keys remain separate from Iceberg physical
partitioning.

Housekeeping derives its table list, retention, compaction, delete-file, and
manifest policies from contract v3. It checks conformance before maintenance,
skips compaction until the contract's minimum active-file threshold is met,
preserves table rows, expires snapshots, and removes aged orphan files. A dry
run previews cleanup without changing table data or metadata.

The image bundles Spark 4.1 and Iceberg runtime JARs for Raw and Bronze
SparkApplications. Lakekeeper access uses the Spark Entra client and MinIO
credentials supplied by Kubernetes Secrets.
