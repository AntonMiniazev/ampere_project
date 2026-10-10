# Spark Iceberg runtime

This Spark image runs PostgreSQL-to-Raw extraction and Raw-to-Bronze daily and
full-history applications. It includes `tools/contracts/ampere_tables.json`
and the shared v3 resolver from the same repository revision as its application
code. Catalog initialization and housekeeping use the separate
`ampere-spark-connect-client` image.

Bronze jobs validate the table schema, partition specification, and relevant
properties before applying data. They do not create tables. The Bronze DAG
passes the configured executor node selector to the SparkApplication template;
`spark_executor_node_selector` defaults to `ampere-k8s-node4`.

The daily Bronze DAG selects validated Raw batches using the registry and
configured lookbacks. The full-rebuild DAG scans every validated Raw manifest
and replays it into the freshly initialized tables. Snapshot partitions,
mutable dimensions, facts, and events keep their existing write semantics.
Raw extraction partition keys remain separate from Iceberg physical
partitioning.

The image bundles Spark 4.1 and Iceberg runtime JARs for Raw and Bronze
SparkApplications. Its Python environment includes classic PySpark and boto3
for S3 manifest fallback reads; it does not include Spark Connect client
dependencies. Lakekeeper access uses the Spark Entra client and MinIO
credentials supplied by Kubernetes Secrets.
