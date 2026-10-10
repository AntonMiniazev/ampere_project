# Spark Connect client image

This small Python image runs the contract-driven catalog initializer and weekly
Iceberg housekeeping client from `app/`. Both submit SQL through the persistent
Spark Connect service; they do not start a local Spark driver or need a JVM,
Spark distribution, or Iceberg JARs.

The image pins `pyspark-client` to Spark 4.1.0 and NumPy 2.3.4 for the homelab
CPU baseline. It includes only the two client scripts and contract helpers.
Raw extraction and Bronze application continue to use the full
`ampere-spark-iceberg` image.

Airflow resolves this image from `ampere_release_version`. The optional
`iceberg_spark_connect_client_image` Airflow Variable can override that tag and
must reference `ghcr.io/antonminiazev/ampere-spark-connect-client`.
