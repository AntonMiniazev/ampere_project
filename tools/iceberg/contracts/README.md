# Bronze Iceberg schema contract

`ampere_tables.json` defines the Bronze table names, column order, Spark SQL types, and partition keys used by `iceberg_bronze.catalog.ensure_iceberg_table`. The Spark image copies this file to `/opt/spark/app/bronze_contract.json`.

The operational `bronze_apply_registry` column schema is maintained in `docker/spark/iceberg_raw_etl/app/iceberg_bronze/bronze_apply_registry_schema.json`. Change both the table contract and affected dbt models when a business column changes, then run the Bronze contract check before building the Spark image.
