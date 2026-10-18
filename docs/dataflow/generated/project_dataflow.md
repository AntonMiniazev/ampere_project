# Project dataflow

This flowchart shows the high-level layer transitions, the main logic inside each layer, and the engine used between layers.

```mermaid
flowchart LR
    L_SOURCE["Python generator + PostgreSQL<br/>Logic: PostgreSQL owns current state and compact batch controls; MinIO holds typed, replayable event files and commit manifests.<br/>Python, PostgreSQL<br/>PostgreSQL"]
    L_RAW["Raw Iceberg change history<br/>Logic: Typed event files and commit markers, ordered by dataset batch and change sequence.<br/>Spark<br/>MinIO, Iceberg<br/>Lakekeeper"]
    L_BRONZE["Bronze<br/>Logic: Resume the accepted transfer across both application groups, reduce changes in source order, apply stable-key inserts and updates, and delete source rows without stale replay.<br/>Spark, Iceberg<br/>MinIO, Iceberg<br/>Lakekeeper"]
    L_SILVER["Silver<br/>Logic: Preserve full history while applying only changed orders; unchanged replays skip Iceberg updates.<br/>DuckDB, dbt, Iceberg<br/>MinIO, Iceberg<br/>Lakekeeper"]
    L_GOLD["Gold<br/>Logic: Replace only months recorded by successful Silver publications and preserve untouched months.<br/>DuckDB, dbt, Iceberg<br/>MinIO, Iceberg<br/>Lakekeeper"]
    L_SERVING["Serving / BI<br/>Logic: Curie refreshes its Iceberg cache after a successful Gold publish.<br/>Curie<br/>Cache"]

    L_SOURCE -->|"Spark"| L_RAW
    L_RAW -->|"Spark + Iceberg"| L_BRONZE
    L_BRONZE -->|"DuckDB + dbt"| L_SILVER
    L_SILVER -->|"DuckDB + dbt"| L_GOLD
    L_GOLD -->|"Curie cache refresh"| L_SERVING
```
