# PostgreSQL source generation backfill

For a January 2026 dataset, initialize the PostgreSQL source with this explicit reset configuration first:

```json
{
  "source_init_mode": "reset",
  "project_start_date": "2026-01-01"
}
```

The init DAG recreates the Ampere-owned `source` and `pipeline` schemas, seeds dictionaries and initial clients/resources, and stages a typed baseline (`B`) for all 16 source tables in MinIO. PostgreSQL retains only compact dataset, batch, and per-table count records; it does not create a row-event journal. A batch's Parquet files and manifest are uploaded before the database transaction commits, and `COMMITTED.json` is published only after commit. Raw requires both that marker and a completed PostgreSQL batch record. A retry of the same Airflow init run repairs a missing marker from the committed baseline. The date anchors initial client registrations; init does not generate orders. Explicitly request `source_init_mode=reset` only when replacing the Ampere source dataset is intended. After init succeeds, confirm `PROJECT_START_DATE` is `2026-01-01`, then run the backfill below.

The manual Airflow DAG `ampere__pre_raw__generators__backfill` runs the generator for an inclusive date range. It starts one Kubernetes pod on `ampere-k8s-node4` and generates days sequentially, keeping only the active day's working frames in memory. PostgreSQL source changes and compact batch metadata commit in one transaction per calendar month. Each day's exact inserts, update after-images, and delete images are streamed to bounded typed Parquet chunks as mutations occur. After a month commits, its batch markers become visible to Raw. Daily generation uses the same capture path in one transaction per day.

Trigger `ampere__pre_raw__generators__backfill` with:

```json
{
  "start_date": "2026-01-01",
  "end_date": "2026-01-31",
  "seed": 42
}
```

Both dates are inclusive; there is no day-count cap. Backfill remains sequential, with one committed calendar month at a time. If a month fails, that month's source and batch-control changes roll back together; previously committed months remain. Uploaded but uncommitted Parquet objects are ineligible orphans. Retry with the same date range and seed so completed dates are skipped and their markers are repaired if needed, while the failed month is regenerated. The Airflow DAG and task each have an eight-hour timeout. For an existing source with generated orders, begin after its order-date watermark:

```sql
SELECT MAX(order_date) AS completed_through FROM source.orders;
```

If `source.orders` is empty after initialization, the first generation date must not precede the latest client registration date. With the January 1 init anchor above, January 1 is valid. Use the same range and seed when retrying a failed run. Already generated dates are skipped by the backfill.

The backfill DAG writes PostgreSQL current state and MinIO staging files. It does not initialize or truncate source tables and does not trigger Raw or Bronze. Within each month transaction, each day's completed batch metadata remains distinct, so Raw can order changes by business date and batch sequence after the month commits. Raw validates every referenced file's size and checksum metadata before reading it; after successful Raw and all 16 per-table acknowledgments, staged Parquet payloads are eligible for cleanup after the configured 7–14 day grace period. Orphan cleanup is separate.

After a successful run, check the generated date coverage with:

```sql
SELECT order_date, COUNT(*) AS orders
FROM source.orders
WHERE order_date BETWEEN DATE '2026-01-01' AND DATE '2026-01-31'
GROUP BY order_date
ORDER BY order_date;
```

Expect 31 dates with rows. Airflow task logs report how many requested dates were generated and how many were already complete.
