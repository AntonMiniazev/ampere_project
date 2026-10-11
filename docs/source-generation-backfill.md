# PostgreSQL source generation backfill

For a January 2026 dataset, initialize the PostgreSQL source with this Airflow trigger configuration first:

```json
{
  "project_start_date": "2026-01-01"
}
```

The init DAG truncates and repopulates the `source` tables. In the same transaction it retires the prior dataset, clears old rows from the typed `source_changes` journal tables, and writes a new baseline for the reset source data. Small dataset and batch metadata remains for lineage. `source_init_mode=journal_migrate` leaves source rows and the active dataset's journal intact, while clearing journal payloads for inactive dataset IDs. The date anchors the initial client registrations; it does not generate orders. Run init only when resetting the source dataset is intended. After init succeeds, check the init task's rendered fields in Airflow and confirm `PROJECT_START_DATE` is `2026-01-01`; then run the backfill below.

The manual Airflow DAG `ampere__pre_raw__generators__backfill` runs the generator for an inclusive date range. It starts one Kubernetes pod on `ampere-k8s-node4` and generates days sequentially, keeping only the active day's working frames in memory. PostgreSQL source changes and journal rows commit in one transaction per calendar month. Capture triggers run once per SQL write statement and copy the statement's affected rows to the typed journal set-wise; they do not execute Python or PL/pgSQL capture logic once per source row. Daily generation retains one transaction per day.

Trigger `ampere__pre_raw__generators__backfill` with:

```json
{
  "start_date": "2026-01-01",
  "end_date": "2026-01-31",
  "seed": 42
}
```

Both dates are inclusive; there is no day-count cap. Backfill remains sequential, with one committed calendar month at a time. If a month fails, that month's source and journal changes roll back together; previously committed months remain. Retry with the same date range and seed so completed batches are skipped and the failed month is regenerated. The Airflow DAG and task each have an eight-hour timeout. For an existing source with generated orders, begin after its order-date watermark:

```sql
SELECT MAX(order_date) AS completed_through FROM source.orders;
```

If `source.orders` is empty after initialization, the first generation date must not precede the latest client registration date. With the January 1 init anchor above, January 1 is valid. Use the same range and seed when retrying a failed run. Already generated dates are skipped by the backfill.

The backfill DAG only writes PostgreSQL. It does not initialize or truncate source tables and does not trigger Raw or Bronze. Within each month transaction, each day's completed batch metadata remains distinct, so Raw can still order changes by business date and batch sequence after the month commits.

After a successful run, check the generated date coverage with:

```sql
SELECT order_date, COUNT(*) AS orders
FROM source.orders
WHERE order_date BETWEEN DATE '2026-01-01' AND DATE '2026-01-31'
GROUP BY order_date
ORDER BY order_date;
```

Expect 31 dates with rows. Airflow task logs report how many requested dates were generated and how many were already complete.
