# PostgreSQL source generation backfill

For a January 2026 dataset, initialize the PostgreSQL source with this Airflow trigger configuration first:

```json
{
  "project_start_date": "2026-01-01"
}
```

The init DAG truncates and repopulates the `source` tables. The date anchors the initial client registrations; it does not generate orders. Run init only when resetting the source dataset is intended. After init succeeds, check the init task's rendered fields in Airflow and confirm `PROJECT_START_DATE` is `2026-01-01`; then run the backfill below.

The manual Airflow DAG `ampere__pre_raw__generators__backfill` runs the existing generator for an inclusive date range. It starts one Kubernetes pod on `ampere-k8s-node4`; that process generates each day sequentially and commits each day in its own PostgreSQL transaction. A retry skips dates already present in `source.orders` and continues from the next date. Gaps are rejected.

Trigger `ampere__pre_raw__generators__backfill` with:

```json
{
  "start_date": "2026-01-01",
  "end_date": "2026-01-31",
  "seed": 42
}
```

Both dates are inclusive, and the range may contain at most 31 days. For an existing source with generated orders, begin after its order-date watermark:

```sql
SELECT MAX(order_date) AS completed_through FROM source.orders;
```

If `source.orders` is empty after initialization, the first generation date must not precede the latest client registration date. With the January 1 init anchor above, January 1 is valid. Use the same range and seed when retrying a failed run. Already generated dates are skipped by the backfill.

The backfill DAG only writes PostgreSQL. It does not initialize or truncate source tables and does not trigger Raw or Bronze.

After a successful run, check the generated date coverage with:

```sql
SELECT order_date, COUNT(*) AS orders
FROM source.orders
WHERE order_date BETWEEN DATE '2026-01-01' AND DATE '2026-01-31'
GROUP BY order_date
ORDER BY order_date;
```

Expect 31 dates with rows. Airflow task logs report how many requested dates were generated and how many were already complete.
