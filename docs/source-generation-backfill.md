# PostgreSQL source generation backfill

The manual Airflow DAG `ampere__pre_raw__generators__backfill` runs the existing generator for an inclusive date range. It starts one Kubernetes pod on `ampere-k8s-node4`; that process generates each day sequentially and commits each day in its own PostgreSQL transaction. A retry skips dates already present in `source.orders` and continues from the next date. Gaps are rejected.

Trigger the DAG with JSON such as:

```json
{
  "start_date": "2026-10-12",
  "end_date": "2026-11-10",
  "seed": 42
}
```

Both dates are inclusive, and the range may contain at most 31 days. Before choosing dates, check the existing source watermark:

```sql
SELECT MAX(order_date) AS completed_through FROM source.orders;
```

Choose the first date after that watermark; the example is based on the successful daily generation for 2026-10-11. Use the same range and seed when retrying a failed run. A date already committed by the scheduled daily DAG is skipped by the backfill.

This DAG only writes PostgreSQL. It does not initialize or truncate source tables and does not trigger Raw or Bronze. Do not run `ampere__pre_raw__generators__init` for this test: that DAG truncates and repopulates the existing source tables.

After a successful run, check the generated date coverage with:

```sql
SELECT order_date, COUNT(*) AS orders
FROM source.orders
WHERE order_date BETWEEN DATE '2026-10-12' AND DATE '2026-11-10'
GROUP BY order_date
ORDER BY order_date;
```

Replace the example dates if the test starts later. Airflow task logs report how many requested dates were generated and how many were already complete.
