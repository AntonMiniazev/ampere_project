# Silver/Gold incremental validation

The v2 correction treats PostgreSQL initial/source, Raw and Bronze as read-only.
It changes only downstream models, publication metadata and runtime diagnostics.
The Raw/Bronze contract sections and upstream generator code are unchanged.

## Publication boundaries

Every Raw delta branch and client history read selects `ICEBERG_DATASET_ID`.
Daily history reads use the same bounded batch interval as affected orders.
A daily dataset transition fails before staging; an explicit downstream full
rebuild establishes the new dataset's baseline. Gold requires Silver to have a
successful baseline for that dataset before it builds anything.

`pipeline.silver_publications.status` distinguishes pending invalidations from
completed publications. Pending month sets are stored before the first Iceberg
commit, merged on retry, and completed with the Silver checkpoint only after all
tables succeed. Gold consumes completed rows through the sequence frozen at
preparation. Metadata migration occurs in `pipeline.*` during normal downstream
execution; no catalog-init or upstream initialization is needed.
Gold refuses to read Silver while a pending publication exists, even if an older
successful checkpoint remains, because independent table commits may be mixed.

Budget comparison includes deleted keys and both dates of moved keys. Empty
complete budget snapshots are valid. Changed store labels invalidate budget-only
months, while Raw dimension deltas expand affected orders for labels and costs.
Gold replaces only explicitly affected months, including empty ones. An empty
affected set checkpoints a Gold no-op without building models.

## Domain and unknown history

A store covers multiple zones. A recorded order zone must be covered by that
order's selected store. Neither an arbitrary zone nor a store ID supplies a
missing delivery zone. Preferred-store history uses current-dataset records and
effective intervals. `preferred_zone_id_at_order` remains a nullable compatibility
column and is always NULL in Silver.

The shared Gold delivery intermediate resolves the final recorded assignment and
uses the order's selected store and courier type, without requiring an order zone.
Every covered zone must have exactly one non-null tariff for that type, and all
zone tariffs must agree. The shared amount produces one cost row per order;
no zone is assigned and the client's current preferred store is not used.
Duplicate or conflicting tariffs are ambiguous; missing coverage or any missing
zone tariff leaves the cost unknown. Zone/tariff changes invalidate orders at
the store level, including prior store mappings for moved/deleted zones.
Product
costing similarly resolves one effective interval per line. Ambiguous inputs fail
tests; missing inputs produce warnings and NULL amounts. Monthly totals remain
NULL if any contributing cost is unknown. Budget-only months with no actual
orders still have zero actuals. Tariff history absent from the existing source
cannot be reconstructed by this change.

## Daily execution and reference validation

Daily Silver and Gold DAGs trigger their downstream DAG immediately after a
successful build. There is no intermediate handoff-check task or isolation flag.

Use the existing Silver and Gold full-rebuild DAGs with the following configuration
to build a local full-history reference from existing inputs and compare it to
published tables:

```json
{"validation_only": true}
```

This mode issues only SELECTs to published Iceberg tables and PostgreSQL metadata.
It writes local staged DuckDB files, never publishes tables or advances a
checkpoint, and runs on the full-rebuild DAGs, which have no downstream triggers. It requires existing metadata and
published target tables; missing infrastructure is a blocker, not permission to
initialize upstream data. Each table reports exact missing/extra business-row
counts and differences in month/store/product aggregate signatures. Excluded
build lineage is not a business difference. Every count must be zero for parity.

## Evidence and resource limits

Pod and DuckDB memory limits are unchanged. Phase logs report exit code/signal,
child RSS, kernel cgroup peak when exposed, sampled cgroup memory, sampled spill
peak, and elapsed time. Missing kernel metrics are NULL; sampled values and child
RSS are not exact pod peaks. Failed pods are retained for termination and Event
inspection; successful pods are deleted normally.

Publisher JSON query profiles report actual scanned rows, operator/table details,
buffer/temp peaks and file counters when exposed by the extension. A no-change
MERGE must report `business_rows_affected: 0`. A file filter alone does not set
`file_scan_metrics_available`. If Iceberg does not expose target file counts,
record that gap and obtain equivalent storage/engine evidence before claiming
pruning. Delivery tracking is unpartitioned: ID ranges bound merge work, not the
number of Iceberg files scanned. Full target scans are reported rather than
described as partition pruning.

Use `.local/tests/silver_gold_incremental_v2/validation.ipynb` on the Windows
development machine for before/after upstream snapshot checks, source mutation
counters, input cardinality summaries and untouched Gold month signatures. Only
aggregates are transferred locally. Fixture results, reference parity and live
resource measurements are separate acceptance evidence.

## Fact dependencies and partition discovery

The internal affected-order table carries flags for each dependent fact and Gold.
Client churn updates flattened client attributes in fact_orders and effective
client history, without rebuilding historical order-product, status, tracking or
payment facts. Product/category labels select the line fact; resource changes
select tracking; status changes select status history and payment status labels.
Order changes select all dependent facts, preserving moved/deleted-order handling.
Gold month invalidation uses order dates and Gold-relevant changes; client history
invalidates its effective month and later months, rather than past order months.

Daily line publication gets old dates from published fact_orders and new dates
from staged lines. It never scans fact_order_product to discover dates. Literal
day bounds constrain each target MERGE and cleanup. Full rebuilds retain complete
history with weekly slices. The publisher orders dependent facts before
fact_orders so old dates remain available until the dependent commits finish.
Retries preserve pending invalidations from interrupted publications, including
any wider month set prepared by an earlier version.
