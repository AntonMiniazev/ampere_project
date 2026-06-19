# Unity Catalog table inventory

Source: `docs/data_contracts/*.json`
Extracted at UTC: `2026-06-19T22:48:05+00:00`

| Catalog | Schema | Table | Layer | Storage location | Comment / Description |
|---|---|---|---|---|---|
| ampere | bronze | assortment | bronze | s3://ampere-bronze/bronze/source/assortment | Bronze table: assortment |
| ampere | bronze | bronze_apply_registry | bronze | s3://ampere-bronze/bronze/ops/bronze_apply_registry | Operational table: bronze_apply_registry |
| ampere | bronze | clients | bronze | s3://ampere-bronze/bronze/source/clients | Bronze table: clients |
| ampere | bronze | costing | bronze | s3://ampere-bronze/bronze/source/costing | Bronze table: costing |
| ampere | bronze | delivery_costing | bronze | s3://ampere-bronze/bronze/source/delivery_costing | Bronze table: delivery_costing |
| ampere | bronze | delivery_resource | bronze | s3://ampere-bronze/bronze/source/delivery_resource | Bronze table: delivery_resource |
| ampere | bronze | delivery_tracking | bronze | s3://ampere-bronze/bronze/source/delivery_tracking | Bronze table: delivery_tracking |
| ampere | bronze | delivery_type | bronze | s3://ampere-bronze/bronze/source/delivery_type | Bronze table: delivery_type |
| ampere | bronze | order_product | bronze | s3://ampere-bronze/bronze/source/order_product | Bronze table: order_product |
| ampere | bronze | order_status_history | bronze | s3://ampere-bronze/bronze/source/order_status_history | Bronze table: order_status_history |
| ampere | bronze | order_statuses | bronze | s3://ampere-bronze/bronze/source/order_statuses | Bronze table: order_statuses |
| ampere | bronze | orders | bronze | s3://ampere-bronze/bronze/source/orders | Bronze table: orders |
| ampere | bronze | payments | bronze | s3://ampere-bronze/bronze/source/payments | Bronze table: payments |
| ampere | bronze | product_categories | bronze | s3://ampere-bronze/bronze/source/product_categories | Bronze table: product_categories |
| ampere | bronze | products | bronze | s3://ampere-bronze/bronze/source/products | Bronze table: products |
| ampere | bronze | stores | bronze | s3://ampere-bronze/bronze/source/stores | Bronze table: stores |
| ampere | bronze | zones | bronze | s3://ampere-bronze/bronze/source/zones | Bronze table: zones |
| ampere | gold | budget_orders_sales | gold | s3://ampere-gold/gold/budget_orders_sales | Gold table: budget_orders_sales |
| ampere | gold | dim_clients | gold | s3://ampere-gold/gold/dim_clients | Gold table: dim_clients |
| ampere | gold | dim_costing | gold | s3://ampere-gold/gold/dim_costing | Gold table: dim_costing |
| ampere | gold | dim_delivery_cost | gold | s3://ampere-gold/gold/dim_delivery_cost | Gold table: dim_delivery_cost |
| ampere | gold | dim_products | gold | s3://ampere-gold/gold/dim_products | Gold table: dim_products |
| ampere | gold | dim_resource | gold | s3://ampere-gold/gold/dim_resource | Gold table: dim_resource |
| ampere | gold | dim_stores | gold | s3://ampere-gold/gold/dim_stores | Gold table: dim_stores |
| ampere | gold | fct_deliveries | gold | s3://ampere-gold/gold/fct_deliveries | Gold table: fct_deliveries |
| ampere | gold | fct_order_margin | gold | s3://ampere-gold/gold/fct_order_margin | Gold table: fct_order_margin |
| ampere | gold | fct_orders_sales | gold | s3://ampere-gold/gold/fct_orders_sales | Gold table: fct_orders_sales |
| ampere | silver | budget_orders_sales | silver | s3://ampere-silver/silver/budget_orders_sales | Silver table: budget_orders_sales |
| ampere | silver | dim_assortment | silver | s3://ampere-silver/silver/dim_assortment | Silver table: dim_assortment |
| ampere | silver | dim_clients | silver | s3://ampere-silver/silver/dim_clients | Silver table: dim_clients |
| ampere | silver | dim_costing | silver | s3://ampere-silver/silver/dim_costing | Silver table: dim_costing |
| ampere | silver | dim_delivery_costing | silver | s3://ampere-silver/silver/dim_delivery_costing | Silver table: dim_delivery_costing |
| ampere | silver | dim_delivery_resource | silver | s3://ampere-silver/silver/dim_delivery_resource | Silver table: dim_delivery_resource |
| ampere | silver | dim_delivery_type | silver | s3://ampere-silver/silver/dim_delivery_type | Silver table: dim_delivery_type |
| ampere | silver | dim_order_statuses | silver | s3://ampere-silver/silver/dim_order_statuses | Silver table: dim_order_statuses |
| ampere | silver | dim_product_categories | silver | s3://ampere-silver/silver/dim_product_categories | Silver table: dim_product_categories |
| ampere | silver | dim_products | silver | s3://ampere-silver/silver/dim_products | Silver table: dim_products |
| ampere | silver | dim_stores | silver | s3://ampere-silver/silver/dim_stores | Silver table: dim_stores |
| ampere | silver | dim_zones | silver | s3://ampere-silver/silver/dim_zones | Silver table: dim_zones |
| ampere | silver | fact_delivery_tracking | silver | s3://ampere-silver/silver/fact_delivery_tracking | Silver table: fact_delivery_tracking |
| ampere | silver | fact_order_product | silver | s3://ampere-silver/silver/fact_order_product | Silver table: fact_order_product |
| ampere | silver | fact_order_status_history | silver | s3://ampere-silver/silver/fact_order_status_history | Silver table: fact_order_status_history |
| ampere | silver | fact_orders | silver | s3://ampere-silver/silver/fact_orders | Silver table: fact_orders |
| ampere | silver | fact_payments | silver | s3://ampere-silver/silver/fact_payments | Silver table: fact_payments |
