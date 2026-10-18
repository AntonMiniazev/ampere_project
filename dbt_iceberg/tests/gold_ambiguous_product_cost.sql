{{ config(tags=['gold']) }}
select fact_order_product_key from {{ ref('int_product_line_costs') }} where cost_match_count > 1
