{{ config(tags=['gold'], severity='warn') }}
select cost_match_count, count(*) as affected_lines
from {{ ref('int_product_line_costs') }}
where product_cost_amount is null
group by cost_match_count
