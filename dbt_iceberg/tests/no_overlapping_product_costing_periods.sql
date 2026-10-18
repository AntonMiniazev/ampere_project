select left_cost.costing_key, right_cost.costing_key
from {{ ref('dim_costing') }} as left_cost
join {{ ref('dim_costing') }} as right_cost
  on left_cost.product_id = right_cost.product_id
 and left_cost.store_id = right_cost.store_id
 and left_cost.costing_key < right_cost.costing_key
 and left_cost.valid_from < coalesce(right_cost.valid_to, DATE '9999-12-31')
 and right_cost.valid_from < coalesce(left_cost.valid_to, DATE '9999-12-31')
