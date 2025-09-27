-- Checks that a numeric column is non-negative (>= 0)
{% test positive_number(model, column_name) %}
select {{ column_name }} as failing_value
from {{ model }}
where try_cast({{ column_name }} as double) < 0
   or {{ column_name }} is null
{% endtest %}