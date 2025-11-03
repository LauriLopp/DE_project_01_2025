{% test expression_is_true_clickhouse(model, expression, column_name=None) %}

{#- Determine whether failures should store full rows or a sentinel column -#}
{% set column_list = '*' if should_store_failures() else '1' %}

{#- Allow expressions that already include the column reference -#}
{% set predicate = expression %}
{% if column_name is not none and column_name not in expression %}
  {% set predicate = column_name ~ ' ' ~ expression %}
{% endif %}

select
    {{ column_list }}
from {{ model }}
where not ({{ predicate }})

{% endtest %}
