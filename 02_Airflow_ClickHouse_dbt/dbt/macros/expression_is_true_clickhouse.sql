{% test expression_is_true_clickhouse(model, expression, column_name=None) %}
{# ---------------------------------------------------------------------------
  Custom ClickHouse-compatible test.
  Usage (in schema.yml):

    tests:
      - expression_is_true_clickhouse:
          column_name: ASHP_Power
          expression: "ASHP_Power >= 0"
  --------------------------------------------------------------------------- #}

SELECT *
FROM {{ model }}
WHERE NOT ({{ expression }})

{% endtest %}
