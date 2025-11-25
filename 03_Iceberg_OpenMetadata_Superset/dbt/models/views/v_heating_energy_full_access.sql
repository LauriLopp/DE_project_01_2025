{{ config(
    materialized='view',
    sql_security='DEFINER',
    post_hook=[
        "ALTER TABLE {{ this }} MODIFY SQL SECURITY DEFINER",
        "GRANT SELECT ON {{ this }} TO analyst_full"
    ]
) }}

SELECT *
FROM {{ ref('fact_heating_energy_usage') }}