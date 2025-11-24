{{ config(
    materialized='view',
    sql_security='DEFINER',
    post_hook=[
        "ALTER TABLE {{ this }} MODIFY SQL SECURITY DEFINER",
        "GRANT SELECT ON {{ this }} TO analyst_limited"
    ]
) }}

-- Still a draft
-- ToDo: make sure to pseudonymize somewhat reasonable data
SELECT
    TimeKey,
    DeviceKey,
    LocationKey,

    -- Pseudonymized values
    -- IndoorTemp_masked,
    -- OutdoorTemp_masked,
    -- ElectricityPrice_masked,

    ASHP_Power,
    Boiler_Power,
    Air_Drier_Power,
    DewPoint,
    CloudCoverage,
    UV_Index,
    AirPressure_mmHg,
    WindDir,
    WindGustSpeed_ms,
    WindSpeed_ms
FROM {{ ref('fact_heating_energy_usage') }}
