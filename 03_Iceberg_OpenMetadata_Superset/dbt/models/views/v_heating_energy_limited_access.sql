{{ config(
    materialized='view',
    sql_security='DEFINER',
    post_hook=[
        "ALTER TABLE {{ this }} MODIFY SQL SECURITY DEFINER",
        "GRANT SELECT ON {{ this }} TO analyst_limited"
    ]
) }}

/*
Pseudonymized columns:
WC_Temp - shown in 5 degree ranges
ElectricityPrice - shown in 50 cent ranges
WeatherCondition - hash the actual condition,
    so that different conditions are recognized but not identified
*/

SELECT
    FactKey,
    TimeKey,
    DeviceKey,
    LocationKey,

	-- Floor to nearest 0.5, then construct string "0.0 – 0.5", "0.5 – 1.0", etc.
    concat(
        toString(floor(ElectricityPrice * 2) / 2),
        ' – ',
        toString((floor(ElectricityPrice * 2) / 2) + 0.5)
    ) AS ElectricityPrice_Range,

    ASHP_Power,
    Boiler_Power,
    Air_Drier_Power,
    Boiler_Voltage,
    IndoorTemp,
    IndoorHumidityPerc,
    IndoorHumidityAbs,
    WC_HumidityAbs,

	-- Floor to nearest 5, then construct string "20 – 25", "25 – 30", etc.
    concat(
        toString(floor(WC_Temp / 5) * 5),
        ' – ',
        toString((floor(WC_Temp / 5) * 5) + 5)
    ) AS WC_Temp_Range,

    OutdoorTemp,
    DewPoint,
    OutdoorHumidityPerc,
    CloudCoverage,
    UV_Index,
    AirPressure_mmHg,
    WindDir,
    WindGustSpeed_ms,
    WindSpeed_ms,

	-- Hide WeatherCondition using hashing
    lower(hex(SHA256(concat(WeatherCondition, 'my_super_secret_salt_2025')))) AS WeatherCondition

FROM {{ ref('fact_heating_energy_usage') }}