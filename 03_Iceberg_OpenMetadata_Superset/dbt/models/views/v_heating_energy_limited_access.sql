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
ASHP-Power - shown in 100 W ranges
WC_Temp - fully masked
ElectricityPrice - shown in 50 cent ranges
*/

SELECT
    FactKey,
    TimeKey,
    DeviceKey,
    LocationKey,

	-- Floor to nearest 0.5, then construct string "0.0 – 0.5", "0.5 – 1.0", etc.
    concat(
        toString(floor(ElectricityPrice * 2) / 2),
        '...',
        toString((floor(ElectricityPrice * 2) / 2) + 0.5)
    ) AS ElectricityPrice_Range,

    -- Floor to nearest 100, then construct string "100 – 200", "200 – 300", etc.
    concat(
        toString(floor(ASHP_Power / 100) * 100),
        '...',
        toString((floor(ASHP_Power / 100) * 100) + 100)
    ) AS ASHP_Power_Range,
    Boiler_Power,
    Air_Drier_Power,
    Boiler_Voltage,
    IndoorTemp,
    IndoorHumidityPerc,
    IndoorHumidityAbs,
    WC_HumidityAbs,

	-- Completely hide WC_temp.
    'Masked' AS WC_Temp,

    OutdoorTemp,
    DewPoint,
    OutdoorHumidityPerc,
    CloudCoverage,
    UV_Index,
    AirPressure_mmHg,
    WindDir,
    WindGustSpeed_ms,
    WindSpeed_ms,
    WeatherCondition

FROM {{ ref('fact_heating_energy_usage') }}