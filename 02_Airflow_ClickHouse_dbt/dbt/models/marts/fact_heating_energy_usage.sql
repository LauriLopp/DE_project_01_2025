{{
  config(
    materialized='table'
  )
}}

-- Gold layer: FACT_HEATING_ENERGY_USAGE
-- This fact table combines data from staging tables and dimensions.
-- It includes the following columns:
--   - FactID: Primary key
--   - TimeKey: Foreign key to DIM_TIME
--   - DeviceKey: Foreign key to DIM_DEVICE
--   - LocationKey: Foreign key to DIM_LOCATION
--   - Various metrics related to heating energy usage

WITH base_data AS (
  SELECT
    row_number() OVER () AS FactID,
    t.TimeKey,
    d.DeviceKey,
    l.LocationKey,
    p.price_eur_per_kwh AS ElectricityPrice,
    i.heat_pump_power_avg AS ASHP_Power,
    i.boiler_power_avg AS Boiler_Power,
    i.air_drier_power_avg AS Air_Drier_Power,
    i.boiler_voltage_avg AS Boiler_Voltage,
    i.living_room_temp_avg AS IndoorTemp,
    i.living_room_hum_perc_avg AS IndoorHumidityPerc,
    i.living_room_hum_abs_avg AS IndoorHumidityAbs,
    i.wc_hum_abs_avg AS WC_HumidityAbs,
    i.wc_temp_avg AS WC_Temp,
    w.temperature_c AS OutdoorTemp,
    w.dew_point_c AS DewPoint,
    w.humidity_perc AS OutdoorHumidityPerc,
    w.cloud_coverage_pct AS CloudCoverage,
    w.uv_index AS UV_index,
    w.air_pressure_mmhg AS AirPressure_mmHg,
    w.wind_direction_deg AS WindDir,
    w.wind_gust_speed_ms AS WindGustSpeed_ms,
    w.wind_speed_ms AS WindSpeed_ms,
    w.condition_state AS WeatherCondition
  FROM {{ ref('stg_iot_data') }} i
  JOIN {{ ref('dim_time') }} t
    ON i.hour_start = t.FullDate + INTERVAL t.HourOfDay HOUR
  , {{ ref('dim_device') }} d
  , {{ ref('dim_location') }} l
  LEFT JOIN {{ ref('stg_weather_data') }} w
    ON i.hour_start = w.timestamp
  LEFT JOIN {{ ref('stg_price_data') }} p
    ON i.hour_start = p.timestamp
  WHERE i.hour_start BETWEEN d.InstallationDate AND d.ValidTo
    AND i.hour_start BETWEEN l.ValidFrom AND l.ValidTo
)
SELECT *
FROM base_data