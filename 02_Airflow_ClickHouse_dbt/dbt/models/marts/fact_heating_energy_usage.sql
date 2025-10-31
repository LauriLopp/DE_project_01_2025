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
    row_number() OVER () AS FactID, -- Primary key
    t.TimeKey,
    d.DeviceKey,
    l.LocationKey,
    i.heat_pump_power AS ASHP_Power,
    p.price_eur_with_vat AS ElectricityPrice,
    i.living_room_temp AS IndoorTemp,
    i.living_room_hum_perc AS IndoorHumidityPerc,
    i.living_room_hum_abs AS IndoorHumidityAbs,
    i.air_purifier_2_5 AS IndoorAir_pm25,
    i.air_purifier_10 AS IndoorAir_pm10,
    w.temperature_c AS OutdoorTemp,
    w.humidity_perc AS OutdoorHumidityPerc,
    w.air_pressure_hpa AS AirPressure,
    w.wind_speed_ms AS WindSpeed,
    w.wind_direction_deg AS WindDir,
    w.precipitation_mm AS Precipitation,
    w.uv_index AS UV_index,
    w.illuminance_lux AS Illumination,
    w.solar_irradiance AS Irradiation
  FROM {{ ref('stg_iot_data') }} i
  JOIN {{ ref('dim_time') }} t
    ON toStartOfHour(i.timestamp) = t.FullDate + INTERVAL t.HourOfDay HOUR
  JOIN {{ ref('dim_device') }} d
    ON i.timestamp BETWEEN d.InstallationDate AND d.ValidTo
  JOIN {{ ref('dim_location') }} l
    ON i.timestamp BETWEEN l.ValidFrom AND l.ValidTo
  LEFT JOIN {{ ref('stg_weather_data') }} w
    ON toStartOfHour(i.timestamp) = w.timestamp
  LEFT JOIN {{ ref('stg_price_data') }} p
    ON toStartOfHour(i.timestamp) = p.timestamp
)
SELECT *
FROM base_data