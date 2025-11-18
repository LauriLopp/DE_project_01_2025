
  
    
    
    
        
        insert into `default`.`fact_heating_energy_usage`
        ("FactKey", "TimeKey", "DeviceKey", "LocationKey", "ElectricityPrice", "ASHP_Power", "Boiler_Power", "Air_Drier_Power", "Boiler_Voltage", "IndoorTemp", "IndoorHumidityPerc", "IndoorHumidityAbs", "WC_HumidityAbs", "WC_Temp", "OutdoorTemp", "DewPoint", "OutdoorHumidityPerc", "CloudCoverage", "UV_Index", "AirPressure_mmHg", "WindDir", "WindGustSpeed_ms", "WindSpeed_ms", "WeatherCondition")

-- ============================================================================
-- FACT_HEATING_ENERGY_USAGE
-- ---------------------------------------------------------------------------
-- Gold-layer fact table combining IoT sensor data, pricing, weather,
-- device and location dimensions.
-- Works fully in ClickHouse and uses CROSS JOIN + WHERE for range logic.
-- ============================================================================

WITH base AS (
    SELECT
        i.hour_start,
        t.TimeKey,
        p.price_eur_per_kwh           AS ElectricityPrice,
        i.heat_pump_power_avg         AS ASHP_Power,
        i.boiler_power_avg            AS Boiler_Power,
        i.air_drier_power_avg         AS Air_Drier_Power,
        i.boiler_voltage_avg          AS Boiler_Voltage,
        i.living_room_temp_avg        AS IndoorTemp,
        i.living_room_hum_perc_avg    AS IndoorHumidityPerc,
        i.living_room_hum_abs_avg     AS IndoorHumidityAbs,
        i.wc_hum_abs_avg              AS WC_HumidityAbs,
        i.wc_temp_avg                 AS WC_Temp,
        w.temperature_c               AS OutdoorTemp,
        w.dew_point_c                 AS DewPoint,
        w.humidity_perc               AS OutdoorHumidityPerc,
        w.cloud_coverage_pct          AS CloudCoverage,
        w.uv_index                    AS UV_Index,
        w.air_pressure_mmhg           AS AirPressure_mmHg,
        w.wind_direction_deg          AS WindDir,
        w.wind_gust_speed_ms          AS WindGustSpeed_ms,
        w.wind_speed_ms               AS WindSpeed_ms,
        w.condition_state             AS WeatherCondition
    FROM `default`.`stg_iot_data` AS i
      JOIN `default`.`dim_time` AS t
        ON i.hour_start = toDateTime(t.FullDate, 'Europe/Tallinn') + toIntervalHour(t.HourOfDay)
    LEFT JOIN `default`.`stg_weather_data` AS w
      ON i.hour_start = toStartOfHour(w.timestamp)
    LEFT JOIN `default`.`stg_price_data` AS p
      ON i.hour_start = toStartOfHour(p.timestamp)
)

-- ClickHouse-safe join logic
SELECT
  row_number() OVER (
    ORDER BY b.TimeKey, d.DeviceKey, l.LocationKey
  )                                          AS FactKey,
    b.TimeKey,
    d.DeviceKey,
    l.LocationKey,
    b.ElectricityPrice,
    b.ASHP_Power,
    b.Boiler_Power,
    b.Air_Drier_Power,
    b.Boiler_Voltage,
    b.IndoorTemp,
    b.IndoorHumidityPerc,
    b.IndoorHumidityAbs,
    b.WC_HumidityAbs,
    b.WC_Temp,
    b.OutdoorTemp,
    b.DewPoint,
    b.OutdoorHumidityPerc,
    b.CloudCoverage,
    b.UV_Index,
    b.AirPressure_mmHg,
    b.WindDir,
    b.WindGustSpeed_ms,
    b.WindSpeed_ms,
    b.WeatherCondition
FROM base AS b
CROSS JOIN `default`.`dim_device` AS d
CROSS JOIN `default`.`dim_location` AS l
WHERE
    -- Device validity window
    toDate(b.hour_start)
        BETWEEN toDate(COALESCE(d.InstallationDate, toDate('2000-01-01')))
            AND toDate(COALESCE(d.ValidTo, toDate('9999-12-31')))
    -- Location validity window
    AND toDate(b.hour_start)
        BETWEEN toDate(COALESCE(l.ValidFrom, toDate('2000-01-01')))
            AND toDate(COALESCE(l.ValidTo,   toDate('9999-12-31')))
  
  