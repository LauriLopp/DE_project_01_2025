

-- Staging: transform long IoT readings into a wide table per hour with averages
-- Contract:
--   Input  (bronze_iot_raw_data): ingestion_ts DateTime, entity_id String, state String, last_changed DateTime, attributes String
--   Output (view): one row per hour with average values per measurement
-- Notes:
--   - Values are cast to Float64 where possible (non-numeric -> NULL)
--   - Aggregated to hourly averages using toStartOfHour()

WITH base AS (
	SELECT
		-- Parse IoT timestamps (often ISO with 'Z') and convert to local time
		toTimeZone(last_changed, 'Europe/Tallinn') as timestamp,
		entity_id,
		toFloat64OrNull(state)                                                    AS value
	FROM `default`.`bronze_iot_raw_data`
)
SELECT
	row_number() over () as IoTKey,
	toStartOfHour(timestamp) as hour_start,
	-- Power sensors (hourly averages)
	avgIf(value, entity_id = 'sensor.ohksoojus_power')                  AS heat_pump_power_avg,
	avgIf(value, entity_id = 'sensor.0xa4c138cdc6eff777_power')         AS boiler_power_avg,
	avgIf(value, entity_id = 'sensor.ohukuivati_power')                 AS air_drier_power_avg,

	-- Humidity (absolute and relative, hourly averages)
	avgIf(value, entity_id = 'sensor.abshumidkuu2_absolute_humidity')   AS living_room_hum_abs_avg,
	avgIf(value, entity_id = 'sensor.tempniiskuslauaall_humidity')      AS living_room_hum_perc_avg,
	avgIf(value, entity_id = 'sensor.indoor_absolute_humidity')         AS wc_hum_abs_avg,

	-- Temperatures (hourly averages)
	avgIf(value, entity_id = 'sensor.tempniiskuslauaall_temperature')   AS living_room_temp_avg,
	avgIf(value, entity_id = 'sensor.indoor_outdoor_meter_3866')        AS wc_temp_avg,

	-- Voltage sensors (hourly averages)
	avgIf(value, entity_id = 'sensor.0xa4c138cdc6eff777_voltage')       AS boiler_voltage_avg
FROM base
GROUP BY toStartOfHour(timestamp)