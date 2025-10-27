{{
	config(
		materialized='view'
	)
}}

-- Staging: transform long IoT readings into a wide table per timestamp
-- Contract:
--   Input  (raw_data.iot_andmed): entity_id String, state String, last_changed DateTime
--   Output (view): one row per timestamp with a column per measurement
-- Notes:
--   - Values are cast to Float64 where possible (non-numeric -> NULL)
--   - Add/remove maxIf(...) columns below to extend the wide schema

WITH base AS (
	SELECT
		-- Parse IoT timestamps (often ISO with 'Z') and convert to local time
		toTimeZone(parseDateTimeBestEffortOrNull(last_changed), 'Europe/Tallinn') AS timestamp,
		entity_id,
		toFloat64OrNull(state)                                                    AS value
	FROM {{ source('raw_data', 'iot_andmed') }}
)
SELECT
	row_number() over () as IoTKey,
	timestamp,
	-- Power sensors
	maxIf(value, entity_id = 'sensor.ohksoojus_power')                         AS heat_pump_power,
	maxIf(value, entity_id = 'sensor.0xa4c138cdc6eff777_power')                AS boiler_power,

	-- Humidity (absolute and relative)
	maxIf(value, entity_id = 'sensor.abshumidkuu2_absolute_humidity')          AS living_room_hum_abs,
	maxIf(value, entity_id = 'sensor.tempniiskuslauaall_humidity')             AS living_room_hum_perc,

	-- Temperatures
	maxIf(value, entity_id = 'sensor.tempniiskuslauaall_temperature')          AS living_room_temp,
	maxIf(value, entity_id = 'sensor.indoor_outdoor_meter_3866')               AS wc_temp,

	-- Boiler voltage
	maxIf(value, entity_id = 'sensor.0xa4c138cdc6eff777_voltage')              AS boiler_voltage,

	-- Air purifier particulate matter
	maxIf(value, entity_id = 'sensor.air_purifier_particulate_matter_2_5')     AS air_purifier_2_5,
	maxIf(value, entity_id = 'sensor.air_purifier_particulate_matter_10')      AS air_purifier_10
FROM base
GROUP BY timestamp
