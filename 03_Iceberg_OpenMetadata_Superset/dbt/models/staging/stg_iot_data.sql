{{
	config(
		materialized='view'
	)
}}

WITH base AS (
	SELECT
		-- Properly convert from UTC to Tallinn (last_changed is stored as UTC)
		toTimeZone(toDateTime(last_changed, 'UTC'), 'Europe/Tallinn') as timestamp,
		entity_id,
		toFloat64OrNull(state)                                                    AS value
	FROM {{ source('bronze_layer', 'bronze_iot_raw_data') }}
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
