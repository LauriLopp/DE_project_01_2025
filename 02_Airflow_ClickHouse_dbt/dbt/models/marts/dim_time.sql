{{
  config(
    materialized='table'
  )
}}

-- Gold layer: DIM_TIME
-- This dimension table is generated to cover the time range in stg_iot_data.sql.
-- It includes the following columns:
--   - TimeKey: Primary key
--   - FullDate: Full date (not null)
--   - Year: Year of the date
--   - Month: Month of the date
--   - Day: Day of the date
--   - DayOfWeek: Day of the week (1=Monday, 7=Sunday)
--   - HourOfDay: Hour of the day (0-23)
--   - Season: Season of the year
--   - IsHoliday: Boolean indicating if the date is a holiday
--   - IsWeekend: Boolean indicating if the date is a weekend
--   - IsPeakHour: Boolean indicating if the hour is a peak hour

WITH time_range AS (
  SELECT
    min(timestamp) AS start_time,
    max(timestamp) AS end_time
  FROM {{ ref('stg_iot_data') }}
),
all_times AS (
  SELECT
    toDateTime(start_time + number * 3600) AS timestamp
  FROM time_range,
       numbers(dateDiff('hour', start_time, end_time) + 1)
),
base_time AS (
  SELECT
    toDate(timestamp) AS FullDate,
    toYear(timestamp) AS Year,
    toMonth(timestamp) AS Month,
    toDayOfMonth(timestamp) AS Day,
    toDayOfWeek(timestamp) AS DayOfWeek,
    toHour(timestamp) AS HourOfDay,
    multiIf(
      Month IN (12, 1, 2), 'Winter',
      Month IN (3, 4, 5), 'Spring',
      Month IN (6, 7, 8), 'Summer',
      'Autumn'
    ) AS Season,
    DayOfWeek IN (6, 7) AS IsWeekend,
    HourOfDay IN (7, 8, 9, 17, 18, 19) AS IsPeakHour
  FROM all_times
),
holidays AS (
  SELECT
    toDate(date) AS holiday_date
  FROM {{ ref('estonian_holidays') }}
)
SELECT
  row_number() OVER () AS TimeKey,
  bt.FullDate,
  bt.Year,
  bt.Month,
  bt.Day,
  bt.DayOfWeek,
  bt.HourOfDay,
  bt.Season,
  holiday_date IS NOT NULL AS IsHoliday,
  bt.IsWeekend,
  bt.IsPeakHour
FROM base_time bt
LEFT JOIN holidays h
  ON bt.FullDate = h.holiday_date;