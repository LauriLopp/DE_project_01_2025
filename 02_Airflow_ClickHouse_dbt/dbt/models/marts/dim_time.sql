{{ config(materialized='table') }}

-- Gold-layer: DIM_TIME (sparse)
-- Build only the hours actually used across staging sources (IoT, weather, price)

WITH needed_hours AS (
  SELECT toStartOfHour(hour_start) AS ts
  FROM {{ ref('stg_iot_data') }}
  WHERE hour_start IS NOT NULL
  UNION ALL
  SELECT toStartOfHour(timestamp) AS ts
  FROM {{ ref('stg_weather_data') }}
  WHERE timestamp IS NOT NULL
  UNION ALL
  SELECT toStartOfHour(timestamp) AS ts
  FROM {{ ref('stg_price_data') }}
  WHERE timestamp IS NOT NULL
),
distinct_hours AS (
  SELECT DISTINCT ts FROM needed_hours
),
base_time AS (
  SELECT
    ts,
    toDate(ts)       AS FullDate,
    toYear(ts)       AS Year,
    toMonth(ts)      AS Month,
    toDayOfMonth(ts) AS Day,
    toDayOfWeek(ts)  AS DayOfWeek,
    toHour(ts)       AS HourOfDay,
    multiIf(
      Month IN (12,1,2), 'Winter',
      Month IN (3,4,5), 'Spring',
      Month IN (6,7,8), 'Summer',
      'Autumn'
    ) AS Season,
    DayOfWeek IN (6,7) AS IsWeekend,
    HourOfDay IN (7,8,9,17,18,19) AS IsPeakHour
  FROM distinct_hours
),
holidays AS (
  SELECT toDate(timestamp) AS holiday_date
  FROM {{ ref('estonian_holidays') }}
)

SELECT
  row_number() OVER (ORDER BY ts) AS TimeKey,
  bt.FullDate,
  bt.Year,
  bt.Month,
  bt.Day,
  bt.DayOfWeek,
  bt.HourOfDay,
  bt.Season,
  h.holiday_date IS NOT NULL AS IsHoliday,
  bt.IsWeekend,
  bt.IsPeakHour
FROM base_time AS bt
LEFT JOIN holidays AS h
  ON bt.FullDate = h.holiday_date
