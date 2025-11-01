{{ config(materialized='table') }}

-- ============================================================================
-- DIM_TIME
-- ---------------------------------------------------------------------------
-- Gold-layer time dimension
-- Derived from stg_iot_data to include every hour observed.
-- Keeps full datetime (hour_start) for correct joins with fact tables.
-- ============================================================================

WITH base AS (
    SELECT DISTINCT
        toStartOfHour(hour_start) AS FullDate  -- keep as DateTime
    FROM {{ ref('stg_iot_data') }}
    WHERE hour_start IS NOT NULL
)

SELECT
    row_number() OVER (ORDER BY FullDate) AS TimeKey,
    FullDate,                             -- DateTime at the hour
    toDate(FullDate)                      AS DateOnly,
    toYear(FullDate)                      AS Year,
    toMonth(FullDate)                     AS Month,
    toDayOfMonth(FullDate)                AS Day,
    toDayOfWeek(FullDate)                 AS DayOfWeek,
    toHour(FullDate)                      AS Hour,
    toISOWeek(FullDate)                   AS ISOWeek,
    formatDateTime(FullDate, '%Y-%m-%d %H:00:00') AS HourLabel,
    formatDateTime(FullDate, '%Y-%m-%d') AS DateLabel,
    formatDateTime(FullDate, '%Y-%m')    AS MonthLabel,
    formatDateTime(FullDate, '%Y')       AS YearLabel
FROM base
ORDER BY FullDate
