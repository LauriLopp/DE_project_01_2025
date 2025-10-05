/*
1. How can you use an air-source heat pump to keep your home
at 20C as efficiently as possible, while taking the outdoor
temperature into account?
*/

-- Q1
-- useful for Power vs Outdoor temp analysis
WITH bucketed AS (
  SELECT
    5*FLOOR(f.OutdoorTemp/5.0) AS bin_start,
    f.ASHP_Power
  FROM FACT_HEATING_ENERGY_USAGE f
  JOIN DIM_DEVICE dd ON f.DeviceKey = dd.DeviceKey
  WHERE f.OutdoorTemp >= -25
    AND f.OutdoorTemp < 30
    AND f.ASHP_Power IS NOT NULL
    AND dd.Model = 'Daikin_123'
)
SELECT
  bin_start,
  bin_start + 5 AS bin_end,
  AVG(ASHP_Power) AS avg_power_w,
  COUNT(*) AS hours
FROM bucketed
GROUP BY bin_start
ORDER BY bin_start;

-- Q2
-- Extract data for linear model fitting outside Postgres
SELECT
  dt.FullDate,
  dt.HourOfDay,
  dt.IsWeekend,
  f.OutdoorTemp,
  f.IndoorTemp,
  f.IndoorTemp - f.OutdoorTemp AS TempDelta,
  f.ASHP_Power                 AS power_w,
  f.ASHP_Power/1000.0          AS energy_kwh_per_hr
FROM FACT_HEATING_ENERGY_USAGE f
JOIN DIM_TIME dt ON f.TimeKey = dt.TimeKey
JOIN DIM_DEVICE dd ON dd.DeviceKey = f.DeviceKey
WHERE f.IndoorTemp BETWEEN 15 AND 25
  AND f.OutdoorTemp IS NOT NULL
  AND f.ASHP_Power IS NOT NULL
  AND dd.Model = 'Daikin_123'
ORDER BY dt.FullDate, dt.HourOfDay;

-- Q3
-- How much energy is needed per temperature difference
-- (result could be used for Energy_used ≈ k × (Indoor − Outdoor))
SELECT
  SUM(f.ASHP_Power) / NULLIF(SUM(f.IndoorTemp - f.OutdoorTemp), 0) AS watts_per_C,
  COUNT(*)                                                         AS hours_used
FROM FACT_HEATING_ENERGY_USAGE f
JOIN DIM_DEVICE dd ON dd.DeviceKey = f.DeviceKey
WHERE f.IndoorTemp BETWEEN 19.5 AND 20.5
  AND f.OutdoorTemp IS NOT NULL
  AND f.IndoorTemp > f.OutdoorTemp
  AND f.ASHP_Power IS NOT NULL
  AND dd.Model = 'Daikin_123';

/*
2. At what outdoor temperatures does the A/C need more energy
in the given period?
*/

-- Q1...Q3 will answer this question as well

/*
3. How much do electricity price fluctuations impact costs
for running the AC unit?
*/


-- Q4
-- How many W/kWh do is used across different price buckets?

SELECT
  0.05 * FLOOR(f.ElectricityPrice / 0.05)           AS price_bin_eur,  -- e.g., 0.10, 0.15, ...
  AVG(f.ElectricityPrice)                           AS avg_price_in_bin,
  AVG(f.ASHP_Power)                                 AS avg_power_w,
  SUM(f.ASHP_Power)/1000.0                          AS energy_kwh,
  SUM((f.ASHP_Power/1000.0) * f.ElectricityPrice)   AS bucket_cost,       -- money spent in this bucket
  COUNT(*)                                          AS hours
FROM FACT_HEATING_ENERGY_USAGE f
JOIN DIM_TIME dt ON f.TimeKey = dt.TimeKey
JOIN DIM_DEVICE dd on f.DeviceKey = dd.DeviceKey
WHERE f.ElectricityPrice IS NOT NULL
  AND f.ASHP_Power IS NOT NULL
  AND dd.Model = 'Daikin_123'
GROUP BY 0.05 * FLOOR(f.ElectricityPrice / 0.05)
ORDER BY price_bin_eur;


-- Q5
-- Data extraction for visualization or future analysis
SELECT
  dt.FullDate,
  dt.HourOfDay,
  dt.IsPeakHour,
  dt.IsWeekend,
  dt.IsHoliday,
  f.ElectricityPrice                                AS price_eur_per_kwh,
  f.ASHP_Power                                      AS power_w,
  f.ASHP_Power/1000.0                               AS energy_kwh,
  (f.ASHP_Power/1000.0) * f.ElectricityPrice        AS cost_eur
FROM FACT_HEATING_ENERGY_USAGE f
JOIN DIM_TIME dt ON f.TimeKey = dt.TimeKey
JOIN DIM_DEVICE dd on f.DeviceKey = dd.DeviceKey
WHERE f.ASHP_Power IS NOT NULL
  AND f.ElectricityPrice IS NOT NULL
  AND dd.Model = 'Daikin_123'
ORDER BY dt.FullDate, dt.HourOfDay;

-- Q6
-- Peak-hour consumption on weekends, holidays, and weekdays
SELECT
  CASE
    WHEN dt.IsHoliday THEN 'Holiday'
    WHEN dt.IsWeekend THEN 'Weekend'
    ELSE 'Weekday'
  END                                               AS day_type,
  dt.IsPeakHour,
  SUM(f.ASHP_Power)/1000.0                          AS energy_kwh,
  SUM((f.ASHP_Power/1000.0)*f.ElectricityPrice)     AS cost_eur,
  AVG(f.ElectricityPrice)                           AS avg_price_eur_per_kwh,
  COUNT(*)                                          AS hours
FROM FACT_HEATING_ENERGY_USAGE f
JOIN DIM_TIME dt ON f.TimeKey = dt.TimeKey
JOIN DIM_DEVICE dd on f.DeviceKey = dd.DeviceKey
WHERE f.ASHP_Power IS NOT NULL
  AND f.ElectricityPrice IS NOT NULL
  AND dd.Model = 'Daikin_123'
GROUP BY 
  CASE
    WHEN dt.IsHoliday THEN 'Holiday'
    WHEN dt.IsWeekend THEN 'Weekend'
    ELSE 'Weekday'
  END,
  dt.IsPeakHour
ORDER BY day_type, dt.IsPeakHour DESC;

/*
4. When is it cheaper to pre-heat or pre-cool the home?
Could costs be reduced by pre-heating (above 20 °C) before expensive hours
and letting the room slightly below 20 °C during peak price hours?
*/

/*
This needs already some model training or heuristic heating scheduling to
compare the cost of predicted heating schedule vs actual heating pattern.
*/

/*
5. During the coldest days in December 2024 – January 2025,
how much more energy did the heat pump need to keep 20 °C indoors?
*/

-- Q7
-- Energy usage per temperature bucket and per month
WITH bucketed AS (
  SELECT
    dt.Year,
    dt.Month,
    5*FLOOR(f.OutdoorTemp/5.0) AS bin_start,
    f.ASHP_Power
  FROM FACT_HEATING_ENERGY_USAGE f
  JOIN DIM_TIME dt   ON f.TimeKey = dt.TimeKey
  JOIN DIM_DEVICE dd ON f.DeviceKey = dd.DeviceKey
  WHERE f.OutdoorTemp >= -25
    AND f.OutdoorTemp < 30
    AND f.ASHP_Power IS NOT NULL
    AND dd.Model = 'Daikin_123'
)
SELECT
  Year,
  Month,
  bin_start,
  bin_start + 5 AS bin_end,
  SUM(ASHP_Power)/1000.0 AS energy_kwh,  -- assuming hourly rows
  COUNT(*)                AS hours
FROM bucketed
GROUP BY Year, Month, bin_start
ORDER BY Year, Month, bin_start;

/*
6. Based on December–January weather and price data,
what would be the expected savings?
*/

/*
This needs already some model training or heuristic heating scheduling to
compare the cost of predicted heating schedule vs actual heating pattern.
*/

/*
7. How much of the day is the A/C unit actually running
or idling at minimum power consumption?
Under what conditions could the pump be turned off completely?
*/

-- Q8
-- Hours per day here the avarage power this hour was less than devices min power
SELECT
  dt.Year,
  dt.Month,
  dt.Day,
  COUNT(*) AS hours_below_min
FROM FACT_HEATING_ENERGY_USAGE f
JOIN DIM_TIME dt   ON f.TimeKey   = dt.TimeKey
JOIN DIM_DEVICE dd ON f.DeviceKey = dd.DeviceKey
WHERE f.ASHP_Power IS NOT NULL
  AND dd.MinPower IS NOT NULL
  AND f.ASHP_Power < dd.MinPower
  AND dd.Model = 'Daikin_123'
GROUP BY dt.Year, dt.Month, dt.Day
ORDER BY 1,2,3;