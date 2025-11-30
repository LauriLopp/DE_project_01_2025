-- GRANTS FOR ANALYST ROLES
-- analyst_full: Full access to gold table dims and full access view
-- analyst_limited: Dims + pseudonymized view only (no direct fact table access)

-- analyst_full: Full access

-- Dimension tables
GRANT SELECT ON default.dim_device TO analyst_full;
GRANT SELECT ON default.dim_location TO analyst_full;
GRANT SELECT ON default.dim_time TO analyst_full;

-- Full access view
GRANT SELECT ON default.v_heating_energy_full_access TO analyst_full;

-- Iceberg/Bronze view (for price data exploration)
GRANT SELECT ON default.bronze_elering_iceberg_readonly TO analyst_full;

-- analyst_limited: Dims + pseudonymized view only
GRANT SELECT ON default.dim_device TO analyst_limited;
GRANT SELECT ON default.dim_location TO analyst_limited;
GRANT SELECT ON default.dim_time TO analyst_limited;

-- Pseudonymized view (instead of direct fact table access)
GRANT SELECT ON default.v_heating_energy_limited_access TO analyst_limited;

GRANT SELECT ON default.bronze_elering_iceberg_readonly TO analyst_limited;

-- NOTE: analyst_limited has NO direct access to fact_heating_energy_usage.
-- Must use v_heating_energy_limited_access (pseudonymized: price buckets, temp ranges, hashed conditions).
