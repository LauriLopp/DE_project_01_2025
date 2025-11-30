-- =============================================================================
-- GRANT STATEMENTS FOR ANALYST ROLES
-- =============================================================================
-- This file grants SELECT permissions on gold tables and views to analyst roles.
-- analyst_full: Full access to all gold tables and views
-- analyst_limited: Access only to pseudonymized view (no direct fact table access)
-- =============================================================================

-- -----------------------------------------------------------------------------
-- GRANTS FOR analyst_full ROLE
-- Full access to all dimension tables, fact tables, and views
-- -----------------------------------------------------------------------------

-- Dimension tables
GRANT SELECT ON default.dim_device TO analyst_full;
GRANT SELECT ON default.dim_location TO analyst_full;
GRANT SELECT ON default.dim_time TO analyst_full;

-- Fact table (full access)
GRANT SELECT ON default.fact_heating_energy_usage TO analyst_full;

-- Full access view
GRANT SELECT ON default.v_heating_energy_full_access TO analyst_full;

-- Iceberg/Bronze view (for price data exploration)
GRANT SELECT ON default.bronze_elering_iceberg_readonly TO analyst_full;


-- -----------------------------------------------------------------------------
-- GRANTS FOR analyst_limited ROLE
-- Access to dimension tables and pseudonymized view only
-- NO direct access to fact table (must use pseudonymized view)
-- -----------------------------------------------------------------------------

-- Dimension tables (needed for JOINs)
GRANT SELECT ON default.dim_device TO analyst_limited;
GRANT SELECT ON default.dim_location TO analyst_limited;
GRANT SELECT ON default.dim_time TO analyst_limited;

-- Pseudonymized view (instead of direct fact table access)
GRANT SELECT ON default.v_heating_energy_limited_access TO analyst_limited;

-- Iceberg/Bronze view (price data is not sensitive)
GRANT SELECT ON default.bronze_elering_iceberg_readonly TO analyst_limited;

-- NOTE: analyst_limited does NOT have SELECT on fact_heating_energy_usage
-- They must use v_heating_energy_limited_access which pseudonymizes:
--   1. ElectricityPrice -> ElectricityPrice_Range (0.5€ buckets)
--   2. WC_Temp -> WC_Temp_Range (5°C buckets)  
--   3. WeatherCondition -> SHA256 hash with salt
