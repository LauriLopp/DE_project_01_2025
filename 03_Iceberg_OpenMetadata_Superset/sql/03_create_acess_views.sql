-- Full access view: exposes all columns
CREATE OR REPLACE VIEW v_heating_energy_full_access AS
SELECT *
FROM default.fact_heating_energy_usage;

-- Limited access view: exposes only selected columns 
-- ToDo: STILL A DRAFT!!!

CREATE OR REPLACE VIEW v_heating_energy_limited_access AS
SELECT
    TimeKey,
    DeviceKey,
    LocationKey,
    ElectricityPrice,
    ASHP_Power,
    Boiler_Power,
    Air_Drier_Power,
    IndoorTemp,
    OutdoorTemp,
    DewPoint,
    CloudCoverage,
    UV_Index,
    AirPressure_mmHg,
    WindDir,
    WindGustSpeed_ms,
    WindSpeed_ms
FROM default.fact_heating_energy_usage;
