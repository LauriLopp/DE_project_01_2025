-- Grant roles to users
GRANT analyst_full TO user_full;
GRANT analyst_limited TO user_limited;


GRANT SELECT *
    ON default.v_heating_energy_full_access
    TO analyst_full;

GRANT SELECT *
    ON default.v_heating_energy_limited_access
    TO analyst_limited;