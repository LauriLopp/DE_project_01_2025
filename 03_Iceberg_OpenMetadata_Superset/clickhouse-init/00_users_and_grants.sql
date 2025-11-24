/* Currently commented out but kept. P2 worked with this code but P3 role creation did not. This file failed silently. */

/*
-- Runs only on the FIRST container start (empty data dir)
/* Roles & grants */
CREATE ROLE IF NOT EXISTS project_rw;

GRANT
    SELECT, INSERT, DELETE,
    CREATE, ALTER, SHOW DATABASES
ON default.* TO project_rw;



/* App user */
CREATE USER IF NOT EXISTS airflow IDENTIFIED BY 'supersecret'
    SETTINGS connection_limit = 100;

GRANT project_rw TO airflow;

*/