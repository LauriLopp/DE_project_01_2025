-- Runs only on the FIRST container start (empty data dir)
/* Roles & grants */
CREATE ROLE IF NOT EXISTS project_rw;
GRANT
    SELECT, INSERT, UPDATE, DELETE,
    CREATE, ALTER, SHOW DATABASES
ON default.* TO project_rw;

/* App user */
CREATE USER IF NOT EXISTS airflow IDENTIFIED BY 'supersecret';
GRANT project_rw TO airflow;
