-- Grant all rights to pipeline role
GRANT
    SELECT, INSERT, DELETE,
    CREATE, ALTER, SHOW DATABASES
ON default.* TO project_rw;

-- Analyst rights will be granted later, after the access views are created