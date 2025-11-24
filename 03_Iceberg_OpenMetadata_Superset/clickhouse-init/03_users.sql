/* App user */
CREATE USER IF NOT EXISTS airflow IDENTIFIED BY 'supersecret'
    SETTINGS connection_limit = 100;



-- Create users for analysts
CREATE USER IF NOT EXISTS user_analyst_full
    IDENTIFIED BY 'strong_password_full';
CREATE USER IF NOT EXISTS user_analyst_limited 
    IDENTIFIED BY 'strong_password_limited';


-- Grant roles to users
GRANT project_rw TO airflow;
GRANT analyst_full TO user_analyst_full;
GRANT analyst_limited TO user_analyst_limited;