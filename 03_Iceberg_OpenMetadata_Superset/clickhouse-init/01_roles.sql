-- Role for the pipeline
CREATE ROLE IF NOT EXISTS project_rw;

-- Create roles for analysts
CREATE ROLE IF NOT EXISTS analyst_full;
CREATE ROLE IF NOT EXISTS analyst_limited;