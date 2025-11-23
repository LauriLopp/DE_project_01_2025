-- Remove the outdated view if it exists
DROP VIEW IF EXISTS bronze_elering_price_iceberg;

-- Create a simple VIEW that selects from the DAG-created IcebergS3 table
-- (This does NOT point to MinIO directly, it simply re-exports the correct table)
CREATE VIEW bronze_elering_price_iceberg AS
SELECT *
FROM bronze_elering_price;
