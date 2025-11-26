-- ClickHouse S3/Parquet Table (Read-Only from MinIO)

-- This creates a read-only view of the Parquet files stored in MinIO.
-- ClickHouse reads the Parquet files directly using the S3 table function.

-- NOTE: This table should be created AFTER the first DAG run populates
-- the Parquet files in MinIO.

-- Create a view that reads all parquet files from the bronze IoT path
CREATE OR REPLACE VIEW default.bronze_iot_iceberg_readonly AS
SELECT *
FROM s3(
    'http://minio:9000/warehouse/bronze/iot_raw_iceberg/data/**/*.parquet',
    'minioadmin',
    'minioadmin',
    'Parquet'
);

-- Grant SELECT to analyst roles (run after roles are created)
-- GRANT SELECT ON default.bronze_iot_iceberg_readonly TO analyst_full;
-- GRANT SELECT ON default.bronze_iot_iceberg_readonly TO analyst_limited;
