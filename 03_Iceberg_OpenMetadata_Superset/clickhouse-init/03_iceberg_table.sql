-- ClickHouse S3/Parquet View (Read-Only from MinIO)

-- This creates a read-only view of the Elering price Parquet files stored in MinIO.
-- ClickHouse reads the Parquet files directly using the S3 table function.
-- This is the single source of truth for electricity price data.

-- NOTE: This view is auto-created by the DAG task 'create_clickhouse_iceberg_view'
-- after each Iceberg write. This file is a backup for manual execution if needed.

-- Create a view that reads all parquet files from the bronze Elering price path
CREATE OR REPLACE VIEW default.bronze_elering_iceberg_readonly AS
SELECT 
    ingestion_ts,
    ts_utc,
    zone,
    currency,
    price_per_mwh
FROM s3(
    'http://minio:9000/warehouse/bronze/elering_price_iceberg/data/**/*.parquet',
    'minioadmin',
    'minioadmin',
    'Parquet'
);

-- Grant SELECT to analyst roles (run after roles are created)
-- GRANT SELECT ON default.bronze_elering_iceberg_readonly TO analyst_full;
-- GRANT SELECT ON default.bronze_elering_iceberg_readonly TO analyst_limited;
