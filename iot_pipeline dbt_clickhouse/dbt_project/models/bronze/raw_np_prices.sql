{{ config(
    materialized='table',
    engine='File',
    file='/var/lib/clickhouse/user_files/raw_np_prices.csv',
    format='CSVWithNames',
    options={'format_csv_delimiter': ';'}
) }}

SELECT
    raw_period,
    raw_price_with_vat,
    raw_price_without_vat
FROM file(
    '/var/lib/clickhouse/user_files/raw_np_prices.csv',
    'CSVWithNames',
    'raw_period String, raw_price_with_vat String, raw_price_without_vat String'
);
