import pandas as pd
import pyarrow as pa

from pyiceberg.catalog import load_catalog
from pyiceberg.schema import Schema
from pyiceberg.types import NestedField, TimestampType, StringType, DoubleType
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow_clickhouse_plugin.hooks.clickhouse import ClickHouseHook
from airflow.providers.http.hooks.http import HttpHook
from datetime import datetime, timezone
import os
import json
import logging

DBT_PROJECT_DIR = "/opt/airflow/dbt"
# Route dbt logs and target to /tmp to avoid bind-mount permission issues on Windows
DBT_LOG_PATH = "/tmp/dbt-logs"
DBT_TARGET_PATH = "/tmp/dbt-target"
DBT_ENV = {
    "DBT_PROFILES_DIR": DBT_PROJECT_DIR,
    "DBT_LOG_PATH": DBT_LOG_PATH,
    "DBT_TARGET_PATH": DBT_TARGET_PATH,
    "PATH": f"/home/airflow/.local/bin:{os.environ.get('PATH', '')}",
}

HA_IOT_ENTITY_IDS = [
    # Power sensors
    "sensor.ohksoojus_power",                 # heat pump power
    "sensor.0xa4c138cdc6eff777_power",        # boiler power
    "sensor.ohukuivati_power",                # air drier power

    # Humidity (absolute and relative)
    "sensor.abshumidkuu2_absolute_humidity", # living room absolute humidity
    "sensor.tempniiskuslauaall_humidity",    # living room relative humidity
    "sensor.indoor_absolute_humidity",       # wc absolute humidity

    # Temperatures
    "sensor.tempniiskuslauaall_temperature", # living room temperature
    "sensor.indoor_outdoor_meter_3866",      # wc temperature

    # Voltage sensors
    "sensor.0xa4c138cdc6eff777_voltage"     # boiler voltage    

]
# The specific entity ID for weather history
HA_WEATHER_ENTITY_ID = "weather.forecast_home"


# --- DATABASE SETUP TASKS ---
def setup_bronze_elering_table():
    """
    Creates a READ-ONLY ClickHouse table that maps to the Iceberg table on MinIO.
    Name stays bronze_elering_price so dbt doesn't change.
    """
    ch_conn = ClickHouseHook(clickhouse_conn_id="clickhouse_default").get_conn()

    ch_conn.execute("""
        DROP TABLE IF EXISTS bronze_elering_price
    """)

    ch_conn.execute("""
        CREATE TABLE bronze_elering_price
        ENGINE = IcebergS3(
            'http://minio:9000/iceberg/warehouse/bronze/elering_price_iceberg',
            'minio',
            'minio123',
            'Parquet'
        )
    """)
    
def get_elering_iceberg_table():
    """
    Returns the Iceberg table for Elering prices, creating namespace/table if missing.

    Notes:
      - Nessie is configured with a named warehouse "warehouse"
        pointing to s3://iceberg/warehouse/
      - Bronze tables are created nullable (required=False) to match Pandas/Arrow default.
      - If you need to rebuild the table with a new schema, set:
            ICEBERG_RECREATE_ELERING=1
        in the Airflow container env and rerun once.
    """
    catalog = load_catalog(
        "bronze_catalog",
        **{
            "type": "rest",
            "uri": "http://nessie:19120/iceberg",

            # must be the Nessie warehouse NAME
            "warehouse": "warehouse",

            # MinIO / S3 props for PyIceberg FileIO
            "s3.endpoint": "http://minio:9000",
            "s3.access-key-id": "minio",
            "s3.secret-access-key": "minio123",
            "s3.region": "us-east-1",
            "s3.path-style-access": "true",

            # safety: allow automatic ns->us downcast if any sneaks through
            "downcast-ns-timestamp-to-us-on-write": "true",
        },
    )

    # ensure namespace exists
    existing_namespaces = {ns[0] for ns in catalog.list_namespaces()}
    if "bronze" not in existing_namespaces:
        catalog.create_namespace("bronze")

    table_id = "bronze.elering_price_iceberg"
    table_location = "s3://iceberg/warehouse/bronze/elering_price_iceberg"

    # optional one-time rebuild if schema changed
    if os.environ.get("ICEBERG_RECREATE_ELERING") == "1" and catalog.table_exists(table_id):
        catalog.drop_table(table_id)

    if not catalog.table_exists(table_id):
        schema = Schema(
            NestedField(field_id=1, name="ingestion_ts", field_type=TimestampType(), required=False),
            NestedField(field_id=2, name="ts_utc", field_type=TimestampType(), required=False),
            NestedField(field_id=3, name="zone", field_type=StringType(), required=False),
            NestedField(field_id=4, name="currency", field_type=StringType(), required=False),
            NestedField(field_id=5, name="price_per_mwh", field_type=DoubleType(), required=False),
        )

        catalog.create_table(
            table_id,
            schema=schema,
            location=table_location,
            properties={
                "format-version": "2",
                "write.format.default": "parquet",
            },
        )
    tbl = catalog.load_table(table_id)
    logging.info(f"[Elering Iceberg] Active table location: {tbl.location()}")
    return tbl


def setup_bronze_iot_table():
    """Creates the table for raw Home Assistant IoT sensor data."""
    ch_conn = ClickHouseHook(clickhouse_conn_id="clickhouse_default").get_conn()
    ch_conn.execute("""
        CREATE TABLE IF NOT EXISTS bronze_iot_raw_data (
            ingestion_ts DateTime,
            entity_id String,
            state String,
            last_changed DateTime,
            attributes String
        ) ENGINE = MergeTree()
        ORDER BY (entity_id, last_changed)
    """)

def setup_bronze_price_table():
    """Creates a VIEW for hourly Elering price data that reads Iceberg from MinIO."""
    ch_conn = ClickHouseHook(clickhouse_conn_id="clickhouse_default").get_conn()

    active_uuid_folder = "elering_price_iceberg_52af5092-7dab-491c-beb2-77ba954f00f7"
    iceberg_url = (
        f"http://minio:9000/iceberg/warehouse/bronze/{active_uuid_folder}"
    )

    # Drop any previous local table or view with that name
    ch_conn.execute("DROP TABLE IF EXISTS bronze_elering_price")
    ch_conn.execute("DROP VIEW IF EXISTS bronze_elering_price")

    # Create view pointing to Iceberg on MinIO
    ch_conn.execute(f"""
        CREATE VIEW bronze_elering_price AS
        SELECT *
        FROM iceberg(
            '{iceberg_url}',
            'minio',
            'minio123',
            'Parquet'
        )
    """)

def setup_bronze_weather_table():
    """Creates the new, separate table for historical weather data."""
    ch_conn = ClickHouseHook(clickhouse_conn_id="clickhouse_default").get_conn()
    ch_conn.execute("""
        CREATE TABLE IF NOT EXISTS bronze_weather_history (
            ingestion_ts        DateTime,
            entity_id           String,
            condition_state     String,
            last_changed        DateTime,
            temperature_C       Nullable(Float64),
            pressure_hPa        Nullable(Float64),
            humidity_percent    Nullable(Float64),
            wind_speed_kmh      Nullable(Float64),
            wind_bearing_deg    Nullable(Float64),
            cloud_coverage_pct  Nullable(Float64),
            dew_point_C         Nullable(Float64),
            uv_index            Nullable(Float64),
            wind_gust_speed_kmh Nullable(Float64)
        )
        ENGINE = MergeTree()
        ORDER BY (entity_id, last_changed)
    """)


# --- DATA FETCHING TASKS ---

def fetch_iot_history(**context):
    """Fetches history for IoT SENSORS and loads into bronze_iot_raw_data."""
    start_dt = context["data_interval_start"]
    end_dt = context["data_interval_end"]
    print(f"--- IoT Ingestion: Fetching data from {start_dt.isoformat()} to {end_dt.isoformat()} ---")

    http_hook = HttpHook(method='GET', http_conn_id='home_assistant_api')
    ch_conn = ClickHouseHook(clickhouse_conn_id="clickhouse_default").get_conn()
    ha_conn = http_hook.get_connection(http_hook.http_conn_id)
    headers = {"Authorization": f"Bearer {ha_conn.password}"}

    endpoint = f"api/history/period/{start_dt.isoformat()}"
    api_params = {"filter_entity_id": ",".join(HA_IOT_ENTITY_IDS), "end_time": end_dt.isoformat()}

    resp = http_hook.run(endpoint=endpoint, data=api_params, headers=headers)
    history_data = resp.json()

    rows = []
    ingestion_ts = datetime.utcnow()
    for entity_history in history_data:
        for s in entity_history:
            ts = s.get("last_changed")
            dt = datetime.fromisoformat(ts.replace("Z", "+00:00")).astimezone(timezone.utc).replace(tzinfo=None) if ts else None
            rows.append((ingestion_ts, s.get("entity_id"), s.get("state"), dt, json.dumps(s.get("attributes", {}))))

    if rows:
        ch_conn.execute("INSERT INTO bronze_iot_raw_data VALUES", rows)
        print(f"Inserted {len(rows)} IoT records.")

def fetch_elering_history(**context):
    start_dt = context["data_interval_start"]
    end_dt = context["data_interval_end"]
    print(f"--- Elering Ingestion: Fetching data from {start_dt.isoformat()} to {end_dt.isoformat()} ---")

    http = HttpHook(method="GET", http_conn_id="elering_api")
    params = {"start": start_dt.isoformat(), "end": end_dt.isoformat()}

    # IMPORTANT: params must go as query string, not request body
    resp = http.run(
        endpoint="/api/nps/price",
        extra_options={"params": params},
    )

    print(f"Elering status={resp.status_code}")
    payload = resp.json()
    print(f"Elering success={payload.get('success')}, keys={list(payload.keys())}")

    if not payload.get("success") or not payload.get("data"):
        print("No Elering data returned.")
        return

    ee_data = payload["data"].get("ee", [])
    if not ee_data:
        print("No EE zone rows in payload.")
        return

    rows = []
    ingestion_ts = datetime.utcnow()
    for rec in ee_data:
        rows.append({
            "ingestion_ts": ingestion_ts,
            "ts_utc": datetime.utcfromtimestamp(rec["timestamp"]),
            "zone": "EE",
            "currency": "EUR",
            "price_per_mwh": float(rec["price"]),
        })

    df = pd.DataFrame(rows)

    # Iceberg via PyIceberg does NOT support ns precision; force microseconds.
    for col in ["ingestion_ts", "ts_utc"]:
        df[col] = (
            pd.to_datetime(df[col], utc=False)
              .dt.tz_localize(None)        # ensure naive timestamps
              .astype("datetime64[us]")    # downcast to microseconds
        )

    arrow_tbl = pa.Table.from_pandas(df, preserve_index=False)

    tbl = get_elering_iceberg_table()
    tbl.append(arrow_tbl)

    print(f"Wrote {len(df)} Elering rows to Iceberg on MinIO.")




def fetch_weather_history(**context):
    """Fetches history for the WEATHER entity and loads into bronze_weather_history."""
    start_dt = context["data_interval_start"]
    end_dt = context["data_interval_end"]
    print(f"--- Weather Ingestion: Fetching data from {start_dt.isoformat()} to {end_dt.isoformat()} ---")

    http_hook = HttpHook(method='GET', http_conn_id='home_assistant_api')
    ch_conn = ClickHouseHook(clickhouse_conn_id="clickhouse_default").get_conn()
    ha_conn = http_hook.get_connection(http_hook.http_conn_id)
    headers = {"Authorization": f"Bearer {ha_conn.password}"}

    endpoint = f"api/history/period/{start_dt.isoformat()}"
    api_params = {"filter_entity_id": HA_WEATHER_ENTITY_ID, "end_time": end_dt.isoformat()}

    resp = http_hook.run(endpoint=endpoint, data=api_params, headers=headers)
    history_data = resp.json()

    rows = []
    ingestion_ts = datetime.utcnow()

    def f(attrs, key):
        v = attrs.get(key)
        try: return float(v) if v is not None else None
        except (ValueError, TypeError): return None

    for entity_history in history_data:
        for s in entity_history:
            ts = s.get("last_changed")
            dt = datetime.fromisoformat(ts.replace("Z", "+00:00")).astimezone(timezone.utc).replace(tzinfo=None) if ts else None
            attrs = s.get("attributes", {})
            rows.append((
                ingestion_ts, s.get("entity_id"), s.get("state"), dt,
                f(attrs, "temperature"), f(attrs, "pressure"), f(attrs, "humidity"),
                f(attrs, "wind_speed"), f(attrs, "wind_bearing"), f(attrs, "cloud_coverage"),
                f(attrs, "dew_point"),
                f(attrs, "uv_index"),
                f(attrs, "wind_gust_speed")
            ))

    if rows:
        ch_conn.execute("INSERT INTO bronze_weather_history VALUES", rows)
        print(f"Inserted {len(rows)} weather records.")


# --- DAG DEFINITION ---
def create_device_and_location_tables():
    """
    Create and load static bronze_device and bronze_location tables from CSVs.
    """
    ch = ClickHouseHook(clickhouse_conn_id="clickhouse_default").get_conn()

    sql_device = """
    CREATE TABLE IF NOT EXISTS bronze_device
    ENGINE = MergeTree()
    ORDER BY tuple()
    AS
    SELECT *
    FROM file('/var/lib/clickhouse/user_files/device_data.csv', 'CSVWithNames')
    """

    sql_location = """
    CREATE TABLE IF NOT EXISTS bronze_location
    ENGINE = MergeTree()
    ORDER BY tuple()
    AS
    SELECT *
    FROM file('/var/lib/clickhouse/user_files/location_data.csv', 'CSVWithNames')
    """

    ch.execute(sql_device)
    ch.execute(sql_location)

    print("Loaded bronze_device and bronze_location from CSVs into schema.")

with DAG(
    dag_id="continuous_ingestion_pipeline",
    start_date=datetime(2025, 10, 20),
    schedule_interval="@hourly",
    catchup=False,
    max_active_runs=1,
    tags=['project2', 'ingestion']
) as dag:

    # Setup tasks to ensure tables exist
    task_setup_iot = PythonOperator(task_id="setup_iot_table", python_callable=setup_bronze_iot_table)
    task_setup_price = PythonOperator(task_id="setup_price_table", python_callable=setup_bronze_price_table)
    task_setup_weather = PythonOperator(task_id="setup_weather_table", python_callable=setup_bronze_weather_table)
    task_create_static_tables = PythonOperator(task_id="create_device_and_location_tables", python_callable=create_device_and_location_tables,
    )

    # Ingestion tasks to fetch and load data
    task_fetch_iot = PythonOperator(task_id="fetch_iot_history", python_callable=fetch_iot_history)
    task_fetch_price = PythonOperator(task_id="fetch_elering_history", python_callable=fetch_elering_history)
    task_fetch_weather = PythonOperator(task_id="fetch_weather_history", python_callable=fetch_weather_history)

    # dbt tasks to materialize bronze -> marts layers automatically
    task_dbt_debug = BashOperator(
        task_id="dbt_debug",
        bash_command=f"mkdir -p {DBT_LOG_PATH} {DBT_TARGET_PATH} && cd {DBT_PROJECT_DIR} && dbt debug",
        do_xcom_push=False,
        env=DBT_ENV,
    )

    task_dbt_deps = BashOperator(
        task_id="dbt_deps",
        bash_command=f"mkdir -p {DBT_LOG_PATH} {DBT_TARGET_PATH} && cd {DBT_PROJECT_DIR} && dbt deps",
        do_xcom_push=False,
        env=DBT_ENV,
    )

    task_dbt_seed = BashOperator(
        task_id="dbt_seed",
        bash_command=f"mkdir -p {DBT_LOG_PATH} {DBT_TARGET_PATH} && cd {DBT_PROJECT_DIR} && dbt seed",
        do_xcom_push=False,
        env=DBT_ENV,
    )

    task_dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=f"mkdir -p {DBT_LOG_PATH} {DBT_TARGET_PATH} && cd {DBT_PROJECT_DIR} && dbt run",
        do_xcom_push=False,
        env=DBT_ENV,
    )

    task_dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"mkdir -p {DBT_LOG_PATH} {DBT_TARGET_PATH} && cd {DBT_PROJECT_DIR} && dbt test",
        do_xcom_push=False,
        env=DBT_ENV,
    )

    # Define dependencies: each stream is independent, dbt runs after ingestion
    task_setup_iot >> task_fetch_iot
    task_setup_price >> task_fetch_price
    task_setup_weather >> task_fetch_weather
    [task_fetch_iot, task_fetch_price, task_fetch_weather, task_create_static_tables] >> task_dbt_debug
    task_dbt_debug >> task_dbt_deps >> task_dbt_seed >> task_dbt_run >> task_dbt_test
    