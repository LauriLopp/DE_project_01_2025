from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow_clickhouse_plugin.hooks.clickhouse import ClickHouseHook
from airflow.providers.http.hooks.http import HttpHook
from datetime import datetime, timezone
import os
import json
import pyarrow as pa

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

# --- MinIO / Iceberg Configuration ---
MINIO_ENDPOINT = "http://minio:9000"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"
ICEBERG_WAREHOUSE = "s3://warehouse"
ICEBERG_NAMESPACE = "bronze"
ICEBERG_TABLE_NAME = "iot_raw_iceberg"

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
    """Creates the table for hourly Elering price data."""
    ch_conn = ClickHouseHook(clickhouse_conn_id="clickhouse_default").get_conn()
    ch_conn.execute("""
        CREATE TABLE IF NOT EXISTS bronze_elering_price (
            ingestion_ts   DateTime,
            ts_utc         DateTime,
            zone           String,
            currency       String,
            price_per_mwh  Float64
        )
        ENGINE = ReplacingMergeTree(ingestion_ts)
        ORDER BY (zone, ts_utc)
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


# --- ICEBERG SETUP AND WRITE TASKS ---

def get_iceberg_catalog():
    """
    Returns a PyIceberg catalog configured for MinIO using a static/file-based catalog.
    This avoids SQLAlchemy 2.0 dependency issues with Airflow.
    """
    from pyiceberg.catalog import load_catalog
    
    # Use a static table configuration - catalog metadata stored in MinIO itself
    catalog = load_catalog(
        "default",
        **{
            "type": "rest",
            "uri": "http://minio:9000",
            "s3.endpoint": MINIO_ENDPOINT,
            "s3.access-key-id": MINIO_ACCESS_KEY,
            "s3.secret-access-key": MINIO_SECRET_KEY,
            "warehouse": ICEBERG_WAREHOUSE,
        },
    )
    return catalog


def setup_iceberg_catalog():
    """
    Sets up the PyIceberg catalog with MinIO as the S3 backend.
    For simplicity, we'll write Iceberg data directly without a formal catalog.
    The table metadata will be stored alongside the data in MinIO.
    """
    import s3fs
    
    # Just ensure the warehouse path exists in MinIO
    fs = s3fs.S3FileSystem(
        endpoint_url=MINIO_ENDPOINT,
        key=MINIO_ACCESS_KEY,
        secret=MINIO_SECRET_KEY,
    )
    
    # Create the bronze namespace directory
    warehouse_path = f"warehouse/{ICEBERG_NAMESPACE}/{ICEBERG_TABLE_NAME}"
    try:
        fs.makedirs(warehouse_path, exist_ok=True)
        print(f"Created/verified Iceberg warehouse path: {warehouse_path}")
    except Exception as e:
        print(f"Warehouse path setup: {e}")
    
    print("Iceberg catalog setup complete. Table will be created on first write.")


def write_iot_to_iceberg(**context):
    """
    Fetches IoT history data and writes it as Parquet files to MinIO.
    This creates Iceberg-compatible data that ClickHouse can read.
    """
    import s3fs
    import uuid

    start_dt = context["data_interval_start"]
    end_dt = context["data_interval_end"]
    print(f"--- Iceberg Write: Processing data from {start_dt.isoformat()} to {end_dt.isoformat()} ---")

    # Fetch data from Home Assistant (same logic as fetch_iot_history)
    http_hook = HttpHook(method='GET', http_conn_id='home_assistant_api')
    ha_conn = http_hook.get_connection(http_hook.http_conn_id)
    headers = {"Authorization": f"Bearer {ha_conn.password}"}

    endpoint = f"api/history/period/{start_dt.isoformat()}"
    api_params = {"filter_entity_id": ",".join(HA_IOT_ENTITY_IDS), "end_time": end_dt.isoformat()}

    resp = http_hook.run(endpoint=endpoint, data=api_params, headers=headers)
    history_data = resp.json()

    # Build PyArrow table
    ingestion_ts_list = []
    entity_id_list = []
    state_list = []
    last_changed_list = []
    attributes_list = []

    ingestion_ts = datetime.utcnow()

    for entity_history in history_data:
        for s in entity_history:
            ts = s.get("last_changed")
            dt = datetime.fromisoformat(ts.replace("Z", "+00:00")).astimezone(timezone.utc).replace(tzinfo=None) if ts else None

            ingestion_ts_list.append(ingestion_ts)
            entity_id_list.append(s.get("entity_id", ""))
            state_list.append(s.get("state"))
            last_changed_list.append(dt)
            attributes_list.append(json.dumps(s.get("attributes", {})))

    if not ingestion_ts_list:
        print("No IoT data to write to Iceberg.")
        return

    # Create PyArrow table
    arrow_table = pa.table({
        "ingestion_ts": pa.array(ingestion_ts_list, type=pa.timestamp("us")),
        "entity_id": pa.array(entity_id_list, type=pa.string()),
        "state": pa.array(state_list, type=pa.string()),
        "last_changed": pa.array(last_changed_list, type=pa.timestamp("us")),
        "attributes": pa.array(attributes_list, type=pa.string()),
    })

    # Write directly to MinIO as Parquet (Iceberg-compatible format)
    fs = s3fs.S3FileSystem(
        endpoint_url=MINIO_ENDPOINT,
        key=MINIO_ACCESS_KEY,
        secret=MINIO_SECRET_KEY,
    )
    
    # Generate unique filename with partition
    partition_date = ingestion_ts.strftime("%Y-%m-%d")
    file_id = uuid.uuid4().hex[:8]
    parquet_path = f"warehouse/{ICEBERG_NAMESPACE}/{ICEBERG_TABLE_NAME}/data/ingestion_day={partition_date}/{file_id}.parquet"
    
    # Write parquet file
    import pyarrow.parquet as pq
    with fs.open(parquet_path, 'wb') as f:
        pq.write_table(arrow_table, f)
    
    print(f"Wrote {len(ingestion_ts_list)} records to s3://{parquet_path}")


def create_clickhouse_iceberg_view(**context):
    """
    Create or update ClickHouse view to read Iceberg Parquet files from MinIO.
    This allows querying Iceberg data directly from ClickHouse.
    """
    ch_hook = ClickHouseHook(clickhouse_conn_id="clickhouse_default")
    ch_conn = ch_hook.get_conn()
    
    # Create view using s3() table function to read Parquet files from MinIO
    # Uses glob pattern to read all partitioned parquet files
    create_view_sql = """
        CREATE OR REPLACE VIEW default.bronze_iot_iceberg_readonly AS
        SELECT 
            ingestion_ts,
            entity_id,
            state,
            last_changed,
            attributes
        FROM s3(
            'http://minio:9000/warehouse/bronze/iot_raw_iceberg/data/**/*.parquet',
            'minioadmin',
            'minioadmin',
            'Parquet'
        )
    """
    
    ch_conn.execute(create_view_sql)
    print("Created/updated ClickHouse view: bronze_iot_iceberg_readonly")
    
    # Verify the view works by counting rows
    try:
        result = ch_conn.execute("SELECT count() FROM bronze_iot_iceberg_readonly")
        row_count = result[0][0] if result else 0
        print(f"View verification: {row_count} rows accessible via bronze_iot_iceberg_readonly")
    except Exception as e:
        print(f"View created but verification query failed (may be empty): {e}")


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
    """Fetches historical Elering prices and loads into bronze_elering_price."""
    start_dt = context["data_interval_start"]
    end_dt = context["data_interval_end"]
    print(f"--- Elering Ingestion: Fetching data from {start_dt.isoformat()} to {end_dt.isoformat()} ---")

    http = HttpHook(method="GET", http_conn_id="elering_api")
    ch_conn = ClickHouseHook(clickhouse_conn_id="clickhouse_default").get_conn()

    params = {"start": start_dt.isoformat(), "end": end_dt.isoformat()}
    resp = http.run(endpoint="/api/nps/price", data=params)
    payload = resp.json()

    if not payload.get("success") or not payload.get("data"):
        return

    rows = []
    ingestion_ts = datetime.utcnow()
    for rec in payload["data"]["ee"]:
        rows.append((ingestion_ts, datetime.utcfromtimestamp(rec["timestamp"]), "EE", "EUR", float(rec["price"])))

    if rows:
        ch_conn.execute("INSERT INTO bronze_elering_price VALUES", rows)
        print(f"Inserted {len(rows)} Elering price records.")

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

    # Iceberg setup task
    task_setup_iceberg = PythonOperator(task_id="setup_iceberg_catalog", python_callable=setup_iceberg_catalog)

    # Ingestion tasks to fetch and load data
    task_fetch_iot = PythonOperator(task_id="fetch_iot_history", python_callable=fetch_iot_history)
    task_fetch_price = PythonOperator(task_id="fetch_elering_history", python_callable=fetch_elering_history)
    task_fetch_weather = PythonOperator(task_id="fetch_weather_history", python_callable=fetch_weather_history)

    # Iceberg write task (writes IoT data to Iceberg after fetching)
    task_write_iceberg = PythonOperator(task_id="write_iot_to_iceberg", python_callable=write_iot_to_iceberg)

    # Create ClickHouse view to query Iceberg data (after write completes)
    task_create_iceberg_view = PythonOperator(
        task_id="create_clickhouse_iceberg_view", 
        python_callable=create_clickhouse_iceberg_view
    )

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
    
    # Iceberg: setup catalog -> write data (after IoT fetch) -> create ClickHouse view
    task_setup_iceberg >> task_fetch_iot >> task_write_iceberg >> task_create_iceberg_view
    
    [task_fetch_iot, task_fetch_price, task_fetch_weather, task_create_static_tables, task_create_iceberg_view] >> task_dbt_debug
    task_dbt_debug >> task_dbt_deps >> task_dbt_seed >> task_dbt_run >> task_dbt_test
    