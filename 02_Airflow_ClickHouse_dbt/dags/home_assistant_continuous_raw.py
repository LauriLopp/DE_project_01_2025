from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow_clickhouse_plugin.hooks.clickhouse import ClickHouseHook
from airflow.providers.http.hooks.http import HttpHook
from datetime import datetime, timezone
import json

# --- CONFIGURATION ---
# The IoT sensor entity IDs
HA_IOT_ENTITY_IDS = [
    "sensor.tempniiskuslauaall_temperature",
    "sensor.tempniiskuslauaall_humidity",
    "sensor.ohksoojus_power",
    "sensor.indoor_absolute_humidity",
    "sensor.ohukuivati_power"
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

    # Ingestion tasks to fetch and load data
    task_fetch_iot = PythonOperator(task_id="fetch_iot_history", python_callable=fetch_iot_history)
    task_fetch_price = PythonOperator(task_id="fetch_elering_history", python_callable=fetch_elering_history)
    task_fetch_weather = PythonOperator(task_id="fetch_weather_history", python_callable=fetch_weather_history)

    # Define dependencies: each stream is independent
    task_setup_iot >> task_fetch_iot
    task_setup_price >> task_fetch_price
    task_setup_weather >> task_fetch_weather