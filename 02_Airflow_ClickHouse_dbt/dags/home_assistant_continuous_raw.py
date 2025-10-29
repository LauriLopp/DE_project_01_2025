from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow_clickhouse_plugin.hooks.clickhouse import ClickHouseHook
from airflow.providers.http.hooks.http import HttpHook
from datetime import datetime, timedelta, timezone

import json

ENTITY_IDS = [
    "sensor.tempniiskuslauaall_temperature",
    "sensor.tempniiskuslauaall_humidity",
    "sensor.ohksoojus_power",
    "sensor.indoor_absolute_humidity",
    "sensor.ohukuivati_power"
]

def create_bronze_raw_table():
    """
    Creates the table for raw, continuous data.
    """
    hook = ClickHouseHook(clickhouse_conn_id="clickhouse_default")
    
    # 
    # The get_conn() method returns a raw 'Client' object from the driver.
    # This object has a direct .execute() method, it does not use cursors.
    conn = hook.get_conn()
    
    sql = """
    CREATE TABLE IF NOT EXISTS bronze_iot_raw_data (
        ingestion_ts DateTime,
        entity_id String,
        state String,
        last_changed DateTime,
        attributes String
    ) ENGINE = MergeTree()
    ORDER BY (entity_id, last_changed)
    """
    conn.execute(sql) # We call .execute() directly on the connection object.


def fetch_and_load_continuous(**context):
    """
    Fetches the iot devices' values from Home Assistant API
    and stores it in ClickHouse.
    """
    start_dt = context["data_interval_start"]
    end_dt = context["data_interval_end"]

    print(f"--- Continuous Ingestion Run ---")
    print(f"Fetching data from: {start_dt.isoformat()}")
    print(f"Fetching data to:   {end_dt.isoformat()}")

    http_hook = HttpHook(method='GET', http_conn_id='home_assistant_api')
    ch_hook = ClickHouseHook(clickhouse_conn_id="clickhouse_default")
    ch = ch_hook.get_conn()  # raw clickhouse_driver.Client

    ha_conn = http_hook.get_connection(http_hook.http_conn_id)
    headers = {"Authorization": f"Bearer {ha_conn.password}"}

    endpoint = f"api/history/period/{start_dt.isoformat()}"
    api_params = {
        "filter_entity_id": ",".join(ENTITY_IDS),
        "end_time": end_dt.isoformat(),
    }

    resp = http_hook.run(endpoint=endpoint, data=api_params, headers=headers)
    history_data = resp.json()

    rows = []
    ingestion_ts = datetime.utcnow()  # naive UTC expected by CH driver

    for entity_history in history_data:
        for s in entity_history:
            ts = s.get("last_changed")
            # Make it naive UTC for ClickHouse
            if ts:
                dt = datetime.fromisoformat(ts.replace("Z", "+00:00")).astimezone(timezone.utc).replace(tzinfo=None)
            else:
                dt = None
            rows.append((
                ingestion_ts,
                s.get("entity_id"),
                s.get("state"),
                dt,
                json.dumps(s.get("attributes", {})),
            ))

    if rows:
        print(f"Inserting {len(rows)} records for this hour...")
        ch.execute(
            "INSERT INTO bronze_iot_raw_data "
            "(ingestion_ts, entity_id, state, last_changed, attributes) VALUES",
            rows
        )
        print("Success.")
    else:
        print("No state changes occurred during this interval.")


def fetch_and_load_elering_price(**context):
    """
    Fetch current EE electricity price (EUR/MWh) from Elering and store it.
    """
    http = HttpHook(method="GET", http_conn_id="elering_api")
    ch = ClickHouseHook(clickhouse_conn_id="clickhouse_default").get_conn()

    # Call API (no auth needed)
    resp = http.run(endpoint="/api/nps/price/EE/current")
    payload = resp.json()

    rec = payload["data"][0]                       # {'timestamp': 1761686100, 'price': 78.15}
    ts_utc = datetime.utcfromtimestamp(rec["timestamp"])  # naive UTC
    price = float(rec["price"])
    ingestion_ts = datetime.utcnow()

    # Create table if missing
    ch.execute("""
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

    # Insert single row
    ch.execute(
        "INSERT INTO bronze_elering_price "
        "(ingestion_ts, ts_utc, zone, currency, price_per_mwh) VALUES",
        [(ingestion_ts, ts_utc, "EE", "EUR", price)]
    )
    print(f"Inserted EE price {price} EUR/MWh @ {ts_utc.isoformat()}")

def fetch_and_load_weather_current(**context):
    from datetime import datetime, timezone
    import json
    from airflow.providers.http.hooks.http import HttpHook
    from airflow_clickhouse_plugin.hooks.clickhouse import ClickHouseHook

    http = HttpHook(method="GET", http_conn_id="home_assistant_api")
    ch = ClickHouseHook(clickhouse_conn_id="clickhouse_default").get_conn()

    # Create table if it doesn’t exist
    ch.execute("""
        CREATE TABLE IF NOT EXISTS bronze_weather_current (
            ingestion_ts        DateTime,
            entity_id           String,
            condition_state     String,
            last_changed        DateTime,
            temperature_C       Nullable(Float64),
            dew_point_C         Nullable(Float64),
            humidity_percent    Nullable(Float64),
            cloud_coverage_pct  Nullable(Float64),
            uv_index            Nullable(Float64),
            pressure_hPa        Nullable(Float64),
            wind_bearing_deg    Nullable(Float64),
            wind_gust_speed_kmh Nullable(Float64),
            wind_speed_kmh      Nullable(Float64)
        )
        ENGINE = MergeTree()
        ORDER BY (entity_id, last_changed)
    """)

    ha_conn = http.get_connection(http.http_conn_id)
    headers = {"Authorization": f"Bearer {ha_conn.password}"}

    # Pull full state for the weather entity
    resp = http.run(endpoint="api/states/weather.forecast_home", headers=headers)
    payload = resp.json()

    entity_id = payload.get("entity_id")
    condition_state = payload.get("state")

    ts = payload.get("last_changed")
    last_changed = (
        datetime.fromisoformat(ts.replace("Z", "+00:00"))
        .astimezone(timezone.utc).replace(tzinfo=None)
        if ts else None
    )

    attrs = payload.get("attributes", {})
    ingestion_ts = datetime.utcnow()

    # Extract numeric fields
    def f(key):
        v = attrs.get(key)
        try:
            return float(v) if v is not None else None
        except Exception:
            return None

    row = (
        ingestion_ts,
        entity_id,
        condition_state,
        last_changed,
        f("temperature"),
        f("dew_point"),
        f("humidity"),
        f("cloud_coverage"),
        f("uv_index"),
        f("pressure"),
        f("wind_bearing"),
        f("wind_gust_speed"),
        f("wind_speed"),
    )

    ch.execute("""
        INSERT INTO bronze_weather_current
        (ingestion_ts, entity_id, condition_state, last_changed,
         temperature_C, dew_point_C, humidity_percent, cloud_coverage_pct,
         uv_index, pressure_hPa, wind_bearing_deg,
         wind_gust_speed_kmh, wind_speed_kmh)
        VALUES
    """, [row])

    print(f"Inserted weather snapshot for {entity_id} @ {last_changed}")

with DAG(
    dag_id="home_assistant_continuous_raw",
    start_date=datetime(2025, 10, 20),
    schedule_interval="@hourly",
    catchup=False,
    max_active_runs=1,
    tags=['project2', 'ingestion', 'continuous', 'raw']
) as dag:

    create_table = PythonOperator(
        task_id="create_bronze_raw_table",
        python_callable=create_bronze_raw_table,
    )

    fetch_load = PythonOperator(
        task_id="fetch_and_load_continuous",
        python_callable=fetch_and_load_continuous,
    )

    fetch_price = PythonOperator(
        task_id="fetch_and_load_elering_price",
        python_callable=fetch_and_load_elering_price,
    )

    fetch_weather_current = PythonOperator(
        task_id="fetch_and_load_weather_current",
        python_callable=fetch_and_load_weather_current,
    )
    

    create_table >> fetch_load >> fetch_price >> fetch_weather_current