"""
Backfill DAG - loads historical data for Superset.
Trigger manually, then disable.
"""
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.http.hooks.http import HttpHook
from airflow_clickhouse_plugin.hooks.clickhouse import ClickHouseHook
from datetime import datetime, timedelta, timezone
import os
import json
import requests
import asyncio
import aiohttp
import pyarrow as pa
import pyarrow.parquet as pq

# --- Configuration ---
MINIO_ENDPOINT = "http://minio:9000"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"
ICEBERG_WAREHOUSE = "s3://warehouse"
ICEBERG_NAMESPACE = "bronze"
ICEBERG_TABLE_NAME = "elering_price_iceberg"

# PyIceberg REST Catalog configuration - uses the iceberg-rest service
ICEBERG_CATALOG_CONFIG = {
    "uri": "http://iceberg-rest:8181",
    "s3.endpoint": MINIO_ENDPOINT,
    "s3.access-key-id": MINIO_ACCESS_KEY,
    "s3.secret-access-key": MINIO_SECRET_KEY,
    "warehouse": ICEBERG_WAREHOUSE,
}


def ensure_minio_bucket():
    """Create MinIO warehouse bucket if it doesn't exist.
    
    This is a self-healing fallback in case the createbuckets init container
    failed or the bucket was deleted after a volume reset.
    """
    import boto3
    from botocore.exceptions import ClientError
    
    s3 = boto3.client(
        's3',
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
    )
    
    bucket_name = 'warehouse'
    try:
        s3.head_bucket(Bucket=bucket_name)
        print(f"MinIO bucket '{bucket_name}' exists.")
    except ClientError as e:
        error_code = e.response.get('Error', {}).get('Code', '')
        if error_code in ('404', 'NoSuchBucket'):
            print(f"MinIO bucket '{bucket_name}' not found, creating...")
            s3.create_bucket(Bucket=bucket_name)
            print(f"Created MinIO bucket '{bucket_name}'.")
        else:
            raise


# Backfill range - adjust as needed
BACKFILL_START = datetime(2025, 10, 1, tzinfo=timezone.utc)
# NOTE: BACKFILL_END is now calculated at runtime inside each task function
# to ensure it reflects the actual execution time, not DAG parse time.
# This prevents the backfill from overlapping with data already ingested
# by the continuous DAG.

# Open-Meteo Historical API configuration (for weather backfill)
OPENMETEO_URL = "https://archive-api.open-meteo.com/v1/archive"
TARTU_LAT = 58.366
TARTU_LON = 26.726

# dbt configuration
DBT_PROJECT_DIR = "/opt/airflow/dbt"
DBT_LOG_PATH = "/tmp/dbt-logs"
DBT_TARGET_PATH = "/tmp/dbt-target"
DBT_ENV = {
    "DBT_PROFILES_DIR": DBT_PROJECT_DIR,
    "DBT_LOG_PATH": DBT_LOG_PATH,
    "DBT_TARGET_PATH": DBT_TARGET_PATH,
    "PATH": f"/home/airflow/.local/bin:{os.environ.get('PATH', '')}",
}

# IoT sensors to backfill (must match entity_ids in HA)
HA_IOT_ENTITY_IDS = [
    "sensor.ohksoojus_power",
    "sensor.0xa4c138cdc6eff777_power",
    "sensor.ohukuivati_power",
    "sensor.abshumidkuu2_absolute_humidity",
    "sensor.tempniiskuslauaall_humidity",
    "sensor.indoor_absolute_humidity",
    "sensor.tempniiskuslauaall_temperature",
    "sensor.indoor_outdoor_meter_3866",
    "sensor.0xa4c138cdc6eff777_voltage",
]

HA_WEATHER_ENTITY_ID = "weather.forecast_home"


def _get_backfill_end():
    """Calculate BACKFILL_END at runtime to avoid DAG parse-time issues.
    
    Returns the start of the previous hour to avoid overlap with continuous DAG.
    E.g., if called at 14:45 UTC, returns 13:00 UTC.
    """
    now = datetime.now(timezone.utc)
    return now.replace(minute=0, second=0, microsecond=0) - timedelta(hours=1)


def backfill_elering_to_iceberg(**context):
    """Fetch historical Elering prices and write to Iceberg table using PyIceberg REST catalog.
    
    IDEMPOTENT: Deletes existing data in the backfill range before inserting.
    """
    from pyiceberg.catalog import load_catalog
    from pyiceberg.expressions import GreaterThanOrEqual, LessThan, And
    
    # Ensure MinIO bucket exists before any Iceberg operations
    ensure_minio_bucket()
    
    backfill_end = _get_backfill_end()
    print(f"--- Backfilling Elering data from {BACKFILL_START} to {backfill_end} ---")
    
    http = HttpHook(method="GET", http_conn_id="elering_api")
    params = {
        "start": BACKFILL_START.strftime("%Y-%m-%dT%H:%M:%S.000Z"),
        "end": backfill_end.strftime("%Y-%m-%dT%H:%M:%S.000Z")
    }
    resp = http.run(endpoint="/api/nps/price", data=params)
    payload = resp.json()

    if not payload.get("success") or not payload.get("data"):
        print("No Elering data returned")
        return

    # Build lists for PyArrow table
    ingestion_ts_list = []
    ts_utc_list = []
    zone_list = []
    currency_list = []
    price_list = []
    
    ingestion_ts = datetime.utcnow()
    
    for rec in payload["data"].get("ee", []):
        ts_utc = datetime.utcfromtimestamp(rec["timestamp"])
        ingestion_ts_list.append(ingestion_ts)
        ts_utc_list.append(ts_utc)
        zone_list.append("EE")
        currency_list.append("EUR")
        price_list.append(float(rec["price"]))

    if not ingestion_ts_list:
        print("No Elering price records to write.")
        return

    # Create PyArrow table
    arrow_table = pa.table({
        "ingestion_ts": pa.array(ingestion_ts_list, type=pa.timestamp("us")),
        "ts_utc": pa.array(ts_utc_list, type=pa.timestamp("us")),
        "zone": pa.array(zone_list, type=pa.string()),
        "currency": pa.array(currency_list, type=pa.string()),
        "price_per_mwh": pa.array(price_list, type=pa.float64()),
    })

    # Connect to Iceberg catalog and write data
    print("Connecting to Iceberg REST catalog...")
    catalog = load_catalog("rest", **ICEBERG_CATALOG_CONFIG)
    table_identifier = f"{ICEBERG_NAMESPACE}.{ICEBERG_TABLE_NAME}"
    
    try:
        table = catalog.load_table(table_identifier)
        
        # IDEMPOTENCY: Delete existing data in backfill range before inserting
        start_ts = BACKFILL_START.replace(tzinfo=None)
        end_ts = backfill_end.replace(tzinfo=None) if backfill_end.tzinfo else backfill_end
        
        try:
            delete_filter = And(
                GreaterThanOrEqual("ts_utc", start_ts),
                LessThan("ts_utc", end_ts)
            )
            table.delete(delete_filter)
            print(f"Deleted existing Iceberg data for backfill range {start_ts} to {end_ts}")
        except Exception as e:
            print(f"Delete step skipped (table may be empty): {e}")
        
        table.append(arrow_table)
        print(f"✅ Backfilled {len(ingestion_ts_list)} Elering price records to Iceberg table: {table_identifier}")
    except Exception as e:
        print(f"⚠️ Error writing to Iceberg table: {e}")
        print("  Table may need to be created first by the continuous DAG's setup_iceberg_catalog task")
        raise


def backfill_iot_from_statistics(**context):
    """Fetch IoT stats via HA WebSocket API, fallback to REST if unavailable."""
    backfill_end = _get_backfill_end()
    print(f"--- Backfilling IoT data from {BACKFILL_START} to {backfill_end} using WebSocket Statistics API ---")
    
    # Run the async function with the runtime-calculated end time
    asyncio.run(_backfill_iot_websocket(backfill_end))


async def _backfill_iot_websocket(backfill_end):
    """Async WebSocket fetch for HA long-term statistics.
    
    Args:
        backfill_end: The end datetime for the backfill period (calculated at runtime).
    """
    from airflow.providers.http.hooks.http import HttpHook
    
    # Get HA connection details from Airflow
    http_hook = HttpHook(method='GET', http_conn_id='home_assistant_api')
    ha_conn = http_hook.get_connection(http_hook.http_conn_id)
    
    # Build WebSocket URL from HTTP connection
    # ha_conn.host might be like "homeassistant.tail82b62.ts.net:8123" or include http://
    ha_host = ha_conn.host
    if ha_host.startswith("http://"):
        ha_host = ha_host[7:]
    elif ha_host.startswith("https://"):
        ha_host = ha_host[8:]
    
    # Check if port is already in the host string (e.g., "host:8123")
    if ":" in ha_host:
        # Port already included in host
        ws_url = f"ws://{ha_host}/api/websocket"
    else:
        # Add port
        ha_port = ha_conn.port or 8123
        ws_url = f"ws://{ha_host}:{ha_port}/api/websocket"
    
    token = ha_conn.password
    
    print(f"  Connecting to WebSocket: {ws_url}")
    
    ch_conn = ClickHouseHook(clickhouse_conn_id="clickhouse_default").get_conn()
    
    try:
        async with aiohttp.ClientSession() as session:
            async with session.ws_connect(ws_url) as ws:
                # Wait for auth_required message
                msg = await ws.receive_json()
                print(f"  Received: {msg.get('type')}")
                
                if msg.get("type") != "auth_required":
                    raise Exception(f"Expected auth_required, got {msg.get('type')}")
                
                # Authenticate
                await ws.send_json({"type": "auth", "access_token": token})
                msg = await ws.receive_json()
                print(f"  Auth response: {msg.get('type')}")
                
                if msg.get("type") != "auth_ok":
                    raise Exception(f"Authentication failed: {msg}")
                
                print("  ✅ WebSocket authenticated")
                
                # First, list available statistic IDs to verify sensors have statistics
                await ws.send_json({
                    "id": 1,
                    "type": "recorder/list_statistic_ids",
                    "statistic_type": "mean"
                })
                
                list_result = await ws.receive_json()
                if list_result.get("success"):
                    available_ids = [s.get("statistic_id") for s in list_result.get("result", [])]
                    matching = [eid for eid in HA_IOT_ENTITY_IDS if eid in available_ids]
                    missing = [eid for eid in HA_IOT_ENTITY_IDS if eid not in available_ids]
                    
                    print(f"  Found {len(matching)}/{len(HA_IOT_ENTITY_IDS)} sensors with statistics")
                    if missing:
                        print(f"  ⚠️  Missing statistics for: {missing}")
                    
                    if not matching:
                        print("  ❌ No sensors have long-term statistics, falling back to chunked API")
                        await ws.close()
                        _backfill_iot_chunked_sync(ch_conn, token, backfill_end)
                        return
                
                # Request statistics for all sensors
                await ws.send_json({
                    "id": 2,
                    "type": "recorder/statistics_during_period",
                    "start_time": BACKFILL_START.isoformat(),
                    "end_time": backfill_end.isoformat(),
                    "statistic_ids": HA_IOT_ENTITY_IDS,  # Must be JSON array
                    "period": "hour",
                    "types": ["mean", "min", "max", "sum", "state"]
                })
                
                result = await ws.receive_json()
                
                if not result.get("success"):
                    error = result.get("error", {})
                    print(f"  ❌ Statistics request failed: {error}")
                    await ws.close()
                    _backfill_iot_chunked_sync(ch_conn, token, backfill_end)
                    return
                
                statistics_data = result.get("result", {})
                print(f"  Received statistics for {len(statistics_data)} entities")
                
                rows = []
                ingestion_ts = datetime.utcnow()
                
                for entity_id, stats_list in statistics_data.items():
                    for stat in stats_list:
                        # Use 'start' as the timestamp for the hourly bucket
                        # HA Statistics API returns 'start' as Unix timestamp in MILLISECONDS
                        start_val = stat.get("start")
                        dt = None
                        if start_val is not None:
                            try:
                                if isinstance(start_val, (int, float)):
                                    # Check if timestamp is in milliseconds (13+ digits)
                                    # Unix seconds for year 2025 is ~1.7 billion (10 digits)
                                    # Milliseconds would be ~1.7 trillion (13 digits)
                                    if start_val > 1e12:  # Milliseconds
                                        dt = datetime.utcfromtimestamp(start_val / 1000.0)
                                    else:  # Seconds
                                        dt = datetime.utcfromtimestamp(start_val)
                                elif isinstance(start_val, str):
                                    # Fallback for ISO string format
                                    dt = datetime.fromisoformat(start_val.replace("Z", "+00:00"))
                                    dt = dt.astimezone(timezone.utc).replace(tzinfo=None)
                            except Exception as parse_err:
                                print(f"    ⚠️ Failed to parse timestamp {start_val}: {parse_err}")
                                continue  # Skip this record instead of adding with None dt
                        
                        # Skip records with no valid timestamp
                        if dt is None:
                            continue
                        
                        # Get the state value - prefer 'mean' for numeric sensors
                        # Use explicit None check to preserve 0 values (0 is falsy in Python)
                        mean_val = stat.get("mean")
                        state_val = stat.get("state")
                        if mean_val is not None:
                            state_value = mean_val
                        elif state_val is not None:
                            state_value = state_val
                        else:
                            state_value = ""
                        
                        # Build attributes JSON with all available statistics
                        attrs = {
                            "mean": stat.get("mean"),
                            "min": stat.get("min"),
                            "max": stat.get("max"),
                            "sum": stat.get("sum"),
                            "state": stat.get("state"),
                            "source": "long_term_statistics"
                        }
                        
                        rows.append((
                            ingestion_ts,
                            entity_id,
                            str(state_value),
                            dt,
                            json.dumps(attrs)
                        ))
                
                if rows:
                    # Delete existing data in backfill range to avoid duplicates
                    # Use < (exclusive end) to preserve continuous DAG's most recent data
                    start_str = BACKFILL_START.strftime("%Y-%m-%d %H:%M:%S")
                    end_str = backfill_end.strftime("%Y-%m-%d %H:%M:%S")
                    ch_conn.execute(f"""
                        ALTER TABLE bronze_iot_raw_data 
                        DELETE WHERE last_changed >= '{start_str}' AND last_changed < '{end_str}'
                    """)
                    ch_conn.execute("INSERT INTO bronze_iot_raw_data VALUES", rows)
                    print(f"✅ Inserted {len(rows)} IoT records from Long-Term Statistics API")
                else:
                    print("  No IoT statistics data returned, trying chunked fallback...")
                    _backfill_iot_chunked_sync(ch_conn, token, backfill_end)
                    
    except aiohttp.ClientError as e:
        print(f"  ❌ WebSocket connection failed: {e}")
        print("  Falling back to chunked REST API...")
        _backfill_iot_chunked_sync(ch_conn, token, backfill_end)
    except Exception as e:
        print(f"  ❌ Unexpected error: {e}")
        print("  Falling back to chunked REST API...")
        _backfill_iot_chunked_sync(ch_conn, token, backfill_end)


def _backfill_iot_chunked_sync(ch_conn, token, backfill_end):
    """Fallback: fetch IoT data via REST API in daily chunks (limited to ~10 days by HA).
    
    Args:
        ch_conn: ClickHouse connection.
        token: Home Assistant API token.
        backfill_end: The end datetime for the backfill period (calculated at runtime).
    """
    from airflow.providers.http.hooks.http import HttpHook
    
    # Validate token before proceeding
    if not token:
        print("  ❌ ERROR: No Home Assistant token available. Check Airflow connection 'home_assistant_api'.")
        return
    
    http_hook = HttpHook(method='GET', http_conn_id='home_assistant_api')
    ha_conn = http_hook.get_connection(http_hook.http_conn_id)
    
    # Use token from connection if passed token is invalid
    actual_token = token if token else ha_conn.password
    if not actual_token:
        print("  ❌ ERROR: No token found in Airflow connection. Cannot fetch IoT data.")
        return
        
    headers = {"Authorization": f"Bearer {actual_token}"}
    
    # Try full range - HA will return empty for dates beyond purge window
    current = BACKFILL_START.replace(tzinfo=None)
    end = backfill_end.replace(tzinfo=None)
    total_rows = 0
    
    print(f"  ⚠️  Chunked fallback: Fetching from {current.date()} to {end.date()}")
    print(f"      Note: HA typically only keeps ~10 days of history (purge_keep_days)")
    
    while current < end:
        next_day = current + timedelta(days=1)
        print(f"    Fetching IoT data for {current.date()}...")
        
        try:
            endpoint = f"api/history/period/{current.isoformat()}"
            api_params = {
                "filter_entity_id": ",".join(HA_IOT_ENTITY_IDS),
                "end_time": next_day.isoformat(),
                "minimal_response": "true",
                "significant_changes_only": "false"
            }
            resp = http_hook.run(endpoint=endpoint, data=api_params, headers=headers)
            history_data = resp.json()
            
            rows = []
            ingestion_ts = datetime.utcnow()
            
            for entity_history in history_data:
                for s in entity_history:
                    ts = s.get("last_changed")
                    if ts:
                        dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
                        dt = dt.astimezone(timezone.utc).replace(tzinfo=None)
                    else:
                        dt = None
                    
                    rows.append((
                        ingestion_ts,
                        s.get("entity_id"),
                        s.get("state"),
                        dt,
                        json.dumps(s.get("attributes", {}))
                    ))
            
            if rows:
                ch_conn.execute("INSERT INTO bronze_iot_raw_data VALUES", rows)
                total_rows += len(rows)
                print(f"      Inserted {len(rows)} records")
                
        except Exception as e:
            print(f"      Error fetching {current.date()}: {e}")
        
        current = next_day
    
    print(f"✅ Backfilled {total_rows} total IoT records via chunked API")


def backfill_weather_from_openmeteo(**context):
    """Fetch weather history from Open-Meteo API. UV index estimated from radiation."""
    # Calculate BACKFILL_END at runtime to avoid DAG parse-time issues
    backfill_end = _get_backfill_end()
    actual_end = backfill_end.replace(tzinfo=None)
    
    print(f"--- Backfilling Weather data from Open-Meteo API ---")
    print(f"    Location: Tartu ({TARTU_LAT}, {TARTU_LON})")
    print(f"    Period: {BACKFILL_START} to {backfill_end} (hour precision)")
    print(f"    Will skip hours >= {actual_end.isoformat()}")
    
    ch_conn = ClickHouseHook(clickhouse_conn_id="clickhouse_default").get_conn()
    
    # Delete only the backfill period - preserve any data outside this range
    # Use < (exclusive end) to avoid deleting continuous DAG's data at boundary
    backfill_start_str = BACKFILL_START.strftime("%Y-%m-%d %H:%M:%S")
    backfill_end_str = actual_end.strftime("%Y-%m-%d %H:%M:%S")
    print(f"  Deleting existing weather data in backfill range: {backfill_start_str} to {backfill_end_str} (exclusive)")
    ch_conn.execute(f"""
        ALTER TABLE bronze_weather_history 
        DELETE WHERE last_changed >= '{backfill_start_str}' AND last_changed < '{backfill_end_str}'
    """)
    
    # Open-Meteo API parameters
    # Request hourly data for all weather attributes we need
    hourly_params = [
        "temperature_2m",
        "surface_pressure",
        "relative_humidity_2m",
        "wind_speed_10m",
        "wind_direction_10m",
        "cloud_cover",
        "dew_point_2m",
        "shortwave_radiation",  # Used to estimate UV index
        "wind_gusts_10m"
    ]
    
    # Process in 90-day chunks (Open-Meteo recommendation for large requests)
    current = BACKFILL_START.replace(tzinfo=None)
    end = actual_end
    total_rows = 0
    ingestion_ts = datetime.utcnow()
    
    while current < end:
        chunk_end = min(current + timedelta(days=90), end)
        print(f"  Fetching {current.date()} to {chunk_end.date()}...")
        
        try:
            params = {
                "latitude": TARTU_LAT,
                "longitude": TARTU_LON,
                "start_date": current.strftime("%Y-%m-%d"),
                "end_date": chunk_end.strftime("%Y-%m-%d"),
                "hourly": ",".join(hourly_params),
                "timezone": "UTC"
            }
            
            resp = requests.get(OPENMETEO_URL, params=params, timeout=60)
            resp.raise_for_status()
            data = resp.json()
            
            hourly = data.get("hourly", {})
            times = hourly.get("time", [])
            
            if not times:
                print(f"    No data returned for this period")
                current = chunk_end
                continue
            
            rows = []
            skipped = 0
            for i, time_str in enumerate(times):
                # Parse timestamp
                dt = datetime.fromisoformat(time_str)
                
                # Skip hours beyond backfill_end to avoid overlap with continuous DAG
                if dt >= actual_end:
                    skipped += 1
                    continue
                
                # Get values (may be None)
                temp = hourly.get("temperature_2m", [])[i] if i < len(hourly.get("temperature_2m", [])) else None
                pressure = hourly.get("surface_pressure", [])[i] if i < len(hourly.get("surface_pressure", [])) else None
                humidity = hourly.get("relative_humidity_2m", [])[i] if i < len(hourly.get("relative_humidity_2m", [])) else None
                wind_speed = hourly.get("wind_speed_10m", [])[i] if i < len(hourly.get("wind_speed_10m", [])) else None
                wind_dir = hourly.get("wind_direction_10m", [])[i] if i < len(hourly.get("wind_direction_10m", [])) else None
                cloud = hourly.get("cloud_cover", [])[i] if i < len(hourly.get("cloud_cover", [])) else None
                dew_point = hourly.get("dew_point_2m", [])[i] if i < len(hourly.get("dew_point_2m", [])) else None
                radiation = hourly.get("shortwave_radiation", [])[i] if i < len(hourly.get("shortwave_radiation", [])) else None
                wind_gust = hourly.get("wind_gusts_10m", [])[i] if i < len(hourly.get("wind_gusts_10m", [])) else None
                
                # Estimate UV index from shortwave radiation (radiation / 25)
                uv_index = round(radiation / 25.0) if radiation is not None else None
                
                # Map Open-Meteo weather codes to HA-style condition strings
                # For simplicity, we'll use "unknown" since we don't have weather_code
                condition_state = "unknown"
                
                rows.append((
                    ingestion_ts,
                    HA_WEATHER_ENTITY_ID,  # "weather.forecast_home" for dbt compatibility
                    condition_state,
                    dt,
                    temp,           # temperature_C
                    pressure,       # pressure_hPa
                    humidity,       # humidity_percent
                    wind_speed,     # wind_speed_kmh
                    wind_dir,       # wind_bearing_deg
                    cloud,          # cloud_coverage_pct
                    dew_point,      # dew_point_C
                    uv_index,       # uv_index (estimated)
                    wind_gust       # wind_gust_speed_kmh
                ))
            
            if rows:
                ch_conn.execute("INSERT INTO bronze_weather_history VALUES", rows)
                total_rows += len(rows)
                print(f"    Inserted {len(rows)} records" + (f" (skipped {skipped} beyond backfill_end)" if skipped else ""))
                
        except requests.exceptions.RequestException as e:
            print(f"    Error fetching {current.date()} to {chunk_end.date()}: {e}")
        except Exception as e:
            print(f"    Unexpected error: {e}")
        
        current = chunk_end
    
    print(f"✅ Backfilled {total_rows} total weather records from Open-Meteo")


def refresh_clickhouse_iceberg_view(**context):
    """Refresh ClickHouse view to query Iceberg data from MinIO.
    
    Uses Iceberg table function first, falls back to s3() with explicit schema
    if no data exists yet.
    """
    ch_conn = ClickHouseHook(clickhouse_conn_id="clickhouse_default").get_conn()
    
    # Try using the native iceberg() function first (reads Iceberg metadata)
    iceberg_view_sql = f"""
        CREATE OR REPLACE VIEW default.bronze_elering_iceberg_readonly AS
        SELECT 
            ingestion_ts,
            ts_utc,
            zone,
            currency,
            price_per_mwh
        FROM iceberg(
            'http://minio:9000/warehouse/{ICEBERG_NAMESPACE}/{ICEBERG_TABLE_NAME}/',
            'minioadmin',
            'minioadmin'
        )
    """
    
    try:
        ch_conn.execute(iceberg_view_sql)
        print("Created/updated ClickHouse view using iceberg() function.")
    except Exception as e:
        print(f"iceberg() function failed: {e}")
        print("Falling back to s3() with explicit schema (for empty or new tables)...")
        
        # Fallback: Use s3() with explicit schema definition
        # This handles the case where no parquet files exist yet
        fallback_sql = f"""
            CREATE OR REPLACE VIEW default.bronze_elering_iceberg_readonly AS
            SELECT 
                ingestion_ts,
                ts_utc,
                zone,
                currency,
                price_per_mwh
            FROM s3(
                'http://minio:9000/warehouse/{ICEBERG_NAMESPACE}/{ICEBERG_TABLE_NAME}/data/**/*.parquet',
                'minioadmin',
                'minioadmin',
                'Parquet',
                'ingestion_ts Nullable(DateTime64(6)), ts_utc Nullable(DateTime64(6)), zone Nullable(String), currency Nullable(String), price_per_mwh Nullable(Float64)'
            )
            WHERE _file != ''
        """
        
        try:
            ch_conn.execute(fallback_sql)
            print("Created ClickHouse view using s3() with explicit schema.")
        except Exception as e2:
            # If even that fails (no files at all), create an empty view
            print(f"s3() fallback also failed: {e2}")
            print("Creating empty placeholder view...")
            empty_view_sql = """
                CREATE OR REPLACE VIEW default.bronze_elering_iceberg_readonly AS
                SELECT 
                    toDateTime64('1970-01-01 00:00:00', 6) AS ingestion_ts,
                    toDateTime64('1970-01-01 00:00:00', 6) AS ts_utc,
                    '' AS zone,
                    '' AS currency,
                    0.0 AS price_per_mwh
                WHERE 0
            """
            ch_conn.execute(empty_view_sql)
            print("Created empty placeholder view. Run backfill_elering first to populate data.")
            return
    
    try:
        result = ch_conn.execute("SELECT count() FROM bronze_elering_iceberg_readonly")
        print(f"✅ View refreshed. Total Elering records: {result[0][0]}")
    except Exception as e:
        print(f"View created but count query failed: {e}")


# --- DAG Definition ---
with DAG(
    dag_id="backfill_historical_data",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,  # Manual trigger only
    catchup=False,
    tags=['project3', 'backfill', 'one-time'],
    doc_md="""
    ## Historical Data Backfill DAG
    
    **Purpose**: Load historical data for Superset visualizations.
    
    **Usage**: Trigger manually once, then disable.
    
    **Data Sources**:
    - **Elering**: Full price history via API → MinIO/Iceberg
    - **IoT Sensors**: Long-Term Statistics API (hourly aggregates) → ClickHouse
    - **Weather**: Regular history API (chunked by day) → ClickHouse
    
    **After completion**: Runs dbt to refresh all models.
    
    **Configuration**: Edit `BACKFILL_START` and `BACKFILL_END` at the top of the file.
    """
) as dag:

    task_elering = PythonOperator(
        task_id="backfill_elering",
        python_callable=backfill_elering_to_iceberg,
        execution_timeout=timedelta(minutes=30),
    )

    task_iot = PythonOperator(
        task_id="backfill_iot",
        python_callable=backfill_iot_from_statistics,
        execution_timeout=timedelta(hours=2),
    )

    task_weather = PythonOperator(
        task_id="backfill_weather",
        python_callable=backfill_weather_from_openmeteo,
        execution_timeout=timedelta(hours=1),
    )

    task_refresh_view = PythonOperator(
        task_id="refresh_iceberg_view",
        python_callable=refresh_clickhouse_iceberg_view,
    )

    task_dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=f"mkdir -p {DBT_LOG_PATH} {DBT_TARGET_PATH} && cd {DBT_PROJECT_DIR} && dbt deps && dbt run",
        env=DBT_ENV,
    )

    task_dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt test",
        env=DBT_ENV,
    )

    # Run data backfills in parallel, then refresh view and run dbt
    [task_elering, task_iot, task_weather] >> task_refresh_view >> task_dbt_run >> task_dbt_test
