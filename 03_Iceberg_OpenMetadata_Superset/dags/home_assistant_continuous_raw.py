from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow_clickhouse_plugin.hooks.clickhouse import ClickHouseHook
from airflow.providers.http.hooks.http import HttpHook
from datetime import datetime, timezone
import os
import json
import requests
import base64
import time
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
ICEBERG_TABLE_NAME = "elering_price_iceberg"

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

# NOTE: bronze_elering_price table removed - Elering data now stored in MinIO/Iceberg only
# The ClickHouse view bronze_elering_iceberg_readonly reads from MinIO Parquet files

def setup_bronze_weather_table():
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

# PyIceberg REST Catalog configuration - uses the iceberg-rest service
ICEBERG_CATALOG_CONFIG = {
    "uri": "http://iceberg-rest:8181",
    "s3.endpoint": MINIO_ENDPOINT,
    "s3.access-key-id": MINIO_ACCESS_KEY,
    "s3.secret-access-key": MINIO_SECRET_KEY,
    "warehouse": ICEBERG_WAREHOUSE,
}


def ensure_minio_bucket():
    """Ensure MinIO 'warehouse' bucket exists (creates if missing)."""
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


def setup_iceberg_catalog():
    from pyiceberg.catalog import load_catalog
    from pyiceberg.schema import Schema
    from pyiceberg.types import TimestampType, StringType, DoubleType, NestedField
    from pyiceberg.partitioning import PartitionSpec
    from pyiceberg.transforms import DayTransform
    from pyiceberg.exceptions import NoSuchTableError
    
    # Ensure MinIO bucket exists before any Iceberg operations
    ensure_minio_bucket()
    
    print("Connecting to Iceberg REST catalog...")
    catalog = load_catalog("rest", **ICEBERG_CATALOG_CONFIG)
    
    # Create namespace if not exists
    namespaces = [ns[0] for ns in catalog.list_namespaces()]
    if ICEBERG_NAMESPACE not in namespaces:
        catalog.create_namespace(ICEBERG_NAMESPACE)
        print(f"Created namespace: {ICEBERG_NAMESPACE}")
    else:
        print(f"Namespace already exists: {ICEBERG_NAMESPACE}")
    
    # Define Iceberg schema for Elering price data
    table_identifier = f"{ICEBERG_NAMESPACE}.{ICEBERG_TABLE_NAME}"
    
    # Check if table exists by listing tables in the namespace
    existing_tables = [t[1] for t in catalog.list_tables(ICEBERG_NAMESPACE)]
    if ICEBERG_TABLE_NAME in existing_tables:
        print(f"Table already exists: {table_identifier}")
    else:
        # Create new table with proper Iceberg schema
        from pyiceberg.partitioning import PartitionField
        schema = Schema(
            NestedField(1, "ingestion_ts", TimestampType(), required=False),
            NestedField(2, "ts_utc", TimestampType(), required=False),
            NestedField(3, "zone", StringType(), required=False),
            NestedField(4, "currency", StringType(), required=False),
            NestedField(5, "price_per_mwh", DoubleType(), required=False),
        )
        
        # Partition by day on ts_utc
        partition_spec = PartitionSpec(
            PartitionField(source_id=2, field_id=1000, transform=DayTransform(), name="ts_day")
        )
        
        catalog.create_table(
            identifier=table_identifier,
            schema=schema,
            partition_spec=partition_spec,
            location=f"{ICEBERG_WAREHOUSE}/{ICEBERG_NAMESPACE}/{ICEBERG_TABLE_NAME}",
        )
        print(f"Created Iceberg table: {table_identifier}")
    
    print("Iceberg catalog setup complete.")


def write_elering_to_iceberg(**context):
    """IDEMPOTENT: Deletes existing rows for the interval, then appends fresh data."""
    from pyiceberg.catalog import load_catalog
    from pyiceberg.expressions import GreaterThanOrEqual, LessThan, And
    
    start_dt = context["data_interval_start"]
    end_dt = context["data_interval_end"]
    print(f"--- Iceberg Write (Elering): Processing data from {start_dt.isoformat()} to {end_dt.isoformat()} ---")

    # Fetch data from Elering API
    http = HttpHook(method="GET", http_conn_id="elering_api")
    params = {"start": start_dt.isoformat(), "end": end_dt.isoformat()}
    resp = http.run(endpoint="/api/nps/price", data=params)
    payload = resp.json()

    if not payload.get("success") or not payload.get("data"):
        print("No Elering price data to write to Iceberg.")
        return

    # Build lists for PyArrow table
    ingestion_ts_list = []
    ts_utc_list = []
    zone_list = []
    currency_list = []
    price_list = []

    ingestion_ts = datetime.utcnow()

    for rec in payload["data"]["ee"]:
        ts_utc = datetime.utcfromtimestamp(rec["timestamp"])
        ingestion_ts_list.append(ingestion_ts)
        ts_utc_list.append(ts_utc)
        zone_list.append("EE")
        currency_list.append("EUR")
        price_list.append(float(rec["price"]))

    if not ingestion_ts_list:
        print("No Elering price records to write.")
        return

    # Create PyArrow table with Elering price schema
    arrow_table = pa.table({
        "ingestion_ts": pa.array(ingestion_ts_list, type=pa.timestamp("us")),
        "ts_utc": pa.array(ts_utc_list, type=pa.timestamp("us")),
        "zone": pa.array(zone_list, type=pa.string()),
        "currency": pa.array(currency_list, type=pa.string()),
        "price_per_mwh": pa.array(price_list, type=pa.float64()),
    })

    # Connect to Iceberg catalog and write data
    catalog = load_catalog("rest", **ICEBERG_CATALOG_CONFIG)
    table_identifier = f"{ICEBERG_NAMESPACE}.{ICEBERG_TABLE_NAME}"
    
    table = catalog.load_table(table_identifier)
    
    # IDEMPOTENCY: Delete existing rows for this time interval before appending
    # Convert to naive UTC datetime for Iceberg filter (PyIceberg uses microseconds)
    start_ts = start_dt.replace(tzinfo=None) if start_dt.tzinfo else start_dt
    end_ts = end_dt.replace(tzinfo=None) if end_dt.tzinfo else end_dt
    
    try:
        # Use Iceberg's delete API to remove rows in this time range
        delete_filter = And(
            GreaterThanOrEqual("ts_utc", start_ts),
            LessThan("ts_utc", end_ts)
        )
        table.delete(delete_filter)
        print(f"Deleted existing Iceberg data for interval {start_ts} to {end_ts}")
    except Exception as e:
        # Delete may fail on empty table or if no matching rows - that's OK
        print(f"Delete step skipped (table may be empty or no matching rows): {e}")
    
    table.append(arrow_table)
    
    print(f"Wrote {len(ingestion_ts_list)} Elering price records to Iceberg table: {table_identifier}")


def create_clickhouse_iceberg_view(**context):
    ch_hook = ClickHouseHook(clickhouse_conn_id="clickhouse_default")
    ch_conn = ch_hook.get_conn()
    
    # Use ClickHouse's native iceberg() table function to read from Iceberg metadata
    # This reads the proper Iceberg table format with snapshots and manifests
    create_view_sql = f"""
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
        ch_conn.execute(create_view_sql)
        print("Created/updated ClickHouse view using iceberg() function: bronze_elering_iceberg_readonly")
    except Exception as e:
        # Fallback to s3() if iceberg() function fails (e.g., metadata not compatible)
        print(f"iceberg() function failed: {e}")
        print("Falling back to s3() glob pattern...")
        
        fallback_sql = f"""
            CREATE OR REPLACE VIEW default.bronze_elering_iceberg_readonly AS
            SELECT 
                ingestion_ts,
                ts_utc,
                zone,
                currency,
                price_per_mwh
            FROM s3(
                'http://minio:9000/warehouse/{ICEBERG_NAMESPACE}/{ICEBERG_TABLE_NAME}/data/*.parquet',
                'minioadmin',
                'minioadmin',
                'Parquet'
            )
        """
        ch_conn.execute(fallback_sql)
        print("Created/updated ClickHouse view using s3() fallback: bronze_elering_iceberg_readonly")
    
    # Verify the view works by counting rows
    try:
        result = ch_conn.execute("SELECT count() FROM bronze_elering_iceberg_readonly")
        row_count = result[0][0] if result else 0
        print(f"View verification: {row_count} rows accessible via bronze_elering_iceberg_readonly")
    except Exception as e:
        print(f"View created but verification query failed (may be empty): {e}")


# --- DATA FETCHING TASKS ---

def fetch_iot_history(**context):
    """IDEMPOTENT: Deletes existing rows for the interval, then inserts fresh data."""
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
        # IDEMPOTENCY: Delete existing data for this time interval before inserting
        start_str = start_dt.strftime("%Y-%m-%d %H:%M:%S")
        end_str = end_dt.strftime("%Y-%m-%d %H:%M:%S")
        ch_conn.execute(f"ALTER TABLE bronze_iot_raw_data DELETE WHERE last_changed >= '{start_str}' AND last_changed < '{end_str}'")
        print(f"Deleted existing IoT data for interval {start_str} to {end_str}")
        
        ch_conn.execute("INSERT INTO bronze_iot_raw_data VALUES", rows)
        print(f"Inserted {len(rows)} IoT records.")

# NOTE: fetch_elering_history removed - Elering data now written directly to Iceberg via write_elering_to_iceberg

def fetch_weather_history(**context):
    """IDEMPOTENT: Deletes existing rows for the interval, then inserts fresh data."""
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
        # IDEMPOTENCY: Delete existing data for this time interval before inserting
        start_str = start_dt.strftime("%Y-%m-%d %H:%M:%S")
        end_str = end_dt.strftime("%Y-%m-%d %H:%M:%S")
        ch_conn.execute(f"ALTER TABLE bronze_weather_history DELETE WHERE last_changed >= '{start_str}' AND last_changed < '{end_str}'")
        print(f"Deleted existing weather data for interval {start_str} to {end_str}")
        
        ch_conn.execute("INSERT INTO bronze_weather_history VALUES", rows)
        print(f"Inserted {len(rows)} weather records.")


# --- DAG DEFINITION ---
def create_device_and_location_tables():
    """IDEMPOTENT: Drops and recreates tables from CSVs to pick up changes."""
    ch = ClickHouseHook(clickhouse_conn_id="clickhouse_default").get_conn()

    # Drop and recreate to pick up any CSV changes
    ch.execute("DROP TABLE IF EXISTS bronze_device")
    ch.execute("DROP TABLE IF EXISTS bronze_location")

    sql_device = """
    CREATE TABLE bronze_device
    ENGINE = MergeTree()
    ORDER BY tuple()
    AS
    SELECT *
    FROM file('/var/lib/clickhouse/user_files/device_data.csv', 'CSVWithNames')
    """

    sql_location = """
    CREATE TABLE bronze_location
    ENGINE = MergeTree()
    ORDER BY tuple()
    AS
    SELECT *
    FROM file('/var/lib/clickhouse/user_files/location_data.csv', 'CSVWithNames')
    """

    ch.execute(sql_device)
    ch.execute(sql_location)

    print("Loaded bronze_device and bronze_location from CSVs into schema (tables recreated).")


# --- OPENMETADATA SYNC TASK ---
# Configuration for OpenMetadata API
OPENMETADATA_URL = "http://openmetadata-server:8585"
OPENMETADATA_ADMIN_EMAIL = "admin@open-metadata.org"
OPENMETADATA_ADMIN_PASSWORD = "admin"
CLICKHOUSE_SERVICE_NAME = "clickhouse_gold"

# Table descriptions and test definitions
OPENMETADATA_CONFIG = {
    "table_descriptions": {
        "dim_device": "Device dimension table containing IoT sensor information including device names, types, and manufacturers for the ASHP monitoring system.",
        "dim_location": "Location dimension table containing geographic information for sensor locations including city, country, climate zone, and coordinates.",
        "dim_time": "Time dimension table with hourly granularity containing date attributes, day of week, holidays, and time-based analytics support.",
        "fact_heating_energy_usage": "Central fact table containing hourly ASHP performance metrics including power consumption, temperatures, electricity prices, weather data, and calculated COP values.",
    },
    "test_cases": [
        {
            "table": "fact_heating_energy_usage",
            "column": "TimeKey",
            "test_type": "columnValuesToBeNotNull",
            "name": "timekey_not_null",
            "display_name": "TimeKey Not Null",
            "description": "Ensures TimeKey is never null (required for time dimension join)"
        },
        {
            "table": "dim_device",
            "column": "DeviceKey",
            "test_type": "columnValuesToBeUnique",
            "name": "devicekey_unique",
            "display_name": "DeviceKey Unique",
            "description": "Ensures DeviceKey is unique in the device dimension table"
        },
        {
            "table": "fact_heating_energy_usage",
            "column": "ASHP_Power",
            "test_type": "columnValuesToBeBetween",
            "name": "ashp_power_range",
            "display_name": "ASHP Power Valid Range",
            "description": "Ensures ASHP_Power values are within realistic range (0 to 10000 watts)",
            "parameters": [{"name": "minValue", "value": "0"}, {"name": "maxValue", "value": "10000"}]
        }
    ]
}


def sync_openmetadata(**context):
    """
    IDEMPOTENT: Syncs metadata to OpenMetadata after dbt completes.
    
    This task:
    1. Triggers metadata ingestion to discover/refresh all tables
    2. Waits for ingestion to complete
    3. Adds/updates table descriptions
    4. Creates data quality tests (if not exist)
    5. Triggers test suite execution
    """
    print("=" * 60)
    print("OpenMetadata Sync - Phase 2 (After dbt)")
    print("=" * 60)
    
    # --- Helper Functions ---
    def get_token():
        """Get JWT token from OpenMetadata."""
        password_b64 = base64.b64encode(OPENMETADATA_ADMIN_PASSWORD.encode()).decode()
        resp = requests.post(
            f"{OPENMETADATA_URL}/api/v1/users/login",
            json={"email": OPENMETADATA_ADMIN_EMAIL, "password": password_b64},
            headers={"Content-Type": "application/json"},
            timeout=30
        )
        if resp.status_code == 200:
            return resp.json().get("accessToken")
        raise Exception(f"Auth failed: {resp.status_code} - {resp.text}")
    
    def api_get(token, endpoint):
        """GET request to OpenMetadata API."""
        return requests.get(
            f"{OPENMETADATA_URL}/api/v1/{endpoint}",
            headers={"Authorization": f"Bearer {token}"},
            timeout=30
        )
    
    def api_post(token, endpoint, data):
        """POST request to OpenMetadata API."""
        return requests.post(
            f"{OPENMETADATA_URL}/api/v1/{endpoint}",
            headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
            json=data,
            timeout=60
        )
    
    def api_patch(token, endpoint, data):
        """PATCH request to OpenMetadata API."""
        return requests.patch(
            f"{OPENMETADATA_URL}/api/v1/{endpoint}",
            headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json-patch+json"},
            json=data,
            timeout=30
        )
    
    # --- Main Logic ---
    try:
        # Step 1: Authenticate
        print("Authenticating with OpenMetadata...")
        token = get_token()
        print("Authentication successful")
        
        # Step 2: Find and trigger metadata ingestion pipeline
        print("Looking for metadata ingestion pipeline...")
        pipelines_resp = api_get(token, f"services/ingestionPipelines?service={CLICKHOUSE_SERVICE_NAME}&pipelineType=metadata")
        if pipelines_resp.status_code != 200:
            print(f"Warning: Could not fetch pipelines: {pipelines_resp.status_code}")
        else:
            pipelines = pipelines_resp.json().get("data", [])
            if pipelines:
                pipeline_id = pipelines[0].get("id")
                pipeline_name = pipelines[0].get("name")
                deployed = pipelines[0].get("deployed", False)
                
                # Ensure pipeline is deployed first
                if not deployed:
                    print(f"Pipeline not deployed, deploying: {pipeline_name}")
                    deploy_resp = api_post(token, f"services/ingestionPipelines/deploy/{pipeline_id}", {})
                    if deploy_resp.status_code in (200, 201):
                        print("Pipeline deployed successfully")
                    else:
                        print(f"Warning: Deploy failed: {deploy_resp.status_code}")
                
                print(f"Triggering ingestion pipeline: {pipeline_name}")
                
                # Trigger the pipeline (may fail with 400 if already running - that's OK)
                trigger_resp = api_post(token, f"services/ingestionPipelines/trigger/{pipeline_id}", {})
                if trigger_resp.status_code in (200, 201):
                    print("Ingestion triggered successfully")
                elif trigger_resp.status_code == 400:
                    print("Ingestion already running - will wait for it")
                else:
                    print(f"Warning: Trigger failed: {trigger_resp.status_code}")
                
                # Wait for ingestion to complete (up to 90 seconds)
                print("Waiting for ingestion to complete...")
                for i in range(18):  # 18 * 5s = 90 seconds max
                    time.sleep(5)
                    status_resp = api_get(token, f"services/ingestionPipelines/{pipeline_id}")
                    if status_resp.status_code == 200:
                        pipeline_data = status_resp.json()
                        # Check pipelineStatuses - it can be a dict with runId and pipelineState
                        statuses = pipeline_data.get("pipelineStatuses")
                        if isinstance(statuses, dict):
                            state = statuses.get("pipelineState")
                        elif isinstance(statuses, list) and statuses:
                            state = statuses[0].get("pipelineState")
                        else:
                            state = None
                        
                        if state in ("success", "failed", "partialSuccess"):
                            print(f"Ingestion completed: {state}")
                            break
                        elif i % 3 == 0:  # Log every 15 seconds
                            print(f"  Still waiting... (state: {state or 'running'})")
                else:
                    print("Ingestion timeout (90s) - proceeding anyway")
            else:
                print("No metadata ingestion pipeline found - skipping trigger")
        
        # Step 3: Add table descriptions
        print("Updating table descriptions...")
        for table_name, description in OPENMETADATA_CONFIG["table_descriptions"].items():
            fqn = f"{CLICKHOUSE_SERVICE_NAME}.default.default.{table_name}"
            table_resp = api_get(token, f"tables/name/{fqn}")
            if table_resp.status_code == 200:
                table_id = table_resp.json().get("id")
                patch_data = [{"op": "add", "path": "/description", "value": description}]
                patch_resp = api_patch(token, f"tables/{table_id}", patch_data)
                if patch_resp.status_code in (200, 201):
                    print(f"  Updated: {table_name}")
                else:
                    print(f"  Warning: Failed to update {table_name}: {patch_resp.status_code}")
            else:
                print(f"  Warning: Table not found: {table_name}")
        
        # Step 4: Create test suites and test cases
        print("Creating data quality tests...")
        for test_def in OPENMETADATA_CONFIG["test_cases"]:
            table_name = test_def["table"]
            fqn = f"{CLICKHOUSE_SERVICE_NAME}.default.default.{table_name}"
            test_suite_fqn = f"{fqn}.testSuite"
            
            # Ensure test suite exists
            suite_resp = api_get(token, f"dataQuality/testSuites/name/{test_suite_fqn}")
            if suite_resp.status_code != 200:
                # Create test suite
                suite_data = {
                    "name": f"{table_name}_quality_tests",
                    "displayName": f"{table_name} Quality Tests",
                    "description": f"Data quality tests for {table_name}",
                    "executableEntityReference": fqn
                }
                create_suite_resp = api_post(token, "dataQuality/testSuites/executable", suite_data)
                if create_suite_resp.status_code not in (200, 201, 409):
                    print(f"  Warning: Failed to create test suite for {table_name}")
                    continue
            
            # Create test case
            test_case_data = {
                "name": test_def["name"],
                "displayName": test_def["display_name"],
                "description": test_def["description"],
                "testDefinition": test_def["test_type"],
                "entityLink": f"<#E::table::{fqn}::columns::{test_def['column']}>",
                "testSuite": test_suite_fqn,
                "parameterValues": test_def.get("parameters", [])
            }
            
            create_test_resp = api_post(token, "dataQuality/testCases", test_case_data)
            if create_test_resp.status_code in (200, 201):
                print(f"  Created test: {test_def['display_name']}")
            elif create_test_resp.status_code == 409:
                print(f"  Test exists: {test_def['display_name']}")
            else:
                print(f"  Warning: Failed to create test {test_def['name']}: {create_test_resp.status_code}")
        
        # Step 5: Trigger test suite execution (optional - may fail if pipelines don't exist)
        print("Triggering test suite execution...")
        for table_name in set(t["table"] for t in OPENMETADATA_CONFIG["test_cases"]):
            pipeline_name = f"{CLICKHOUSE_SERVICE_NAME}.{table_name}_test_pipeline"
            
            # Check if test pipeline exists, create if not
            test_pipeline_resp = api_get(token, f"services/ingestionPipelines/name/{pipeline_name}")
            if test_pipeline_resp.status_code != 200:
                # Get service ID
                service_resp = api_get(token, f"services/databaseServices/name/{CLICKHOUSE_SERVICE_NAME}")
                if service_resp.status_code == 200:
                    service_id = service_resp.json().get("id")
                    fqn = f"{CLICKHOUSE_SERVICE_NAME}.default.default.{table_name}"
                    
                    pipeline_data = {
                        "name": f"{table_name}_test_pipeline",
                        "displayName": f"{table_name} Test Pipeline",
                        "pipelineType": "TestSuite",
                        "service": {"id": service_id, "type": "databaseService"},
                        "sourceConfig": {
                            "config": {
                                "type": "TestSuite",
                                "entityFullyQualifiedName": fqn
                            }
                        },
                        "airflowConfig": {"scheduleInterval": "0 0 * * *"}
                    }
                    create_pipeline_resp = api_post(token, "services/ingestionPipelines", pipeline_data)
                    if create_pipeline_resp.status_code in (200, 201):
                        pipeline_id = create_pipeline_resp.json().get("id")
                        # Deploy and trigger
                        api_post(token, f"services/ingestionPipelines/deploy/{pipeline_id}", {})
                        time.sleep(2)
                        api_post(token, f"services/ingestionPipelines/trigger/{pipeline_id}", {})
                        print(f"  Created and triggered: {table_name} test pipeline")
                    elif create_pipeline_resp.status_code == 409:
                        print(f"  Test pipeline exists: {table_name}")
            else:
                # Trigger existing pipeline
                pipeline_id = test_pipeline_resp.json().get("id")
                api_post(token, f"services/ingestionPipelines/trigger/{pipeline_id}", {})
                print(f"  Triggered: {table_name} test pipeline")
        
        print("=" * 60)
        print("OpenMetadata sync complete!")
        print("=" * 60)
        
    except requests.exceptions.ConnectionError:
        print("Warning: OpenMetadata server not reachable - skipping sync")
    except Exception as e:
        print(f"Warning: OpenMetadata sync failed: {e}")
        # Don't fail the DAG - OM sync is optional


with DAG(
    dag_id="continuous_ingestion_pipeline",
    start_date=datetime(2025, 10, 20),
    schedule_interval="@hourly",
    catchup=False,
    max_active_runs=1,
    tags=['project2', 'ingestion']
) as dag:

    # Setup tasks to ensure ClickHouse tables exist (IoT and Weather only - Elering uses Iceberg)
    task_setup_iot = PythonOperator(task_id="setup_iot_table", python_callable=setup_bronze_iot_table)
    task_setup_weather = PythonOperator(task_id="setup_weather_table", python_callable=setup_bronze_weather_table)
    task_create_static_tables = PythonOperator(task_id="create_device_and_location_tables", python_callable=create_device_and_location_tables,
    )

    # Iceberg setup task (for Elering price data)
    task_setup_iceberg = PythonOperator(task_id="setup_iceberg_catalog", python_callable=setup_iceberg_catalog)

    # Ingestion tasks to fetch and load data
    task_fetch_iot = PythonOperator(task_id="fetch_iot_history", python_callable=fetch_iot_history)
    task_fetch_weather = PythonOperator(task_id="fetch_weather_history", python_callable=fetch_weather_history)

    # Iceberg write task (writes Elering price data to MinIO/Iceberg - single source of truth)
    task_write_elering_iceberg = PythonOperator(task_id="write_elering_to_iceberg", python_callable=write_elering_to_iceberg)

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

    # OpenMetadata sync task - runs after dbt to update metadata catalog
    task_sync_openmetadata = PythonOperator(
        task_id="sync_openmetadata",
        python_callable=sync_openmetadata,
    )

    # Define dependencies: each stream is independent, dbt runs after ingestion
    task_setup_iot >> task_fetch_iot
    task_setup_weather >> task_fetch_weather
    
    # Iceberg: setup catalog -> write Elering data to MinIO -> create ClickHouse view
    task_setup_iceberg >> task_write_elering_iceberg >> task_create_iceberg_view
    
    [task_fetch_iot, task_fetch_weather, task_create_static_tables, task_create_iceberg_view] >> task_dbt_debug
    task_dbt_debug >> task_dbt_deps >> task_dbt_seed >> task_dbt_run >> task_dbt_test >> task_sync_openmetadata
    