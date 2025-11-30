#!/usr/bin/env python3
"""
OpenMetadata Bootstrap Script - Phase 1 (Startup)

This script runs on `docker compose up` and performs lightweight setup:
1. Creates ClickHouse database service connection
2. Creates a scheduled metadata ingestion pipeline (does NOT run immediately)

Heavy operations (ingestion trigger, descriptions, tests) are handled by Airflow
after dbt has created the tables.

IDEMPOTENT: Safe to run multiple times - uses create-if-not-exists pattern.
"""

import time
import sys
import base64
import requests
from typing import Optional

# Configuration
OPENMETADATA_URL = "http://openmetadata-server:8585"
ADMIN_EMAIL = "admin@open-metadata.org"
ADMIN_PASSWORD = "admin"

# ClickHouse connection details
CLICKHOUSE_SERVICE_NAME = "clickhouse_gold"
CLICKHOUSE_HOST = "clickhouse-server:8123"
CLICKHOUSE_USER = "airflow"
CLICKHOUSE_PASSWORD = "supersecret"
CLICKHOUSE_DATABASE = "default"

# Retry configuration
MAX_RETRIES = 30
RETRY_DELAY = 10  # seconds


def wait_for_openmetadata() -> bool:
    """Wait for OpenMetadata server to be healthy."""
    print(f"Waiting for OpenMetadata server at {OPENMETADATA_URL}...")
    
    for attempt in range(MAX_RETRIES):
        try:
            # Try the health endpoint
            resp = requests.get(f"{OPENMETADATA_URL}/api/v1/system/version", timeout=5)
            if resp.status_code == 200:
                version = resp.json().get("version", "unknown")
                print(f"OpenMetadata server is ready (version: {version})")
                return True
        except requests.exceptions.RequestException as e:
            pass
        
        print(f"  Attempt {attempt + 1}/{MAX_RETRIES} - waiting {RETRY_DELAY}s...")
        time.sleep(RETRY_DELAY)
    
    print("ERROR: OpenMetadata server did not become ready in time")
    return False


def get_auth_token() -> Optional[str]:
    """Authenticate with OpenMetadata and get JWT token."""
    print("Authenticating with OpenMetadata...")
    
    # Password must be base64 encoded
    password_b64 = base64.b64encode(ADMIN_PASSWORD.encode()).decode()
    
    try:
        resp = requests.post(
            f"{OPENMETADATA_URL}/api/v1/users/login",
            json={"email": ADMIN_EMAIL, "password": password_b64},
            headers={"Content-Type": "application/json"},
            timeout=30
        )
        
        if resp.status_code == 200:
            token = resp.json().get("accessToken")
            print("Authentication successful")
            return token
        else:
            print(f"Authentication failed: {resp.status_code} - {resp.text}")
            return None
    except Exception as e:
        print(f"Authentication error: {e}")
        return None


def create_clickhouse_service(token: str) -> Optional[str]:
    """Create ClickHouse database service (idempotent)."""
    print(f"Creating ClickHouse service: {CLICKHOUSE_SERVICE_NAME}...")
    
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    
    # Check if service already exists
    try:
        resp = requests.get(
            f"{OPENMETADATA_URL}/api/v1/services/databaseServices/name/{CLICKHOUSE_SERVICE_NAME}",
            headers=headers,
            timeout=30
        )
        if resp.status_code == 200:
            service_id = resp.json().get("id")
            print(f"Service already exists: {CLICKHOUSE_SERVICE_NAME} (id: {service_id})")
            return service_id
    except Exception:
        pass
    
    # Create new service
    service_payload = {
        "name": CLICKHOUSE_SERVICE_NAME,
        "displayName": "ClickHouse Gold Layer",
        "description": "ClickHouse data warehouse containing bronze, staging, and gold layer tables for ASHP monitoring",
        "serviceType": "Clickhouse",
        "connection": {
            "config": {
                "type": "Clickhouse",
                "scheme": "clickhouse+http",
                "hostPort": CLICKHOUSE_HOST,
                "username": CLICKHOUSE_USER,
                "password": CLICKHOUSE_PASSWORD,
                "databaseName": CLICKHOUSE_DATABASE,
                "databaseSchema": CLICKHOUSE_DATABASE
            }
        }
    }
    
    try:
        resp = requests.post(
            f"{OPENMETADATA_URL}/api/v1/services/databaseServices",
            headers=headers,
            json=service_payload,
            timeout=30
        )
        
        if resp.status_code in (200, 201):
            service_id = resp.json().get("id")
            print(f"Created service: {CLICKHOUSE_SERVICE_NAME} (id: {service_id})")
            return service_id
        elif resp.status_code == 409:
            print(f"Service already exists (409 conflict)")
            # Fetch and return existing ID
            resp = requests.get(
                f"{OPENMETADATA_URL}/api/v1/services/databaseServices/name/{CLICKHOUSE_SERVICE_NAME}",
                headers=headers,
                timeout=30
            )
            return resp.json().get("id") if resp.status_code == 200 else None
        else:
            print(f"Failed to create service: {resp.status_code} - {resp.text}")
            return None
    except Exception as e:
        print(f"Error creating service: {e}")
        return None


def create_ingestion_pipeline(token: str, service_id: str) -> bool:
    """Create scheduled metadata ingestion pipeline (idempotent)."""
    pipeline_name = f"{CLICKHOUSE_SERVICE_NAME}_metadata_scheduled"
    print(f"Creating ingestion pipeline: {pipeline_name}...")
    
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    
    # Check if pipeline already exists
    pipeline_id = None
    try:
        resp = requests.get(
            f"{OPENMETADATA_URL}/api/v1/services/ingestionPipelines/name/{CLICKHOUSE_SERVICE_NAME}.{pipeline_name}",
            headers=headers,
            timeout=30
        )
        if resp.status_code == 200:
            pipeline_id = resp.json().get("id")
            deployed = resp.json().get("deployed", False)
            print(f"Ingestion pipeline already exists: {pipeline_name} (deployed: {deployed})")
            if not deployed:
                # Deploy the existing pipeline
                print("Deploying existing pipeline...")
                deploy_resp = requests.post(
                    f"{OPENMETADATA_URL}/api/v1/services/ingestionPipelines/deploy/{pipeline_id}",
                    headers=headers,
                    timeout=60
                )
                if deploy_resp.status_code in (200, 201):
                    print("Deployed ingestion pipeline successfully")
                else:
                    print(f"Warning: Deploy returned {deploy_resp.status_code}")
            return True
    except Exception:
        pass
    
    # Create new pipeline with 6-hour schedule
    pipeline_payload = {
        "name": pipeline_name,
        "displayName": "ClickHouse Gold Metadata (Scheduled)",
        "description": "Scheduled metadata ingestion for ClickHouse - runs every 6 hours as backup to Airflow-triggered ingestion",
        "pipelineType": "metadata",
        "service": {
            "id": service_id,
            "type": "databaseService"
        },
        "sourceConfig": {
            "config": {
                "type": "DatabaseMetadata",
                "markDeletedTables": True,
                "includeTables": True,
                "includeViews": True
            }
        },
        "airflowConfig": {
            "scheduleInterval": "0 */6 * * *"  # Every 6 hours
        }
    }
    
    try:
        resp = requests.post(
            f"{OPENMETADATA_URL}/api/v1/services/ingestionPipelines",
            headers=headers,
            json=pipeline_payload,
            timeout=30
        )
        
        if resp.status_code in (200, 201):
            pipeline_id = resp.json().get("id")
            print(f"Created ingestion pipeline: {pipeline_name} (id: {pipeline_id})")
            
            # Deploy the pipeline (makes it available in OM's Airflow)
            deploy_resp = requests.post(
                f"{OPENMETADATA_URL}/api/v1/services/ingestionPipelines/deploy/{pipeline_id}",
                headers=headers,
                timeout=60
            )
            if deploy_resp.status_code in (200, 201):
                print(f"Deployed ingestion pipeline successfully")
            else:
                print(f"Warning: Pipeline deploy returned {deploy_resp.status_code} - may need manual deploy")
            
            return True
        elif resp.status_code == 409:
            print(f"Ingestion pipeline already exists (409 conflict)")
            return True
        else:
            print(f"Failed to create pipeline: {resp.status_code} - {resp.text}")
            return False
    except Exception as e:
        print(f"Error creating pipeline: {e}")
        return False


def main():
    """Main bootstrap function."""
    print("=" * 60)
    print("OpenMetadata Bootstrap - Phase 1 (Startup)")
    print("=" * 60)
    
    # Step 1: Wait for OpenMetadata
    if not wait_for_openmetadata():
        sys.exit(1)
    
    # Step 2: Authenticate
    token = get_auth_token()
    if not token:
        sys.exit(1)
    
    # Step 3: Create ClickHouse service
    service_id = create_clickhouse_service(token)
    if not service_id:
        sys.exit(1)
    
    # Step 4: Create scheduled ingestion pipeline
    if not create_ingestion_pipeline(token, service_id):
        print("Warning: Failed to create ingestion pipeline, but service exists")
        # Don't fail - Airflow can handle ingestion
    
    print("=" * 60)
    print("Bootstrap Phase 1 complete!")
    print("- ClickHouse service created/verified")
    print("- Scheduled ingestion pipeline created (runs every 6h)")
    print("- Airflow will trigger immediate ingestion after dbt completes")
    print("=" * 60)


if __name__ == "__main__":
    main()
