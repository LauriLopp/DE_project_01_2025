#!/usr/bin/env python3
import os
import io
import sys
import json
import logging
from datetime import datetime

import requests
import pandas as pd
import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("fetcher")

ELERING_URL = os.getenv("ELERING_URL", "https://dashboard.elering.ee/api/nps/realized_generation")
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "admin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "password")
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "bronze")
MINIO_REGION = os.getenv("MINIO_REGION", "us-east-1")
OBJECT_PREFIX = os.getenv("OBJECT_PREFIX", "elering")


def ensure_bucket(s3_client, bucket: str):
    try:
        s3_client.head_bucket(Bucket=bucket)
        log.info("Bucket '%s' already exists", bucket)
    except ClientError as e:
        # Try to create
        try:
            log.info("Creating bucket '%s'", bucket)
            s3_client.create_bucket(Bucket=bucket)
        except ClientError as ce:
            log.error("Failed to create bucket %s: %s", bucket, ce)
            raise


def fetch_elering(url: str):
    log.info("Fetching Elering data from %s", url)
    resp = requests.get(url, timeout=30)
    resp.raise_for_status()
    content_type = resp.headers.get("Content-Type", "")
    if "application/json" in content_type or resp.text.strip().startswith("{") or resp.text.strip().startswith("["):
        data = resp.json()
        return data
    else:
        # Try to parse as CSV
        try:
            df = pd.read_csv(io.StringIO(resp.text))
            return df
        except Exception as e:
            log.error("Unknown response format and failed to parse CSV: %s", e)
            raise


def normalize_to_df(data):
    if isinstance(data, pd.DataFrame):
        return data
    if isinstance(data, list):
        try:
            df = pd.DataFrame(data)
            return df
        except Exception:
            # Try json_normalize
            df = pd.json_normalize(data)
            return df
    if isinstance(data, dict):
        # If top-level has a key that is list-like, prefer that
        for k, v in data.items():
            if isinstance(v, list):
                try:
                    df = pd.DataFrame(v)
                    # add top-level keys as columns if scalar
                    for tk, tv in data.items():
                        if not isinstance(tv, list):
                            df[tk] = tv
                    return df
                except Exception:
                    continue
        # fallback: normalize dict
        try:
            df = pd.json_normalize(data)
            return df
        except Exception:
            # final fallback: single-row frame
            return pd.DataFrame([data])
    raise ValueError("Unsupported data type for normalization: %s" % type(data))


def df_to_parquet_bytes(df: pd.DataFrame) -> bytes:
    buf = io.BytesIO()
    df.to_parquet(buf, index=False, engine="pyarrow")
    buf.seek(0)
    return buf.read()


def main():
    # Create s3 client pointed at MinIO
    s3_config = Config(signature_version="s3v4", s3={'addressing_style': 'path'})
    s3 = boto3.client(
        's3',
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        config=s3_config,
        region_name=MINIO_REGION,
    )

    # Ensure bucket exists
    try:
        ensure_bucket(s3, MINIO_BUCKET)
    except Exception:
        log.exception("Cannot ensure bucket exists")
        sys.exit(2)

    # Fetch
    try:
        data = fetch_elering(ELERING_URL)
    except Exception:
        log.exception("Failed to fetch Elering data")
        sys.exit(3)

    # Normalize
    try:
        df = normalize_to_df(data)
    except Exception:
        log.exception("Failed to normalize fetched data to DataFrame")
        sys.exit(4)

    if df.empty:
        log.warning("Fetched DataFrame is empty — nothing to write")
        return

    # Write parquet bytes
    try:
        parquet_bytes = df_to_parquet_bytes(df)
    except Exception:
        log.exception("Failed to convert DataFrame to Parquet")
        sys.exit(5)

    # Put to S3/MinIO
    ts = datetime.utcnow().strftime('%Y%m%dT%H%M%SZ')
    key = f"{OBJECT_PREFIX}/{ts}.parquet"
    try:
        log.info("Uploading parquet to s3://%s/%s", MINIO_BUCKET, key)
        s3.put_object(Bucket=MINIO_BUCKET, Key=key, Body=parquet_bytes)
        log.info("Upload complete")
    except Exception:
        log.exception("Failed to upload parquet to MinIO")
        sys.exit(6)


if __name__ == '__main__':
    main()
