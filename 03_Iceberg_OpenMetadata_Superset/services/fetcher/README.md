This service fetches data from an Elering API endpoint and writes Parquet files to MinIO (bucket `bronze`).

Environment variables (defaults shown):

- `ELERING_URL` - URL to fetch (default: https://dashboard.elering.ee/api/nps/realized_generation)
- `MINIO_ENDPOINT` - MinIO endpoint (default: http://minio:9000)
- `MINIO_ACCESS_KEY` - MinIO access key (default: admin)
- `MINIO_SECRET_KEY` - MinIO secret (default: password)
- `MINIO_BUCKET` - Bucket to write Parquet files to (default: bronze)
- `OBJECT_PREFIX` - Prefix inside the bucket (default: elering)

Build and run with docker-compose (from project root):

```bash
docker compose build fetcher
docker compose up fetcher
```

The fetcher runs once and exits. For regular ingestion, schedule via Airflow DAG or run the container periodically.
