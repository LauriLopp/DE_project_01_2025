FROM python:3.11-slim

# Install git
RUN apt-get update && apt-get install -y git

# Install ClickHouse driver and dbt (pin to match Airflow image for consistency)
RUN pip install --no-cache-dir \
    dbt-core==1.7.9 \
    dbt-clickhouse==1.7.6 \
    clickhouse-connect

# keep working dir minimal; volumes will handle your project
WORKDIR /dbt
ENTRYPOINT ["bash"]
