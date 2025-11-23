"""Local shim package to shadow installed `airflow_clickhouse_plugin` during DAG parsing.

This package lives under `airflow/plugins` and provides a lightweight, lazy-loading
shim for the ClickHouse hook so DAGs can be parsed without triggering Airflow
settings initialization at import time.
"""

__all__ = ["hooks"]
