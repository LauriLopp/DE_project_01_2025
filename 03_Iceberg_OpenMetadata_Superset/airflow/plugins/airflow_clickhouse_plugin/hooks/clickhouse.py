"""Lazy-loading shim for ClickHouseHook used during DAG parsing.

The real implementation is provided by the `airflow_clickhouse_plugin` package
installed in site-packages. Importing that implementation triggers importing
`airflow` which initializes settings (and creates DB engines) at import time —
this breaks DAG parsing. This shim defers importing the real implementation
until an attribute or method is actually accessed at runtime.
"""
from importlib import import_module


class ClickHouseHook:
    def __init__(self, *args, **kwargs):
        self._args = args
        self._kwargs = kwargs
        self._real = None

    def _load_real(self):
        if self._real is None:
            # import the real module from site-packages
            real_mod = import_module("airflow_clickhouse_plugin.hooks.clickhouse")
            Real = getattr(real_mod, "ClickHouseHook")
            self._real = Real(*self._args, **self._kwargs)

    def __getattr__(self, item):
        self._load_real()
        return getattr(self._real, item)
