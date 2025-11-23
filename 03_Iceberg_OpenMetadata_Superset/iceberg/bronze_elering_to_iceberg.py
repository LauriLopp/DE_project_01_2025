import logging
from datetime import datetime
import pyarrow as pa
from pyiceberg.catalog import load_catalog

# Configure logging
logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

# Configurations
CATALOG_NAME = "default"  # refers to env variables prefixed with PYICEBERG_CATALOG__DEFAULT
TABLE_NAME = "bronze.elering_price_iceberg"


def create_table_if_not_exists(catalog):
    """Create an Iceberg table if it does not already exist."""
    try:
        table = catalog.load_table(identifier=TABLE_NAME)
        log.info("Table %s already exists", TABLE_NAME)
        return table
    except Exception:
        log.info("Table %s does not exist. Creating...", TABLE_NAME)

        # Define schema for the Iceberg table
        iceberg_schema = pa.schema([
            pa.field("ingestion_ts", pa.timestamp("us"), nullable=False),
            pa.field("ts_utc", pa.timestamp("us"), nullable=False),
            pa.field("zone", pa.string(), nullable=False),
            pa.field("currency", pa.string(), nullable=False),
            pa.field("price_per_mwh", pa.float64(), nullable=False),
        ])

        # Create Iceberg table
        table = catalog.create_table(
            identifier=TABLE_NAME,
            schema=iceberg_schema,
            properties={
                "format-version": "2"
            },
        )
        log.info("Created Iceberg table: %s", TABLE_NAME)
        return table


def append_elering_records(records):
    """
    Append a list of Elering price records into the Iceberg table.
    Each item in `records` must be a dict matching the schema.
    """

    catalog = load_catalog(CATALOG_NAME)
    table = create_table_if_not_exists(catalog)

    # Convert Python dict rows to PyArrow Table
    arrow_table = pa.Table.from_pylist(records)

    # Append data
    table.append(arrow_table)

    log.info("Appended %d rows to %s", len(records), TABLE_NAME)
