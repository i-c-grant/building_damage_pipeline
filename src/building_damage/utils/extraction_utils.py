"""Utility functions for extraction layer of building damage pipeline."""

import json
import logging
import tempfile
from typing import Any, Dict, Optional

import requests
from duckdb import DuckDBPyConnection

logger = logging.getLogger(__name__)


def ensure_spatial_extension(conn: DuckDBPyConnection) -> None:
    """Ensures the DuckDB spatial extension is installed and loaded."""
    conn.execute("INSTALL spatial")
    conn.execute("LOAD spatial")


def download_geojson_to_table(
    conn: DuckDBPyConnection,
    table_name: str,
    endpoint: str,
    params: Optional[Dict[str, Any]] = None,
    create_spatial_index: bool = False,
) -> None:
    """
    Downloads GeoJSON data from an API endpoint and loads it into a new DuckDB table.
    Args:
        conn: DuckDB connection to use
        table_name: Name of the table to create in DuckDB
        endpoint: URL of the GeoJSON API endpoint
        params: Query parameters for the API request
        create_spatial_index: Whether to create a spatial index on the geom column
    """
    # Get the response from the API
    logger.info(f"Downloading data from {endpoint}")
    response = requests.get(endpoint, params=params or {})
    response.raise_for_status()
    geojson_data = response.json()

    logger.info(
        f"Downloaded {len(geojson_data['features'])} features from {endpoint}"
    )

    # DuckDB reading utility expects a file, so write to a tempfile
    with tempfile.NamedTemporaryFile(mode="w", suffix=".geojson") as temp_file:
        json.dump(geojson_data, temp_file)
        temp_file.flush()
        conn.execute(
            f"CREATE TABLE {table_name} AS SELECT * FROM ST_Read('{temp_file.name}')"
        )
        logger.info(f"Created table {table_name}")
        num_rows = conn.execute(
            f"SELECT COUNT(*) FROM {table_name}"
        ).fetchone()[0]
        logger.info(f"Table {table_name} has {num_rows} rows")

        if create_spatial_index:
            logger.info(f"Creating spatial index on {table_name}...")
            conn.execute(
                f"CREATE INDEX {table_name}_geom ON {table_name} USING RTREE (geom)"
            )
            logger.info(f"Created spatial index on {table_name}")
