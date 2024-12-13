"""Utility functions for extraction layer of building damage pipeline."""

from typing import Any, Dict, Optional

import requests
from duckdb import DuckDBPyConnection


def ensure_spatial_extension(connection: db.DuckDBPyConnection) -> None:
    """Ensures the spatial extension is installed and loaded."""
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
    response = requests.get(endpoint, params=params or {})
    geojson_data = response.json()
    conn.execute(f"CREATE TABLE {table_name} AS SELECT * FROM ST_Read({geojson_data})")

    if create_spatial_index:
        conn.execute(
            f"CREATE INDEX {table_name}_geom ON {table_name} USING SPATIAL(geom)"
        )
