from typing import Any, Dict, Optional

import duckdb as db
import requests
from duckdb import DuckDBPyConnection


def ensure_spatial_extension(connection: duckdb.DuckDBPyConnection) -> None:
    """Ensures the spatial extension is installed and loaded."""
    connection.execute("INSTALL spatial")
    connection.execute("LOAD spatial")


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


def main() -> None:
    conn = db.connect("building_damage.db")

    # Load building footprints
    building_footprints_endpoint: str = (
        "https://data.cityofnewyork.us/resource/7w4b-tj9d.geojson"
    )
    load_geojson_to_table(
        conn,
        "building_footprints",
        building_footprints_endpoint,
        create_spatial_index=True,
    )

    # Load community districts
    community_districts_endpoint: str = (
        "https://services5.arcgis.com/GfwWNkhOj9bNBqoJ/arcgis/rest/services/"
        "NYC_Community_Districts/FeatureServer/0/query"
    )
    load_geojson_to_table(
        conn,
        "community_districts",
        community_districts_endpoint,
        params={
            "where": "1=1",
            "outFields": "*",
            "f": "geojson",
            "returnGeometry": "true",
        },
        create_spatial_index=True,
    )


if __name__ == "__main__":
    main()
