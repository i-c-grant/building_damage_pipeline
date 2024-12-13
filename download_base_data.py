from typing import Any, Dict, Optional

import duckdb as db
import requests
from duckdb import DuckDBPyConnection


def main() -> None:
    conn = db.connect("building_damage.db")

    ensure_spatial_extension(conn)

    # Load building footprints
    building_footprints_endpoint: str = (
        "https://data.cityofnewyork.us/resource/7w4b-tj9d.geojson"
    )
    download_geojson_to_table(
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
