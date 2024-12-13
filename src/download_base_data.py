import logging

import duckdb as db

from src.util.extraction_utils import (
    download_geojson_to_table,
    ensure_spatial_extension,
)

# Limit for big API requests
# Pagination would be safer, but this works for one-off bulk download
MAX_RECORDS = 100_000_000


def main() -> None:
    conn = db.connect("building_damage.db")

    ensure_spatial_extension(conn)

    # Load building footprints
    logging.info("Loading building footprints")
    building_footprints_endpoint: str = (
        "https://data.cityofnewyork.us/resource/qb5r-6dgf.geojson"
    )
    download_geojson_to_table(
        conn,
        "building_footprints",
        building_footprints_endpoint,
        create_spatial_index=True,
        params={"$limit": MAX_RECORDS},
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
