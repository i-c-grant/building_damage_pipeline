import logging

import duckdb as db

from building_damage.utils.extraction_utils import (
    download_geojson_to_table,
    ensure_spatial_extension,
)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

# Limit for big API requests
# Pagination would be safer, but this works for one-off bulk download
MAX_RECORDS = 100_000_000


def main() -> None:
    db_path = "data/db/building_damage.db"
    conn = db.connect(db_path)

    ensure_spatial_extension(conn)

    # Load building footprints
    logger.info("Starting building footprints download")
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

    logger.info("Completed building footprints download")

    # Load community districts
    logger.info("Starting community districts download")
    community_districts_endpoint: str = (
        "https://services5.arcgis.com/GfwWNkhOj9bNBqoJ/arcgis/rest/services/"
        "NYC_Community_Districts/FeatureServer/0/query"
    )
    download_geojson_to_table(
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
    logger.info("Completed community districts download")

    logger.info("Base data download process completed successfully")


if __name__ == "__main__":
    main()
