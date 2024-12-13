#!/usr/bin/env python3
"""
Script to download and load building footprints and community districts data
into a DuckDB database with spatial support.
"""

import logging
from pathlib import Path
from typing import Optional

import click
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

# Default endpoints
BUILDING_FOOTPRINTS_URL = "https://data.cityofnewyork.us/resource/qb5r-6dgf.geojson"
COMMUNITY_DISTRICTS_URL = (
    "https://services5.arcgis.com/GfwWNkhOj9bNBqoJ/arcgis/rest/services/"
    "NYC_Community_Districts/FeatureServer/0/query"
)

def download_base_data(
    db_path: str,
    max_records: int,
    footprints_url: Optional[str] = None,
    districts_url: Optional[str] = None
) -> None:
    """
    Download and load building footprints and community districts data.
    
    Args:
        db_path: Path to DuckDB database
        max_records: Maximum number of records to download
        footprints_url: Optional custom URL for building footprints
        districts_url: Optional custom URL for community districts
    """
    conn = db.connect(db_path)
    ensure_spatial_extension(conn)
    
    try:
        # Load building footprints
        logger.info("Starting building footprints download")
        download_geojson_to_table(
            conn,
            "building_footprints",
            footprints_url or BUILDING_FOOTPRINTS_URL,
            create_spatial_index=True,
            params={"$limit": max_records},
        )
        logger.info("Completed building footprints download")
        
        # Load community districts
        logger.info("Starting community districts download")
        download_geojson_to_table(
            conn,
            "community_districts",
            districts_url or COMMUNITY_DISTRICTS_URL,
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
        
    finally:
        conn.close()

@click.command()
@click.option(
    '--db-path',
    type=click.Path(),
    required=True,
    help='Path to DuckDB database file'
)
@click.option(
    '--max-records',
    type=int,
    default=100_000_000,
    help='Maximum number of records to download (default: 100,000,000)'
)
@click.option(
    '--footprints-url',
    type=str,
    help='Custom URL for building footprints data (optional)'
)
@click.option(
    '--districts-url',
    type=str,
    help='Custom URL for community districts data (optional)'
)
def main(
    db_path: str,
    max_records: int,
    footprints_url: Optional[str],
    districts_url: Optional[str]
) -> None:
    """Download building footprints and community districts data into DuckDB."""
    try:
        # Create parent directories if they don't exist
        Path(db_path).parent.mkdir(parents=True, exist_ok=True)
        
        download_base_data(
            db_path=db_path,
            max_records=max_records,
            footprints_url=footprints_url,
            districts_url=districts_url
        )
    except Exception as e:
        logger.error(f"Data download failed: {str(e)}")
        raise

if __name__ == "__main__":
    main()
