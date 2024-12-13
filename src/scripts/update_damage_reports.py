#!/usr/bin/env python3
"""
Script to process and update building damage reports from CSV files into a DuckDB database.
Assumes database tables are already set up with correct schema.
"""

import logging
import sys
from pathlib import Path

import click
import duckdb as db
from building_damage.DamageReportPipeline import DamageReportPipeline

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Database schema for reference
SCHEMA = (
    "Address VARCHAR,"
    "City VARCHAR,"
    "ZIP_Code VARCHAR,"
    "No_Electricity BOOLEAN,"
    "Basement_Flooded BOOLEAN,"
    "Roof_Damaged BOOLEAN,"
    "Insurance BOOLEAN,"
    "BIN VARCHAR,"
    "Latitude DOUBLE,"
    "Longitude DOUBLE"
)

def process_damage_reports(
    db_path: str,
    csv_path: str,
    schema: str
) -> None:
    """
    Process damage reports from CSV and update database.
    Assumes required tables already exist.
    
    Args:
        db_path: Path to DuckDB database file
        csv_path: Path to input CSV file
        schema: Database schema definition
    """
    try:
        # Validate paths
        if not Path(csv_path).exists():
            raise FileNotFoundError(f"CSV file not found: {csv_path}")
        
        # Connect to database
        conn = db.connect(db_path)
        logger.info(f"Connected to database: {db_path}")
        
        # Initialize and run pipeline
        pipeline = DamageReportPipeline(conn, schema)
        pipeline.process_csv(
            csv_path,
            "test_storm_damage",
            "test_storm_damage_invalid"
        )
        
        # Log summary statistics
        valid_count = conn.execute(
            "SELECT COUNT(*) FROM test_storm_damage"
        ).fetchone()[0]
        invalid_count = conn.execute(
            "SELECT COUNT(*) FROM test_storm_damage_invalid"
        ).fetchone()[0]
        
        logger.info(f"Processing complete. Valid records: {valid_count}, "
                   f"Invalid records: {invalid_count}")
        
    except Exception as e:
        logger.error(f"Failed to process damage reports: {str(e)}")
        raise
    finally:
        if 'conn' in locals():
            conn.close()

@click.command()
@click.option(
    '--db-path',
    type=click.Path(exists=True),
    required=True,
    help='Path to DuckDB database file'
)
@click.option(
    '--csv-path',
    type=click.Path(exists=True),
    required=True,
    help='Path to input CSV file'
)
def main(db_path: str, csv_path: str):
    """Process building damage reports from CSV into DuckDB database."""
    try:
        process_damage_reports(db_path, csv_path, SCHEMA)
    except Exception as e:
        logger.error(f"Script failed: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()
