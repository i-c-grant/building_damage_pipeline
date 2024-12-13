#!/usr/bin/env python3
"""
Script to set up DuckDB database tables for building damage reports.
Creates the main table and invalid records table with appropriate schema.
"""

import logging
import sys
from pathlib import Path

import click
import duckdb as db

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Database schema
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
    "Longitude DOUBLE,"
    "time_updated TIMESTAMP"
)

def set_up_database(db_path: str, schema: str) -> None:
    """
    Set up database tables for damage reports.
    
    Args:
        db_path: Path to DuckDB database file
        schema: Database schema definition
    """
    try:
        # Connect to database (create if doesn't exist)
        conn = db.connect(db_path)
        logger.info(f"Connected to database: {db_path}")
        
        # Drop existing tables if they exist
        tables_to_drop = [
            "storm_damage",
            "storm_damage_invalid"
        ]
        for table in tables_to_drop:
            conn.execute(f"DROP TABLE IF EXISTS {table}")
        
        # Create main table for valid records
        conn.execute(f"CREATE TABLE storm_damage ({schema})")
        logger.info("Created storm_damage table")
        
        # Create table for invalid records with additional error column
        conn.execute(f"""
            CREATE TABLE storm_damage_invalid ({schema})
        """)
        logger.info("Created storm_damage_invalid table")
        
    except Exception as e:
        logger.error(f"Failed to set_up database: {str(e)}")
        raise
    finally:
        if 'conn' in locals():
            conn.close()

@click.command()
@click.option(
    '--db-path',
    type=click.Path(),
    required=True,
    help='Path to DuckDB database file (will be created if it does not exist)'
)
def main(db_path: str):
    """Set up DuckDB database tables for building damage reports."""
    try:
        # Create parent directories if they don't exist
        Path(db_path).parent.mkdir(parents=True, exist_ok=True)
        set_up_database(db_path, SCHEMA)
        logger.info("Database setup completed successfully")
    except Exception as e:
        logger.error(f"Database setup failed: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()
