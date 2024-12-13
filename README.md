# Building Damage Reporting System

For this assignment, I focused on creating infrastructure that is scalable and robust enough to support the envisioned pipeline.

## System Components

### Base Data Management
- `download_base_data.py`: Downloads and maintains base NYC geographic data:
  - Building footprints from NYC Open Data
  - Community district boundaries from NYC ArcGIS
  - Uses DuckDB with spatial extensions for efficient geographic data storage

### Core Pipeline Architecture
- `BasePipeline.py`: Abstract base class defining the ETL pipeline structure
  - Handles CSV processing with validation rules
  - Supports pre and post-validation hooks
  - Manages data staging and final table insertion
  - Tracks invalid records separately

### Damage Report Processing
- `DamageReportPipeline.py`: Specialized pipeline for damage reports
  - Validates geographic coordinates within NYC bounds
  - Ensures proper formatting of BIN numbers and ZIP codes
  - Normalizes address data
  - Sanitizes column names automatically
  - Location validation for Manhattan ZIP codes

### Utility Functions
- `extraction_utils.py`: Helper functions for data extraction
  - GeoJSON download and processing
  - Spatial index creation
  - DuckDB spatial extension management

## Data Validation
The system implements two levels of validation:
1. Column-level validation (e.g., coordinate ranges, ZIP format)
2. Row-level validation (e.g., location consistency checks)

Invalid records are automatically separated into a different table for review.

## Database Structure
Uses DuckDB as the backend database with:
- Spatial extensions for geographic operations
- Temporary staging tables for data processing
- Separate tables for valid and invalid records
- Automatic timestamp tracking

## Getting Started
1. Build the environment with conda.
2. Run pip install -e . to install the building_damage module.
3. Run 'set_up_database.py' to set up the duckdb database.
4. Run 'download_base_data.py' to pull geographic base data from APIs and create spatial indices.
5. Run 'update_damage_reports' to update reports based on a new input CSV.

## Future Enhancements
- Integration with Google Geocoding API for address validation
- Additional BasePipeline subclasses for other data sources
- Better reporting on validation failures
