# Building Damage Reporting System

For this assignment, I focused on creating infrastructure that is scalable and robust enough to support the envisioned pipeline. In particular, I used a database backend--here, DuckDB, but I would prefer PostGreSQL for final implementation, and developed some reusable infrastructure to support validation and consistent representations across the various data types.

My vision for the final pipeline is to develop a centralized representation of damage reports. This representation would be a table where each row is unique with respect to BIN and a source ID corresponding to each timestamped data source. This would make it possible to trace changing reports for each building over time. The key would be to 1) use spatial methods to translate, for instance, flood extents to lists of affected BINs and 2) to use geocoding and other contextual data to assign BINs to buildings that lack them. This full implementation goes beyond the scope of this assignment, but the infrastructure developed here is a start.


## System Components

### Base Data Management
- `download_base_data.py`: Downloads and maintains base NYC geographic data:
  - Building footprints from NYC Open Data
  - Community district boundaries from ArcGIS REST service
  - Uses DuckDB with spatial extension and indexing for efficient access over the course of the deployment

### Core Pipeline Architecture
- `BasePipeline.py`: Abstract base class defining the ETL pipeline structure
  - Handles CSV processing with validation rules
  - Supports pre and post-validation hooks. Pre-validation hooks allow data enhancement (e.g. geocoding) ahead of validation to support additional checks.
  - Manages data staging and final table insertion
  - Tracks invalid records separately
  - Timestamps new records to allow for tracking change over time
  - To do: abstract away from CSV processing to support other report types

### Damage Report Processing
- `DamageReportPipeline.py`: Specialized pipeline for damage reports
  - Extends BasePipeline with validation logic specific to the provided CSV
  - Validates geographic coordinates within NYC bounds
  - Ensures proper formatting of BIN numbers and ZIP codes, catching missing BINs
  - Normalizes address data
  - Sanitizes column names automatically

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
Many more validation methods could be implemented, including spatial methods.

## Database Structure
Uses DuckDB as the backend database with:
- Spatial extensions for geographic operations
- Temporary staging tables for data processing
- Separate tables for valid and invalid records
- Automatic timestamp tracking

## Running the pipeline
For each script, use the --help option to see usage.
1. Build the environment specified in environment.yaml with conda ('conda create build_damage_env -f environment.yaml').
2. Run 'pip install -e .' to install the building_damage package in development mode.
3. Run 'set_up_database.py' to set up the duckdb database with the appropriate schemas.
4. Run 'download_base_data.py' to pull geographic base data from APIs and create spatial indices.
5. Run 'update_damage_reports' to update reports based on a new input CSV.
6. Run aggregate_by_district.sql and export_damage_reports.sql against the database to generate the output GeoJSONs (e.g., 'duckdb building_damage.db < aggregate_by_district.sql').

In a production environment, steps 3 and 4 would happen once at the begin of the deployment. Step 5 would happen whenever new reports are available. Steps 6 and 7 could be run either on a schedule or on demand.


## Output Files
The pipeline generates two main GeoJSON outputs in the data/processed directory:
- `damage_reports.geojson`: Individual damage reports joined with building footprints
- `district_damage_counts.geojson`: Aggregated damage statistics by community district

Note: All spatial data is standardized to EPSG:4326 (WGS84) for this project. Future implementations requiring higher accuracy within NYC might benefit from using State Plane (EPSG:2263).

## Future Enhancements
- Integration with Google Geocoding API for address validation
- Additional BasePipeline subclasses for other data sources
- Better reporting on validation failures
- More spatial validation in general, especially footprints vs. reported coordinates
