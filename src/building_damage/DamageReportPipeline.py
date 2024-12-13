from building_damage.BasePipeline import BasePipeline, ValidationFunction
from typing import Dict, List
import duckdb as db


class DamageReportPipeline(BasePipeline):
    def column_validation_rules(self) -> Dict[str, str]:
        return {
            "latitude": "latitude BETWEEN 40 AND 42",  # NYC area
            "longitude": "longitude BETWEEN -74.5 AND -72.5",  # NYC area
            "address": "LENGTH(address) > 0",
            "bin": "LENGTH(bin::VARCHAR) >= 7 AND bin SIMILAR TO '[0-9]+'",
            "zip_code": "LENGTH(zip_code) = 5 AND zip_code SIMILAR TO '[0-9]{5}'",
        }

    def row_validation_rules(self) -> Dict[str, str]:
        return {
            # TODO: add pre-validation hook to get geocoded address through Google API,
            # then compare that to the given lat-lon here
            "location_mismatch": """
                CASE 
                    WHEN zip_code LIKE '100%' AND 
                         (latitude < 40.6 OR latitude > 40.9 OR 
                          longitude < -74.1 OR longitude > -73.9)
                    THEN false 
                    ELSE true 
                END
            """
        }

    @property
    def pre_validation_hooks(self) -> List[ValidationFunction]:
        def sanitize_column_names(conn: db.DuckDBPyConnection) -> None:
            # Get current column names
            columns = conn.execute(
                "SELECT * FROM staging_data LIMIT 0"
            ).description
            old_names = [col[0] for col in columns]

            # Create new sanitized names
            new_names = [name.lower().replace(" ", "_") for name in old_names]

            # Build and execute rename statement if any changes needed
            if old_names != new_names:
                rename_pairs = [
                    f'"{old}" as "{new}"'
                    for old, new in zip(old_names, new_names)
                ]
                rename_sql = f"""
                    CREATE OR REPLACE TEMPORARY TABLE staging_data AS 
                    SELECT {', '.join(rename_pairs)}
                    FROM staging_data
                """
                conn.execute(rename_sql)

        def normalize_data(conn: db.DuckDBPyConnection) -> None:
            conn.execute(
                """
                UPDATE staging_data SET
                    city = TRIM(UPPER(city)),
                    address = TRIM(UPPER(address))
                """
            )

        return [sanitize_column_names, normalize_data]
