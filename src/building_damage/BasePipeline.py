import logging
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Callable, Optional, List, Tuple, Dict

import duckdb as db

ValidationFunction = Callable[[db.DuckDBPyConnection], None]


class BasePipeline(ABC):
    def __init__(self, db_conn: db.DuckDBPyConnection, schema: str):
        """Initialize pipeline with database connection.

        Args:
            db_conn: DuckDB database connection
            schema: Table schema definition
        """
        self.conn = db_conn
        self.logger = logging.getLogger(__name__)
        self.schema = schema

    def validation_expression(self) -> str:
        """Build validation expression from column and row rules.

        Returns:
            SQL expression that evaluates to true for valid records
        """
        conditions = []

        for rule in self.column_validation_rules().values():
            conditions.append(rule)

        for rule in self.row_validation_rules().values():
            conditions.append(rule)

        # If no conditions, everything is valid
        if not conditions:
            return "TRUE"

        # Return combined validation expression
        return " AND ".join(conditions)

    @property
    def pre_validation_hooks(self) -> Optional[List[ValidationFunction]]:
        """Optional hooks to run before validation for data enrichment."""
        return None

    @property
    def post_validation_hooks(self) -> Optional[List[ValidationFunction]]:
        """Optional hooks to run after validation."""
        return None

    @abstractmethod
    def column_validation_rules(self) -> Dict[str, str]:
        """Mapping of column names to validation rules."""
        pass

    @abstractmethod
    def row_validation_rules(self) -> Dict[str, str]:
        """Mapping of row-level validation rules."""
        pass

    def process_csv(
            self,
            csv_path: str,
            target_table: str,
            invalid_table: str,
    ) -> Tuple[int, int]:
        """
        Process CSV through staging and validation into target table.
        Store invalid records in separate table.

        Args:
            csv_path: Path to input CSV
            target_table: Name of final table
            invalid_table: Name of table to store invalid records

        Returns:
            Tuple of (valid_count, invalid_count)
        """
        try:
            # Create temporary staging table
            self.conn.execute(
                f"""
                CREATE TEMPORARY TABLE staging_data (
                    {self.schema}
                )
            """
            )

            # Load data into staging table
            self.conn.execute(
                f"""
                INSERT INTO staging_data 
                SELECT * FROM read_csv('{csv_path}')
            """
            )

            # Run pre-validation hooks if any
            if self.pre_validation_hooks:
                for hook in self.pre_validation_hooks:
                    hook(self.conn)

            # Create target and invalid tables if they don't exist
            self.conn.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {target_table} AS 
                SELECT * FROM staging_data WHERE 1=0;
                
                CREATE TABLE IF NOT EXISTS {invalid_table} AS 
                SELECT * FROM staging_data WHERE 1=0;
            """
            )

            # Get validation expression
            validation_expr = self.validation_expression()

            # Run post-validation hooks if any
            if self.post_validation_hooks:
                for hook in self.post_validation_hooks:
                    hook(self.conn)

            # add an is_valid column to the staging table
            self.conn.execute(
                f"""
                ALTER TABLE staging_data
                ADD COLUMN is_valid BOOLEAN;
                """
            )

            # Update with the validation expression
            self.conn.execute(
                f"""
                UPDATE staging_data
                SET is_valid = {self.validation_expression()};
                """
            )

            # Add time_updated column to staging table
            self.conn.execute(
                f"""
                ALTER TABLE staging_data
                ADD COLUMN time_updated TIMESTAMP;
                """
            )

            # Update time_updated column
            self.conn.execute(
                f"""
                UPDATE staging_data
                SET time_updated = CURRENT_TIMESTAMP;
                """
            )

            # Insert valid and invalid records
            self.conn.execute(f"""
                INSERT INTO {target_table}
                SELECT * EXCLUDE(is_valid)
                FROM staging_data
                WHERE is_valid
            """)

            self.conn.execute(f"""
                INSERT INTO {invalid_table}
                SELECT * EXCLUDE(is_valid)
                FROM staging_data
                WHERE NOT is_valid
            """)

        # Clean up the staging table if there was an error
        except:
            self.conn.execute("DROP TABLE staging_data")
            raise


    def close(self):
        """Close database connection."""
        self.conn.close()
