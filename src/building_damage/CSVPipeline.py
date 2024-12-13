import logging
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Callable, Dict, List, Optional, Tuple

import duckdb

ValidationFunction = Callable[[duckdb.DuckDBPyConnection], None]


class CSVPipeline(ABC):
    def __init__(self, db_path: str):
        """Initialize pipeline with database connection.

        Args:
            db_path: Path to DuckDB database file
        """
        self.conn = duckdb.connect(db_path)
        self.logger = logging.getLogger(__name__)

    @abstractmethod
    def column_validation_rules(self) -> Dict[str, str]:
        """Define single-column validation rules.

        Returns:
            Dict mapping column names to SQL validation expressions that evaluate
            to true for invalid values
        """
        pass

    @abstractmethod
    def row_validation_rules(self) -> List[Tuple[str, str]]:
        """Define multi-column row-level validation rules.

        Returns:
            List of (rule_name, sql_expression) tuples where sql_expression
            evaluates to true for invalid rows
        """
        pass

    @property
    def pre_validation_hooks(self) -> Optional[List[ValidationFunction]]:
        """Optional hooks to run before validation for data enrichment."""
        return None

    @property
    def post_validation_hooks(self) -> Optional[List[ValidationFunction]]:
        """Optional hooks to run after validation."""
        return None

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
                CREATE TEMPORARY TABLE staging_data AS 
                SELECT * FROM read_csv('{csv_path}')
                WHERE 1=0
            """
            )

            # Load data
            self.conn.execute(
                f"""
                INSERT INTO staging_data 
                SELECT * FROM read_csv('{csv_path}')
            """
            )

            # Run pre-validation hooks if any (e.g. data enrichment and normalization)
            if self.pre_validation_hooks:
                for hook in self.pre_validation_hooks:
                    hook(self.conn)

            # Build validation query
            validation_conditions = []

            # Add column validations
            for column, rule in self.column_validation_rules().items():
                validation_conditions.append(
                    f"WHEN NOT {rule} THEN '{column}_invalid'"
                )

            # Add row validations
            for rule_name, rule_expr in self.row_validation_rules():
                validation_conditions.append(
                    f"WHEN NOT {rule_expr} THEN 'invalid: {rule_name}'"
                )

            validation_case = f"""
                array_agg(CASE 
                    {' '.join(validation_conditions)}
                END) FILTER (WHERE CASE 
                    {' '.join(validation_conditions)}
                END IS NOT NULL) as validation_errors
            """

            # Create invalid records table if it doesn't exist
            self.conn.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {invalid_table} (
                    record_data JSON,
                    validation_errors JSON,
                    insert_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """
            )

            # Insert invalid records with all their validation errors
            invalid_count = self.conn.execute(
                f"""
                WITH validation AS (
                    SELECT *,
                    {validation_case}
                    FROM staging_data
                    GROUP BY *
                )
                INSERT INTO {invalid_table} (record_data, validation_errors)
                SELECT 
                    to_json(struct_pack(*)) EXCLUDE(validation_errors) as record_data,
                    validation_errors
                FROM validation 
                WHERE validation_errors IS NOT NULL
                RETURNING COUNT(*)
            """
            ).fetchone()[0]

            if invalid_count:
                self.logger.warning(
                    f"Found {invalid_count} invalid records - written to {invalid_table}"
                )

            # Run post-validation hooks if any
            if self.post_validation_hooks:
                for hook in self.post_validation_hooks:
                    hook(self.conn)

            # Insert valid records
            valid_count = self.conn.execute(
                f"""
                WITH validation AS (
                    SELECT *,
                    {validation_case}
                    FROM staging_data
                    GROUP BY *
                )
                INSERT INTO {target_table}
                SELECT * EXCLUDE(validation_errors)
                FROM validation 
                WHERE validation_errors IS NULL
                RETURNING COUNT(*)
            """
            ).fetchone()[0]

            return valid_count, invalid_count

        finally:
            pass  # Temporary table will be cleaned up automatically

    def close(self):
        """Close database connection."""
        self.conn.close()
