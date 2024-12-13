from building_damage.DamageReportPipeline import DamageReportPipeline
import duckdb as db

conn = db.connect("data/db/building_damage.db")
path = "data/raw/storm_damage_BIN.csv"

CSV_SCHEMA = (
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

TABLE_SCHEMA = CSV_SCHEMA + ", time_updated TIMESTAMP"

pipeline = DamageReportPipeline(conn, CSV_SCHEMA)

conn.execute("DROP TABLE test_storm_damage")
conn.execute("DROP TABLE test_storm_damage_invalid")
conn.execute("DROP TABLE staging_data")

# Create target table following schema
conn.execute(f"CREATE TABLE test_storm_damage ({TABLE_SCHEMA})")
conn.execute(f"CREATE TABLE test_storm_damage_invalid ({TABLE_SCHEMA})")

pipeline.process_csv(
    path, "test_storm_damage", "test_storm_damage_invalid"
)


