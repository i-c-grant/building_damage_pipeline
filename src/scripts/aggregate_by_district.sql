LOAD SPATIAL;

-- Create temporary table with damage locations joined to building footprints
CREATE TEMPORARY TABLE damage_locations AS
SELECT 
    sd.*,
    bf.geom
FROM storm_damage AS sd
JOIN building_footprints AS bf 
    ON sd.bin = bf.bin::VARCHAR;

-- Create temporary table with district aggregations
CREATE TEMPORARY TABLE district_counts AS
SELECT 
    cd.geom,
    COUNT(*) AS total_reports,
    SUM(CASE WHEN dl.no_electricity THEN 1 ELSE 0 END) AS power_outages,
    SUM(CASE WHEN dl.basement_flooded THEN 1 ELSE 0 END) AS flooded_basements,
    SUM(CASE WHEN dl.roof_damaged THEN 1 ELSE 0 END) AS roof_damage,
    SUM(CASE WHEN dl.insurance THEN 1 ELSE 0 END) AS with_insurance
FROM damage_locations AS dl
JOIN community_districts AS cd
ON ST_Within(dl.geom, cd.geom)
GROUP BY cd.geom;

-- Update the types to INT32 for the aggregation columns
-- (DuckDB likes to make huge ints)
-- Surely there is a better way to do this
ALTER TABLE district_counts ALTER total_reports SET DATA TYPE INT32;
ALTER TABLE district_counts ALTER power_outages SET DATA TYPE INT32;
ALTER TABLE district_counts ALTER flooded_basements SET DATA TYPE INT32;
ALTER TABLE district_counts ALTER roof_damage SET DATA TYPE INT32;
ALTER TABLE district_counts ALTER with_insurance SET DATA TYPE INT32;

-- Export results to GeoJSON
COPY district_counts 
TO 'data/processed/district_damage_counts.geojson'
WITH (FORMAT GDAL, DRIVER 'GeoJSON');

-- Cleanup
DROP TABLE damage_locations;
DROP TABLE district_counts;
