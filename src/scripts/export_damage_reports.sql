-- Join damage reports with building footprints and export to GeoJSON
LOAD SPATIAL;

CREATE TEMPORARY TABLE joined_data AS
SELECT *
FROM storm_damage AS sd
LEFT JOIN building_footprints AS bf
    ON sd.bin = bf.bin;

COPY joined_data TO 'data/processed/damage_reports.geojson' 
WITH (FORMAT GDAL, DRIVER 'GeoJSON');

DROP TABLE joined_data;
