CREATE OR REPLACE VIEW VIEW_AVG_MEASURES AS

WITH AVG_FINAL_TABLE AS (

SELECT 

HF_SENSOR."Device Humidity Measure", 
HF_SENSOR."Device Temperature Measure",
HF_SENSOR."Device Latitude",
HF_SENSOR."Device Longitude",
HF_SENSOR."Device Street",
HF_SENSOR."Device City",
HF_SENSOR."Device Location Name",
HF_SENSOR."Data Stored Day"

FROM VIEW_DATA_HF_SENSORS AS HF_SENSOR

UNION ALL

SELECT 

WEATHER_TOG."Device Humidity Measure", 
WEATHER_TOG."Device Temperature Measure",
WEATHER_TOG."Device Latitude",
WEATHER_TOG."Device Longitude",
WEATHER_TOG."Device Street",
WEATHER_TOG."Device City",
WEATHER_TOG."Device Location Name",
WEATHER_TOG."Data Stored Day"

FROM VIEW_DATA_WEATHER_TOG_SENSORS AS WEATHER_TOG

)

SELECT 

ROUND(AVG(CAST("Device Humidity Measure" AS DOUBLE)), 2) AS "Average Humidity",
ROUND(AVG(CAST("Device Temperature Measure" AS DOUBLE)), 2) AS "Average Temperature",
"Device Location Name",
"Data Stored Day"

FROM AVG_FINAL_TABLE

GROUP BY "Device Location Name", "Data Stored Day"

ORDER BY "Device Location Name", "Data Stored Day"