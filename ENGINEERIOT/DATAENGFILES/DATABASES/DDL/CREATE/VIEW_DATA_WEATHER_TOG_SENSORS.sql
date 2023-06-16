CREATE OR REPLACE VIEW VIEW_DATA_WEATHER_TOG_SENSORS AS

SELECT 

WEATHER_TOG.device_id AS "Device Identifier", 
WEATHER_TOG.humidity_merged AS "Device Humidity Measure", 
WEATHER_TOG.battery AS "Device Battery",
WEATHER_TOG.temperature_merged AS "Device Temperature Measure",
WEATHER_TOG.record_timestamp AS "Measured Time",
WEATHER_TOG.location_lat AS "Device Latitude",
WEATHER_TOG.location_long AS "Device Longitude",
WEATHER_TOG.device_street AS "Device Street",
WEATHER_TOG.device_city AS "Device City",
CONCAT(SUBSTRING(WEATHER_TOG.YEARMONTHDAY, 5, 2), '/', SUBSTRING(WEATHER_TOG.YEARMONTHDAY, 7, 2), '/', SUBSTRING(WEATHER_TOG.YEARMONTHDAY, 1, 4)) AS "Data Stored Day", 
DEV_LIST.name AS "Device Location Name"

FROM TEMP_HUM_WEATHER_TOGETHER AS WEATHER_TOG

LEFT JOIN IOT_DEVICES_LIST AS DEV_LIST

ON DEV_LIST.deviceid = WEATHER_TOG.device_id