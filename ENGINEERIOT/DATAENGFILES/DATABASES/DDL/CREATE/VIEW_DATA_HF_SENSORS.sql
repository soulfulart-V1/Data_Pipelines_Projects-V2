CREATE OR REPLACE VIEW VIEW_DATA_HF_SENSORS AS

SELECT 

HF_SENSOR.dev_id AS "Device Identifier", 
HF_SENSOR.humidity AS "Device Humidity Measure", 
HF_SENSOR.payload_fields_battery AS "Device Battery",
HF_SENSOR.temperature AS "Device Temperature Measure",
HF_SENSOR.record_timestamp AS "Measured Time",
HF_SENSOR.location_lat AS "Device Latitude",
HF_SENSOR.location_long AS "Device Longitude",
HF_SENSOR.street AS "Device Street",
HF_SENSOR.city AS "Device City",
CONCAT(SUBSTRING(HF_SENSOR.YEARMONTHDAY, 5, 2), '/', SUBSTRING(HF_SENSOR.YEARMONTHDAY, 7, 2), '/', SUBSTRING(HF_SENSOR.YEARMONTHDAY, 1, 4)) AS "Data Stored Day", 
DEV_LIST.name AS "Device Location Name"


FROM TEMP_HUM_HF_SENSORS AS HF_SENSOR

LEFT JOIN IOT_DEVICES_LIST AS DEV_LIST

ON DEV_LIST.deviceid = HF_SENSOR.dev_id