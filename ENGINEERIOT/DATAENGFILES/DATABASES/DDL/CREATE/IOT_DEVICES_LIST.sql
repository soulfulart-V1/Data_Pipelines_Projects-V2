CREATE EXTERNAL TABLE IF NOT EXISTS IOT_DEVICES_LIST (

    device_serial STRING,
    device_status STRING,
    device_use STRING,
    deviceid STRING,
    devicetype STRING,
    latitude STRING,
    location_parameters STRING,
    name STRING,
    type STRING,
    record_timestamp STRING,
    recordid STRING,
    location_lat STRING,
    location_lon STRING
)

PARTITIONED BY (YEARMONTHDAY VARCHAR (8))
STORED AS parquet
LOCATION 's3://soulfulart-data-lake/CURATED/ENGINEERINGIOT/GEELONGDATAEXCHANGE_181009304/IOT_DEVICES/DATA/'