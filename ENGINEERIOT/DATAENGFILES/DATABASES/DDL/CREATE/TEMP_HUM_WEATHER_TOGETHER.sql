CREATE EXTERNAL TABLE IF NOT EXISTS TEMP_HUM_WEATHER_TOGETHER (

    device_id STRING,
    humidity_merged STRING,
    battery STRING,
    period STRING,
    time STRING,
    temperature_merged STRING,
    type varchar (10),
    record_timestamp STRING,
    recordid STRING,
    location_lat STRING,
    location_long STRING,
    device_street STRING,
    device_city STRING

)

PARTITIONED BY (YEARMONTHDAY VARCHAR (8))
STORED AS parquet
LOCATION 's3://soulfulart-data-lake/CURATED/ENGINEERINGIOT/GEELONGDATAEXCHANGE_181009304/WEATHER_TOGETHER/DATA/'