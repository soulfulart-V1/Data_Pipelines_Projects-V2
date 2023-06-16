CREATE EXTERNAL TABLE IF NOT EXISTS TEMP_HUM_HF_SENSORS (

    dev_id STRING,
    humidity STRING,
    payload_fields_battery STRING,
    payload_fields_hum STRING,
    payload_fields_period STRING,
    temperature STRING,
    type varchar (10),
    record_timestamp STRING,
    recordid STRING,
    location_lat STRING,
    location_long STRING,
    street STRING,
    city STRING

)

PARTITIONED BY (YEARMONTHDAY VARCHAR (8))
STORED AS parquet
LOCATION 's3://soulfulart-data-lake/CURATED/ENGINEERINGIOT/GEELONGDATAEXCHANGE_181009304/TEMPERATURE_AND_HUMIDITY_181009304/DATA/'