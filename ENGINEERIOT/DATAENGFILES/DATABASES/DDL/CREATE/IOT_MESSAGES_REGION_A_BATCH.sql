CREATE EXTERNAL TABLE IF NOT EXISTS ENGIOT.IOT_MESSAGES_REGION_A_BATCH (

    device_id STRING,
    device_class STRING,
    heart_pulse STRING,
    speed STRING,
    calories STRING,
    pressure_body STRING,
    temp STRING,
    humidity STRING,
    pressure_air STRING,
    recorded_time_device STRING,
    stream_time STRING,
    sound_intensity STRING,
    other_data STRING
    
    )

PARTITIONED BY(yearmonthday int)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE

LOCATION 's3://soulfulart-data-lake/RAW/ENGINEERINGIOT/IOT_GET_DATA_210904307/REGION_A/PHYSICAL_VALUES/'