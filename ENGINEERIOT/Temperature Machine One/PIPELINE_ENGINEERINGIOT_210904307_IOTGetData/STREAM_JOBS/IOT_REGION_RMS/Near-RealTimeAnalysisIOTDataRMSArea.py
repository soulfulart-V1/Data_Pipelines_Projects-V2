import json
import requests


from time import sleep
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

MESSAGES_BY_GET = 10
MESSAGES_DELAY = 1

spark = (SparkSession
    .builder
    .getOrCreate())

sc = spark.sparkContext

headers = {'Kafka-Auto-Offset-Reset': 'earliest'}
    
response = requests.get('https://famous-cowbird-12750-us1-rest-kafka.upstash.io/consume/GROUP_NAME/GROUP_INSTANCE_NAME/IOT_DATA_MICROREGION_RMS',
                        headers=headers,
                        auth=('', ''))

messages = response.text

raw_data_rdd = sc.parallelize([messages])
raw_dataframe = spark.read.json(raw_data_rdd)

try:
    sample_schema_value = raw_dataframe.collect()[0]['value']

except: 
    print("No new messages with value fields!")

else:
    raw_dataframe = raw_dataframe.select('value')

    raw_dataframe_columns = F.split(raw_dataframe['value'],'"')

    schema_dict = {
        
        'device_id': None,
        'temp': None,
        'humidity': None,
        'recorded_time_device': None,
        'sound_intensity': None,
        'solar_panel_volt': None

    }

    sample_schema_value = sample_schema_value.split('"')

    offset = 2 #the value will be after a :

    for key in schema_dict:
        schema_dict[key] = sample_schema_value.index(key) + offset

    df_values_final = raw_dataframe.withColumn('device_id', raw_dataframe_columns.getItem(schema_dict['device_id']))
    df_values_final = df_values_final.withColumn('temp', raw_dataframe_columns.getItem(schema_dict['temp']))
    df_values_final = df_values_final.withColumn('humidity', raw_dataframe_columns.getItem(schema_dict['humidity']))
    df_values_final = df_values_final.withColumn('recorded_time_device', raw_dataframe_columns.getItem(schema_dict['recorded_time_device']))
    df_values_final = df_values_final.withColumn('sound_intensity', raw_dataframe_columns.getItem(schema_dict['sound_intensity']))
    df_values_final = df_values_final.withColumn('solar_panel_volt', raw_dataframe_columns.getItem(schema_dict['solar_panel_volt']))
    df_values_final = df_values_final.drop('value')

    #write filtered data to analysis topic
    filtered_message = df_values_final.toPandas()

    del df_values_final

    number_of_messages = filtered_message.shape[0]
    batch_number = 0
    
    for start_message in range (0, number_of_messages-1, MESSAGES_BY_GET):
        
        end_message = start_message + MESSAGES_BY_GET - 1
        if end_message >= number_of_messages:
            end_message = number_of_messages - 1

        message_batch_get = filtered_message.iloc[start_message:end_message]

        message_batch_get = message_batch_get.to_string()

        response = requests.get('https://famous-cowbird-12750-us1-rest-kafka.upstash.io/produce/IOT_FILTER_DATA_REAL_TIME_RMS/'+message_batch_get,
                            auth=('', ''))

        batch_number+=1

        sleep(MESSAGES_DELAY)

    print(response.text)