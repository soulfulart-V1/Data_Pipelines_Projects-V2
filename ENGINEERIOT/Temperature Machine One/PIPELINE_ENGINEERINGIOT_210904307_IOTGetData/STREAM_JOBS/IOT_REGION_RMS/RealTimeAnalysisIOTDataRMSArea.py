import os
import json
import requests

from time import sleep
from pyspark.sql.types import *
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

MESSAGES_BY_GET = 10
MESSAGES_DELAY = 5

spark = (SparkSession
    .builder
    .getOrCreate())
7
sc = spark.sparkContext

# The schema is encoded in a string.
schemaString = "topic partition offset timestamp key value headers"

sdf_schema = [StructField(field_name, StringType(), True) for field_name in schemaString.split()]
sdf_schema = StructType(sdf_schema)

raw_data_sdf = spark.readStream.\
    format("kafka").\
    option("kafka.bootstrap.servers", "").\
    option("kafka.sasl.mechanism", "SCRAM-SHA-256").\
    option("kafka.security.protocol", "SASL_SSL").\
    option("kafka.sasl.jaas.config", "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"\" password=\"\";").\
    option("kafka.key.serializer", "org.apache.kafka.common.serialization.StringSerializer").\
    option("kafka.value.serializer", "org.apache.kafka.common.serialization.StringSerializer").\
    option("kafka.group.id", "$GROUP_NAME").\
    option("subscribe", "IOT_DATA_MICROREGION_RMS").\
    load()

query = raw_data_sdf\
    .writeStream\
    .queryName("values_raw")\
    .outputMode("append")\
    .format("memory")\
    .start()

for i in range (10):  

    data_frame = spark.sql("SELECT count(value) as num_of_values FROM values_raw")

    data_frame.show()

    sleep(MESSAGES_DELAY)

query.stop()