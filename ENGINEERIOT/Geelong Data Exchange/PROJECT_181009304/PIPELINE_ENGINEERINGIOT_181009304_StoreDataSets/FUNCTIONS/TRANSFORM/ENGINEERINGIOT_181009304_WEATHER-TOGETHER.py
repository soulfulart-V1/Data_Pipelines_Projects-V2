import sys
import boto3
import datetime

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from awsglue.utils import getResolvedOptions

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'run_job_date'])

year = int(args['run_job_date'][0:4])
month = int(args['run_job_date'][5:7])
day = int(args['run_job_date'][8:10])

date_to_extract =  datetime.datetime(year, month, day)

#constants
#global variables
today_date_minus_one = date_to_extract - datetime.timedelta(days=1)

#get year month and day separated
used_year = str(today_date_minus_one.year)

#avoid use numbers without left zero 
if (today_date_minus_one.month < 10):
    used_month = '0'+str(today_date_minus_one.month)
    
else: used_month = str(today_date_minus_one.month)

if (today_date_minus_one.day<10):
    used_day = '0'+str(today_date_minus_one.day)
    
else: used_day = str(today_date_minus_one.day)

#input parameters
bucket_name = 'soulfulart-data-lake'
PARTITION = used_year+used_month+used_day
today_file = used_year+used_month+used_day+'.json'
input_root_path = 's3://'+bucket_name+'/RAW/ENGINEERINGIOT/GEELONGDATAEXCHANGE_181009304/WEATHER_TOGETHER/DATA/'
input_file_path =  input_root_path + today_file
output_file_path = input_file_path.replace('RAW', 'CURATED')
output_file_path = output_file_path.replace('.json', '')

spark = SparkSession.builder.getOrCreate()

#EXTRACT

df = spark.read.json(input_file_path, multiLine = 'true')

#TRANSFORMATION

df = df.select(F.explode('records'))

df_col  = df.select('col.*')

df.unpersist()

df_fields = df_col.select('fields.*', 'geometry.*', 'record_timestamp', 'recordid')

df_col.unpersist()

df_fields_location = df_fields.withColumn('location_lat', df_fields.device_location[0] )
df_fields_location = df_fields_location.withColumn('location_long', df_fields.device_location[1] )

df_col.unpersist()

#split name into city and stret
df_fields_final = df_fields_location.withColumn('device_street', F.split(df_fields.device_name, ", ")[0] )
df_fields_final = df_fields_final.withColumn('device_city', F.split(df_fields.device_name, ", ")[1] )

df_fields_location.unpersist()

#drop duplicated or not necessary columns
df_fields_final = df_fields_final.drop('name')
df_fields_final = df_fields_final.drop('location')
df_fields_final = df_fields_final.drop('coordinates')
df_fields_final = df_fields_final.drop('metadata_time')
df_fields_final = df_fields_final.drop('payload_fields_temp')

#convert all columns to string
for column in df_fields_final.columns:
    df_fields_final = df_fields_final.withColumn(column, df_fields_final[column].cast("String"))

#LOAD
df_fields_final.write.option("header", "true").mode("overwrite").parquet(output_file_path)

#WRITE TO ATHENA TABLE

client_athena = boto3.client('athena')
SQL_QUERY = """ALTER TABLE TEMP_HUM_WEATHER_TOGETHER ADD IF NOT EXISTS PARTITION (YEARMONTHDAY="""+PARTITION+""") LOCATION 's3://soulfulart-data-lake/CURATED/ENGINEERINGIOT/GEELONGDATAEXCHANGE_181009304/WEATHER_TOGETHER/DATA/"""+PARTITION+"""/'"""

queryStart = client_athena.start_query_execution(
    QueryString = SQL_QUERY,
    QueryExecutionContext = {
        'Database': 'geelongdataexchange'
    },
    ResultConfiguration = { 'OutputLocation': 's3://aws-glue-scripts-708253334587-sa-east-1/ATHENA_QUERY/WEATHER_TOGETHER'}
    )