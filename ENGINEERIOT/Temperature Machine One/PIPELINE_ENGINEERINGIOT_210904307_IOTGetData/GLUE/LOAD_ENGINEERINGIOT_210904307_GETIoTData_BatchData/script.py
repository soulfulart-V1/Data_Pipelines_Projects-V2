import sys
import boto3
import datetime

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from awsglue.utils import getResolvedOptions

##aws services
s3_client = boto3.client('s3')

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'run_job_date', 'bucket_name'])

year = int(args['run_job_date'][0:4])
month = int(args['run_job_date'][5:7])
day = int(args['run_job_date'][8:10])

#global variables
date_to_extract =  datetime.datetime(year, month, day)
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
bucket_name = args['bucket_name']
partition_column = 'yearmonthday'
partition_value = used_year+used_month+used_day
input_root_path = 's3://'+bucket_name+'/STREAM/ENGINEERINGIOT/IOT_GET_DATA_210904307/REGION_A/PHYSICAL_VALUES/'
input_file_path =  input_root_path + partition_column+'='+partition_value+'/'
input_file_path_delete = input_root_path.replace('s3://'+bucket_name+'/','')
output_file_path = input_file_path.replace('STREAM', 'RAW')
output_file_path = output_file_path[:-1]

spark = SparkSession.builder.getOrCreate()

#EXTRACT
df = spark.read.csv(input_file_path)

#LOAD
df = df.coalesce(1)
df.write.option("header", "true").mode("overwrite").parquet(output_file_path)

#delete stream files
filter_client = s3_client.list_objects(Bucket=bucket_name, Prefix=input_file_path_delete)

keys_deleted=[]

for item in filter_client['Contents']:
    s3_client.delete_object(Bucket=bucket_name, Key=item['Key'])
    keys_deleted.append(item['Key'])