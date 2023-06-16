import datetime

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

#constants
#global variables
today_date_minus_one = datetime.date.today() - datetime.timedelta(days=1)

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
input_root_path = 's3://'+bucket_name+'/RAW/ENGINEERINGIOT/GEELONGDATAEXCHANGE_181009304/IOT_DEVICES/DATA/'
input_file_path =  input_root_path + today_file
output_file_path = input_root_path.replace('RAW', 'CURATED')
output_file_path = output_file_path+'yearmonthday='+PARTITION+'/'
spark = SparkSession.builder.getOrCreate()

#EXTRACT

df = spark.read.json(input_file_path, multiLine = 'true')

#TRANSFORMATION

df = df.select(F.explode('records'))

df_col  = df.select('col.*')

df.unpersist()

df_fields = df_col.select('fields.*', 'geometry.*', 'record_timestamp', 'recordid')

df_col.unpersist()

df_fields_location = df_fields.withColumn('location_lat', df_fields.location[0] )
df_fields_location = df_fields_location.withColumn('location_long', df_fields.location[1] )

df_fields_location.unpersist()

#drop duplicated or not necessary columns
df_fields_final = df_fields_location.drop('location')
df_fields_final = df_fields_final.drop('coordinates')
df_fields_final = df_fields_final.drop('longitude')

#convert all columns to string
for column in df_fields_final.columns:
    df_fields_final = df_fields_final.withColumn(column, df_fields_final[column].cast("String"))

#LOAD
df_fields_final.write.option("header", "true").mode("overwrite").parquet(output_file_path)