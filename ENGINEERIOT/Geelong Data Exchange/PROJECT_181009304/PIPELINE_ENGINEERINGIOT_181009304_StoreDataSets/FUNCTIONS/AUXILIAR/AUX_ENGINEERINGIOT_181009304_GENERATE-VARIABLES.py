import json
import time
import boto3
import datetime


#global variables
today_date_minus_one = datetime.date.today() - datetime.timedelta(days=1)
bucket_name = "soulfulart-data-lake"
number_retry = 3
delay_time = 1

#get year month and day separated
used_year = str(today_date_minus_one.year)

#avoid use numbers without left zero 
if (today_date_minus_one.month < 10):
    used_month = '0'+str(today_date_minus_one.month)
    
else: str(today_date_minus_one.month)

if (today_date_minus_one.day<10):
    used_day = '0'+str(today_date_minus_one.day)
else: used_day = str(today_date_minus_one.day)

s3 = boto3.client('s3') #define s3 service

def lambda_handler(event, context):
    
    
    PARTITION_VALUE = str(used_year) + str(used_month) + str(used_day)
    
    prefix_file = 'CURATED/ENGINEERINGIOT/GEELONGDATAEXCHANGE_181009304/TEMPERATURE_AND_HUMIDITY_181009304/DATA/'+PARTITION_VALUE
    
    #get csv flie name s3 key
    
    for retry in range (number_retry):
        try:
            input_file_path = s3.list_objects(Bucket=bucket_name, Prefix=prefix_file).get('Contents')[0]['Key']
        
        except NameError:
            print("Path not found!")
            time.sleep(delay_time)
    
    pipeline_parameters = {
        "BUCKET_NAME": bucket_name,
        "input_file_path": input_file_path,
        
        "PARTITION_VALUE": PARTITION_VALUE,
        "PORT": "3306",
        "REGION": "sa-east-1",
        "DATABASE": "engiot_database",
        "TABLE": "TEMP_HUM_HF_SENSORS",
        "PARTITION_COLUMN": "YEARMONTHDAY",
        "HOST": "db-warehouse-companyx.cl3tw5hr3lz9.sa-east-1.rds.amazonaws.com",
        "SECRET_NAME": "arn:aws:secretsmanager:sa-east-1:708253334587:secret:aws-rds-company-X-adm-z8eJkk"
}
    
    # TODO implement
    return {
        'Event' : pipeline_parameters
    }
