import json
import boto3
import urllib
import datetime
import requests

#global objects
s3 = boto3.client('s3') #define s3 service

def lambda_handler(event, context):
    
    year = int(event['time'][0:4])
    month = int(event['time'][5:7])
    day = int(event['time'][8:10])

    date_to_extract =  datetime.datetime(year, month, day)
    
    #date to extract
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

    base_url = 'https://www.geelongdataexchange.com.au/api/records/1.0/search/?'
    
    parameters = {
        
        'dataset' : 'weather-together-temperature-and-humidity',
        'refine.metadata_time' : used_year+'/'+used_month+'/'+used_day,
        'rows' : '2500'
        
    }
    
    url_parameters = ''
    
    for item in parameters:
        url_parameters = url_parameters+item+'='+urllib.parse.quote(parameters[item], safe='')+'&'
    
    url_parameters = url_parameters[:-1] #remove last &

    url_request = base_url + url_parameters
    request_data = requests.get(url_request)
    data_txt = request_data.text
    
    object_path = 'RAW/ENGINEERINGIOT/GEELONGDATAEXCHANGE_181009304/WEATHER_TOGETHER/DATA/'
    objetct_key = object_path+used_year+used_month+used_day+'.json'
    
    s3.put_object(Body=data_txt, Bucket='soulfulart-data-lake', Key=objetct_key)
    
    return {
        
        'message' : 'Request: refine.metadata_time='+str(today_date_minus_one)+' stored at '+objetct_key,
        'time' : str(date_to_extract)
        
    }