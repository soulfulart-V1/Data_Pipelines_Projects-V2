import boto3
import pymysql

from pandas import read_csv
from pandas import to_numeric

def lambda_handler(event, context):
    
    s3 = boto3.client('s3') #define s3 service
    
    data_csv = s3.get_object(Bucket=event['Event']['BUCKET_NAME'], Key=event['Event']['input_file_path'])
    data_pandas = read_csv(data_csv.get("Body"))

    #add partition
    data_pandas[event['Event']['PARTITION_COLUMN']] = event['Event']['PARTITION_VALUE']
    
    # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=event['Event']['REGION']
    )

    get_secret_value_response = client.get_secret_value(
        SecretId=event['Event']['SECRET_NAME']
        )
    
    secret_dict = eval(get_secret_value_response['SecretString'])
    
    # Connection
    conn = pymysql.connect(
        host=event['Event']['HOST'],
        port=int(event['Event']['PORT']),
        database=event['Event']['DATABASE'],
        user=secret_dict['awsRDSUser'],
        password=secret_dict['awsRDSPass'],
        charset='utf8'
        )
        
    cursor = conn.cursor()
    
    # write dataframe to rds db
    write_to_mysql(data_pandas, cursor, event, conn) 
    
    conn.close()
    
    return {
        
        'message' : 'File: '+event['Event']['input_file_path']+' stored at database '+event['Event']['DATABASE']+' on table '+ event['Event']['TABLE']
        
    }
    
def write_to_mysql(dataframe, cursor, event, conn):
    
    sql_command = 'INSERT INTO ' + event['Event']['TABLE'] + ' VALUES '
    sql_value = ''
    
    for row_n in range(dataframe.shape[0]):
        
        sql_row = '('

        for item in dataframe.iloc[row_n]:

            sql_row = sql_row + "'" + str(item) + "'" + ','
    
        sql_row = sql_row[:len(sql_row)-1] 
        sql_row = sql_row + '),'  
        sql_value = sql_value + sql_row + '\n'

    sql_value = sql_value[:len(sql_value)-2]
    sql_value = sql_value + ';'
    
    sql_command = sql_command + sql_value
    
    cursor.execute(sql_command)

    conn.commit()