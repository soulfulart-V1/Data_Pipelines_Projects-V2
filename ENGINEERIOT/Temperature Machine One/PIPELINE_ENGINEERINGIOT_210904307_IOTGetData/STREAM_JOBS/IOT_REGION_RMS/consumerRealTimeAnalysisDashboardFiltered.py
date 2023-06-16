import json
import requests
from pandas import DataFrame

headers = {'Kafka-Auto-Offset-Reset': 'earliest'}
    
response = requests.get('https://famous-cowbird-12750-us1-rest-kafka.upstash.io/consume/GROUP_FILTER_DATA/GROUP_FILTER/IOT_FILTER_DATA_REAL_TIME_RMS',
                        headers=headers,
                        auth=('', ''))

raw_data = str(response.text)
raw_data = raw_data.replace("[ ]",'"[ ]"')
raw_data = raw_data.replace("'",'"')
raw_data = raw_data.replace("[",'')
raw_data = raw_data.replace("]",'')
raw_data = raw_data.replace(" : ",':')

raw_data = json.loads(raw_data, strict=False)

try:
    sample_schema_value = raw_data['Value']

except:
    sample_schema_value = raw_data['value']    

sample_schema_value = sample_schema_value.split('\n')

sample_schema_value_rows = []

for item in sample_schema_value:
    sample_schema_value_rows.append(' '.join(item.split()))

sample_schema_data_row = []

for item in sample_schema_value_rows:
    sample_schema_data_row.append(item.split(' '))

df_columns = sample_schema_data_row[0]
df_columns.insert(0, 'index')

sample_schema_data_row.pop(0)

final_values_df = DataFrame(sample_schema_data_row,
    columns = df_columns)

final_values_df = final_values_df.drop(columns='index')

print(final_values_df.head())