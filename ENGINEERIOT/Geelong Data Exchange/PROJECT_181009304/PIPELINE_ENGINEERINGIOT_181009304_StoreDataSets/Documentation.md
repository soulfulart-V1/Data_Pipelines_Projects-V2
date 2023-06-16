Main characteriscts:
Trigger: Daily at 11 GTC
First INPUT: URL request
Last OUTPUT: SQL table
PARITION: anomedia

<div style="text-align: justify">

This pipeline has a goal of extracting and url request and store its data to a SQL database (warehouse)

It will store json response from the API https://www.geelongdataexchange.com.au/explore/dataset/rising-hf/api/?sort=metadata_time to our data lake in structured format.

The full pipeline is shown in the figure  


<p align="center">

![alt text](https://github.com/soulfulart-V1/Data_Pipelines_Projects/blob/main/Geelong%20Data%20Exchange/ENGINEERINGIOT/PROJECT_181009304/RESOURCES/IMAGES/PIPELINE_ENGINEERINGIOT_181009304_Temperature_Humidity_HF_sensorsInfo.png?raw=true)

</p>

The first task EXTRACT is a LAMDBA located at /FUNCTIONS/EXTRACT/EXTRACT_ENGINEERINGIOT_181009304_GET-JSON-URL.py that gets an URL response and store it on our data lake which is a S3 bucket on the path 'RAW/ENGINEERINGIOT/GEELONGDATAEXCHANGE_181009304/TEMPERATURE_AND_HUMIDITY_181009304/DATA/PARTITIONDAY'

The second task is a GLUE job that filter the json file stored by the previous task. This job takes only the necessary data from the json file and stores it as CSV file on the path 'CURATED/ENGINEERINGIOT/GEELONGDATAEXCHANGE_181009304/TEMPERATURE_AND_HUMIDITY_181009304/DATA/PARTITIONDAY'

The third task creates variables that will be used by the next task. It finds the name of the CSV file stored by the GLUE JOB.

The fourth task get the csv file sent by the previous task and write it to a sql table.

 Resources:

    + TRIGGER: arn:aws:events:sa-east-1:708253334587:rule/TRIGGER_EXTRACT_ENGINEERINGIOT_181009304_GET-JSON-URL

    + VPC: 

        -Endpoints:

            -vpce-0c1bca9b6087e51c9: allow secret accesss from lambda function on task "JSON to SQL Table TEMP_HUM_HF_SENSORS"

        -Route tables:

            -rtb-06e21756795843c03: allow S3 access from lambda function on task "JSON to SQL Table TEMP_HUM_HF_SENSORS"

Monthly cost:

    + RDS instance: 30 USD

    + GLUE JOBS: 2 USD

</div>