import boto3
import time

"""
Find an explanation of this code here: 
https://matthias-funke.medium.com/athena-partitioning-for-cloud-trail-logs-228eb214d809
"""

start = time.time()
# Location should not end with a slash, just CloudTrail
location = "s3://aws-cloudtrail-logs-255149284406-4cec155e/AWSLogs/255149284406/CloudTrail"
# results only needed for the last part (where we validate the query worked)
results = "s3://com.alationpro/athena"
region = "us-west-2"
database = "default"
cloudtrail_table = "cloudtrail_new"
start_year = 2020
end_year = 2020

my_session = boto3.session.Session()

glue = my_session.client('glue', region_name=region)

table_creation_details = {
  "StorageDescriptor": {
    "Columns": [
      {
        "Name": "eventversion",
        "Type": "string"
      },
      {
        "Name": "useridentity",
        "Type": "struct<type:string,principalid:string,arn:string,accountid:string,invokedby:string,accesskeyid:string,userName:string,sessioncontext:struct<attributes:struct<mfaauthenticated:string,creationdate:string>,sessionIssuer:struct<type:string,principalId:string,arn:string,accountId:string,userName:string>>>"
      },
      {
        "Name": "eventtime",
        "Type": "string"
      },
      {
        "Name": "eventsource",
        "Type": "string"
      },
      {
        "Name": "eventname",
        "Type": "string"
      },
      {
        "Name": "awsregion",
        "Type": "string"
      },
      {
        "Name": "sourceipaddress",
        "Type": "string"
      },
      {
        "Name": "useragent",
        "Type": "string"
      },
      {
        "Name": "errorcode",
        "Type": "string"
      },
      {
        "Name": "errormessage",
        "Type": "string"
      },
      {
        "Name": "requestparameters",
        "Type": "string"
      },
      {
        "Name": "responseelements",
        "Type": "string"
      },
      {
        "Name": "additionaleventdata",
        "Type": "string"
      },
      {
        "Name": "requestid",
        "Type": "string"
      },
      {
        "Name": "eventid",
        "Type": "string"
      },
      {
        "Name": "resources",
        "Type": "array<struct<ARN:string,accountId:string,type:string>>"
      },
      {
        "Name": "eventtype",
        "Type": "string"
      },
      {
        "Name": "apiversion",
        "Type": "string"
      },
      {
        "Name": "readonly",
        "Type": "string"
      },
      {
        "Name": "recipientaccountid",
        "Type": "string"
      },
      {
        "Name": "serviceeventdetails",
        "Type": "string"
      },
      {
        "Name": "sharedeventid",
        "Type": "string"
      },
      {
        "Name": "vpcendpointid",
        "Type": "string"
      }
    ],
    "Location": location,
    "InputFormat": "com.amazon.emr.cloudtrail.CloudTrailInputFormat",
    "OutputFormat": "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
    "Compressed": True,
    "NumberOfBuckets": -1,
    "SerdeInfo": {
      "SerializationLibrary": "com.amazon.emr.hive.serde.CloudTrailSerde",
      "Parameters": {
        "serialization.format": "1"
      }
    },
    "BucketColumns": [],
    "SortColumns": [],
    "Parameters": {"Classification" : "cloudtrail"},
    "SkewedInfo": {
      "SkewedColumnNames": [],
      "SkewedColumnValues": [],
      "SkewedColumnValueLocationMaps": {}
    },
    "StoredAsSubDirectories": False
  },
  "PartitionKeys": [
    {
      "Name": "region",
      "Type": "string"
    },
    {
      "Name": "year",
      "Type": "string"
    },
    {
      "Name": "month",
      "Type": "string"
    },
    {
      "Name": "day",
      "Type": "string"
    }
  ]
}

try:
    response = glue.delete_table(
        DatabaseName=database,
        Name=cloudtrail_table,
    )
except:
    print(f"Unable to delete {cloudtrail_table}")

response = glue.create_table(
    DatabaseName=database,
    TableInput=dict(
        Name=cloudtrail_table,
        Description='Developer: matthias.funke@alation.com',
        StorageDescriptor=table_creation_details.get('StorageDescriptor'),
        PartitionKeys=table_creation_details.get('PartitionKeys'),
        Parameters=dict(classification="cloudtrail"),
        TableType="EXTERNAL_TABLE"
    )

)
from calendar import monthrange
PartitionInputList = []

for year in range(start_year, end_year+1):
    for month in range(1, 12 + 1):
        for day in range(1, 1 + monthrange(year, month)[1]):
            my_location = f"{location}/{region}/{year}/{month:02}/{day:02}/"
            Values = [region, f'{year}', f'{month:02}', f'{day:02}']
            StorageDescriptor = table_creation_details.get('StorageDescriptor').copy()
            StorageDescriptor['Location'] = my_location
            PartitionInputList.append(dict(
                Values=Values,
                StorageDescriptor=StorageDescriptor
            ))

max_per_call = 100
batches = len(PartitionInputList) // max_per_call
for i in range(0, batches):
    print(f"Working on Batch {i} of {batches}")
    response = glue.batch_create_partition(
        DatabaseName=database,
        TableName=cloudtrail_table,
        PartitionInputList=PartitionInputList[i*max_per_call : i*max_per_call + max_per_call]
    )

response = glue.batch_create_partition(
    DatabaseName=database,
    TableName=cloudtrail_table,
    PartitionInputList=PartitionInputList[(i+1)*max_per_call : ])

from math import ceil

print(f"Created {cloudtrail_table} in {region} with {len(PartitionInputList)} partitions.")
print(f'Time: {ceil(time.time() - start)} secs.')

query_text = f"""
SELECT eventname, substr(eventtime, 1, 10) as eventdate,count(*)
FROM {database}.{cloudtrail_table}
WHERE region='{region}' 
and year=date_format(date_add('day',-1,current_date),'%Y') 
and month=date_format(date_add('day',-1,current_date),'%m')
and day=date_format(date_add('day',-1,current_date),'%d')
and eventname='StartQueryExecution'
GROUP BY 1, 2
ORDER BY 1, 2
"""

athena = my_session.client('athena', region_name=region)

response = athena.create_named_query(
    Name='Athena QLI Test Query',
    Description='Developer: matthias.funke@alation.com',
    Database=database,
    QueryString=query_text,
    ClientRequestToken=f"{hash(query_text):32}"
)

import uuid

my_query_id = response['NamedQueryId']

print(f"Saved test query with ID {my_query_id}")

response = athena.start_query_execution(
    QueryString=query_text,
    ClientRequestToken=str(uuid.uuid1()),
    QueryExecutionContext={
        'Database': database
    },
    ResultConfiguration={
        'OutputLocation': results,
    }
)

my_query_execution_id = response['QueryExecutionId']

for j in range(10):
    try:
        print(f"Get execution results for {my_query_execution_id}")
        response = athena.get_query_results(
            QueryExecutionId=my_query_execution_id
        )
        break
    except:
        time.sleep(10)

my_result = response.get('ResultSet').get('Rows')
print(f"Your query ran and produced: {my_result}")
