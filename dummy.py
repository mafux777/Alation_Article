import requests
import json
import random
from datetime import datetime, timezone

# Prepare the REST API call parameters
host = "http://2022-1-sandbox.alationproserv.com"
headers = dict(token='juxBQUUiHOp7nXz-Hweh5fXkXcUIIynFhaCopxw-tLE')

# Create a data source
url = host + "/integration/v1/datasource/"

# Create a random title
random = ''.join(random.sample("ABCDEFGHJKLMNPQRSTUVWXYZ0123456789", 4))
print(f"Random file key: {random}")

# Parameters for creating the data source
params=dict(dbtype="postgresql", title=f"My API DS {random}", is_virtual=True, deployment_setup_complete=True)
r = requests.post(url=url, headers=headers, json=params, verify=False)

# get the status
status = r.json()
# extract the data source ID
ds_id = status['id']
print(f"Created data source: {host}/data/{ds_id}/")

#with open("nifi.json") as f:
#    j = json.load(f)

# --- Prepare sample data for the virtual data source
sample_data = [
    # cia_two.person p, cia_two.personname pn, cia_two.identifier
    {"key": f"{ds_id}.cia_two"},
    {"key": f"{ds_id}.cia_two.person", "table_type": "TABLE"},
    {"key": f"{ds_id}.cia_two.personname", "table_type": "TABLE"},
    {"key": f"{ds_id}.cia_two.identifier", "table_type": "TABLE"},
]

body = map(json.dumps, sample_data)
data = "\n".join(body)

url = f"{host}/api/v1/bulk_metadata/extraction/{ds_id}"
r = requests.post(url=url, headers=headers, data=data, params=dict(remove_not_seen=True))

# -- Get the status of the bulk upload job ---
status = (json.loads(r.content))
params=dict(name=status['job_name'].replace("#", "%23"))
url_job = host + "/api/v1/bulk_metadata/job/?name=" + params['name']

while(True):
    r_2 = requests.get(url=url_job, headers=headers)
    status = (json.loads(r_2.content))
    if status['status']!='running':
        objects = json.loads(status['result'])['error_objects']
        if objects:
            for error in objects:
                print (error)
        else:
            print (status)
        break

# We don't have an API for creating a file system, please do this beforehand and note down the ID
id=1
file_api = f"{host}/api/v1/bulk_metadata/file_upload/{id}/"
# bucket = j['processors'][1]['component']['config']['properties']['Bucket']
bucket = "s3-test-bucket"
files = [
            dict(path=f"/",
                 name=f"{bucket}",
                 is_directory=True,
                 ts_last_modified=datetime.now(timezone.utc).strftime(r"%Y-%m-%d %H:%M:%S.%f"),
                 ts_last_accessed=datetime.now(timezone.utc).strftime(r"%Y-%m-%d %H:%M:%S.%f"),
                 ),
            dict(path=f"/{bucket}/",
                 name="File_1",
                 is_directory=False,
                 ts_last_modified=datetime.now(timezone.utc).strftime(r"%Y-%m-%d %H:%M:%S.%f"),
                 ts_last_accessed=datetime.now(timezone.utc).strftime(r"%Y-%m-%d %H:%M:%S.%f"),
                 ),
]

files_json = "\n".join([json.dumps(f) for f in files])
headers['content-type'] = 'application/json'
r = requests.post(url=file_api, headers=headers, data=files_json, params=dict(remove_not_seen=True))


url = host + '/integration/v2/lineage/'
body = {
    "dataflow_objects": [
        {
            "external_id": f"api/transform_01_{ds_id}",
            "title" : f"{random} API dataflow",
            "content": "Transformation refers to the cleansing and aggregation that may need to happen to data to prepare it for analysis."
        },
        {
            "external_id": f"api/visualize_{ds_id}",
            "title": f"{random} Visualization",
            "content": "send data to Tableau",
        },
    ],
    "paths": [
        [
            # Inputs for the dataflow
            [{"otype": "directory", "key": f"{id}./{files[0]['name']}/"}],
            # The DataFlow
            [{"otype": "dataflow", "key": f"api/transform_01_{ds_id}"}],
            # Outputs
            [{"otype": "table", "key": f"{ds_id}.cia_two.person"},
             {"otype": "table", "key": f"{ds_id}.cia_two.personname"},
             {"otype": "table", "key": f"{ds_id}.cia_two.identifier"}],
        ],
        [
            # Inputs for the dataflow
            [{"otype": "table", "key": f"{ds_id}.cia_two.personname"}],
            # The DataFlow
            [{"otype": "dataflow", "key": f"api/visualize_{ds_id}"}],
            # Outputs
            [{"otype": "bi_report", "key": "1.bi_report.LFRJ1Theta Token"},
             {"otype": "bi_report", "key": "1.bi_report.LFRJ1Uniswap"},
            ],
        ],
    ]
}

# Send the above to the Lineage V2 API
r = requests.post(url=url, headers=headers, json=body)

# -- Get the status and print it
status = r.json()
params=dict(id=status['job_id'])
url_job = host + "/api/v1/bulk_metadata/job/"
while(True):
    r_2 = requests.get(url=url_job, headers=headers, params=params)
    status = r_2.json()
    if status['status']!='running':
        objects = status['result']
        if objects:
            print (objects)
        else:
            print (status)
        break




