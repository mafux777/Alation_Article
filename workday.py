import requests
import json
import random
import pandas as pd
from config import args


# Create a data source
url = args['host'] + "/integration/v1/datasource/"
headers = dict(token=args['token'])
host = args['host']
# Create a random title
random = ''.join(random.sample("ABCDEFGHJKLMNPQRSTUVWXYZ0123456789", 4))

# Parameters for creating the data source
params=dict(dbtype="postgresql", title=f"Workday DS {random}", is_virtual=True, deployment_setup_complete=True)
r = requests.post(url=url, headers=headers, json=params, verify=False)

# get the status
status = r.json()
# extract the data source ID
ds_id = status['id']
print(f"Created data source: {args['host']}/data/{ds_id}/")

df = pd.read_csv("/Users/matthias.funke/Downloads/zanlo.csv", sep=';')

schemas = df["Data Set"].unique()
tables = df.groupby(['Data Set', 'Business Object'])
fields = df.groupby(['Data Set', 'Business Object', 'Field name'])

sample_data = []
dataflow_objects = []
paths = []
j = 0

for s in schemas:
    if pd.isna(s):
        break
    sample_data.append(dict(key=f"{ds_id}.{s}"))

for t in tables.groups.keys():
    if pd.isna(t):
        break
    sample_data.append(dict(key=f"{ds_id}.{t[0]}.{t[1]}", table_type='TABLE'))

for f, g in fields:
    if pd.isna(f):
        break
    field_key = f"{ds_id}.{f[0]}.{f[1]}.{f[2]}"
    sample_data.append(dict(key=field_key, column_type=g['Field type'].iloc[0]))
    if g['Luca'].iloc[0] == 'x':
        flow_id = f"api/Luca-{random}-{j}"
        flow = dict(external_id=flow_id, content="SOME TEXT HERE", title=f[2], description="A descr")
        dataflow_objects.append(flow)
        j += 1
        paths.append([[dict(otype='external', key='Luca')],
                      [dict(key=flow_id, otype='dataflow')],
                      [dict(key=field_key, otype='column')]])


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

# -- Prepare the dataflow objects and the paths that link them

url = host + '/integration/v2/lineage/'
body = dict(dataflow_objects=dataflow_objects, paths=paths)

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




