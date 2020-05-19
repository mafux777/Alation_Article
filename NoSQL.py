import requests
import json
from time import sleep
from datetime import datetime, timezone

# -- ADJUST THE FOLLOWING CONFIG PARAMS
ds_id=49
url = "http://18.218.6.215"
headers = dict(token='a11b49ca-d484-4a46-b1d9-d0af03ae348c')
filename = "/Users/matthias.funke/Downloads/ctm_json.json"
folder='root.bq' # should contain 1 dot
collection=f'CTM-v4' # should not contain any dots
# -- THAT WAS IT

def log_me(txt):
    dt = datetime.now(timezone.utc).isoformat(sep=' ', timespec='milliseconds')
    print(f"{dt} {txt}")



api = f"/integration/v1/data/{ds_id}/parse_docstore/"


# the dictionary will be the container for all the physical metadata
# a folder...
# containing a collection...
# containing a bunch of schemas
ukulele = dict(folders=[dict(name=folder, collections=[dict(name=collection, schemata=[])])])
list_of_defs = ukulele['folders'][0]['collections'][0]['schemata']

# let's open the file from BigQuery in JSON format
log_me("Reading file")
with open(filename) as f:
    source = json.load(f)

# let's grab all the fields in that schema and start counting
base = source['schema']['fields']
n=1
logical_metadata = {}
physical_metadata = {}

# This recursive function is designed to encode an existing dictionary
# and convert it into the format that Generic NoSQL expects
def recursive_field_encode(field, parent=None):
    global n, folder, collection
    field["Sequence Number"] = n
    field['Processing Timestamp'] = datetime.now().isoformat(sep=' ', timespec='milliseconds')
    if parent:
        if 'full_key' in parent:
            field['full_key'] = f"{parent['full_key']}.{field['name']}"
        else:
            field['full_key'] = f"{parent['name']}.{field['name']}"
    else:
        field['full_key'] = f"{field['name']}"
    logical_metadata[f'{folder}.{collection}.{field["full_key"]}'] = dict(
        title=f'{n:04}', # let's put the sequence number in the title
        description=f"{field.get('description')}") # and the description in the description
    # We also want to create a SQL VDS later, so let's store the flattened column name
    physical_metadata[f'{folder}.{collection}.{field["full_key"]}'] = dict(
        column_type=f"{field.get('type')}")
    n+=1
    if field.get('fields'):
        d = dict(type='object', properties=dict())
        for c in field['fields']:
            g = recursive_field_encode(field=c, parent=field)        # <--- recursive call!
            for k, v in g.items():
                d["properties"][k] = v # <---- aggregating the children into one dict
        return {field['name']: d} # <---- this is likely to be the final return
    else: # this branch returns a leaf outside
        return {field.pop('name') : field}

log_me("Processing recursive fields")
for f in base:
    name = f.get('name') # will be None if the field does not have a name
    new_schema = recursive_field_encode(f)
    if f.get('name'):
        new_element = dict(name=f['name'], definition=dict(title=f['name'], type='object',
                                                           properties=new_schema[f['name']]['properties']))
    else:
        new_element = dict(name=name, definition=f)
    list_of_defs.append(new_element)

# -- Upload NoSQL "physical metadata"
log_me("Uploading physical metadata for NoSQL")
params=dict(remove_not_seen=True)
r = requests.post(url + api, json=ukulele, headers=headers, params=params)
job_id = json.loads(r.content)['job_id']

log_me("Checking status: uploading physical metadata for NoSQL")
while(True):
    url2 = url + '/api/v1/bulk_metadata/job/'
    params = dict(id=job_id)
    r2 = requests.get(url2, params=params, headers=headers)
    r3 = json.loads(r2.content)
    if r3['status']!='running':
        log_me(r3['msg'])
        log_me(r3['result'])
        break
    sleep(2)

# take care of logical metadata
body=[]
data=""
for k, v in logical_metadata.items():
    d=dict(key=f"{ds_id}.{k}", title=v.get('title'),description=v['description'])
    body.append(d)
    data=data+json.dumps(d)+"\n"

log_me("Uploading logical metadata for NoSQL")
r2 = requests.post(f'{url}/api/v1/bulk_metadata/custom_fields/default/doc_schema', data=data, headers=headers)
log_me(r2.content)

# create a virtual data source
# /integration/v1/datasource/

# Parameters for creating the data source
log_me("Creating a virtual datasource of type bigquery")
params=dict(dbtype="bigquery", title=f"Ukulele VDS", is_virtual=True, deployment_setup_complete=True)
r = requests.post(url=f"{url}/integration/v1/datasource/", headers=headers, json=params, verify=False)

# get the status
status = r.json()
# extract the data source ID
ds_id = status['id']
log_me(f"Created data source: {url}/data/{ds_id}/")
log_me("Deleting some old data sources")
for i in range(53, ds_id):
    r = requests.delete(url=f"{url}/integration/v1/datasource/{i}/", headers=headers, verify=False)
#ds_id=51

# write physical metadata for VDS
# at the same time, prepare logical metadata
body=[dict(key=f'{folder}'), # for the schema
      dict(key=f'{folder}.{collection}', table_type="TABLE")]
body_2 = []
for k, v in physical_metadata.items():
    segments = k.split(".")
    seg = len(segments)
    if seg>4:
        seg_1 = ".".join(segments[0:3])
        seg_2 = "_".join(segments[3:])
        name_2 = f'{seg_1}.{seg_2}'
        body.append(dict(key=name_2,column_type=v['column_type']))
        body_2.append(dict(key=name_2,title=f"{seg-3:02} {segments[-1]}", description=logical_metadata[k].get('description')))
    else:
        body.append(dict(key=k,column_type=v['column_type']))
        body_2.append(dict(key=k,title=f"{seg-3:02} {segments[-1]}", description=logical_metadata[k].get('description')))


data=""
for b in body:
    b['key'] = f"{ds_id}.{b['key']}"
    data=data+json.dumps(b)+"\n"

log_me("Posting physical metadata for bigquery")
url_2 = f"{url}/api/v1/bulk_metadata/extraction/{ds_id}"
r = requests.post(url=url_2, headers=headers, data=data, params=dict(remove_not_seen=True))

# -- Get the status of the bulk upload job ---
status = r.json()
params=dict(name=status['job_name'].replace("#", "%23"))
url_job = f"{url}/api/v1/bulk_metadata/job/?name={params['name']}"

while(True):
    r_2 = requests.get(url=url_job, headers=headers)
    status = r_2.json()
    if status['status'] != 'running':
        objects = json.loads(status['result'])['error_objects']
        if objects:
            for error in objects:
                log_me(error)
        else:
            log_me(status)
        break

# take care of logical metadata
data=""
for b in body_2:
    b['key'] = f"{ds_id}.{b['key']}"
    data=data+json.dumps(b)+"\n"
log_me("Uploading logical metadata")
r2 = requests.post(f'{url}/api/v1/bulk_metadata/custom_fields/default/mixed', data=data, headers=headers)
log_me(r2.json())





