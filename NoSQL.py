import requests
import json
import random
from time import sleep
from datetime import datetime, timezone

# -- ADJUST THE FOLLOWING CONFIG PARAMS
url = "https://funkmeister.alationproserv.com/"
headers = dict(token='rUkC-Si-lI9E6ua5bWfrhY1WSS0N8dEx7GI_RxF-PTc')
filename = "./sampledata/ctm_json.json"
folder='root.bq' # should contain 1 dot
collection=f'CTM-v4' # should not contain any dots
# -- THAT WAS IT

def log_me(txt):
    dt = datetime.now(timezone.utc).isoformat(sep=' ', timespec='milliseconds')
    print(f"{dt} {txt}")



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



url_1 = url + "/integration/v1/datasource/"
random = ''.join(random.sample("ABCDEFGHJKLMNPQRSTUVWXYZ0123456789", 4))
params=dict(dbtype="generic_nosql",
            title=f"DS {random}",
            is_virtual=True,
            deployment_setup_complete=True)
r = requests.post(url=url_1, headers=headers, json=params, verify=False)

status = (json.loads(r.content))
print(status)
ds_id = status['id']
# ds_id = 13
print(f"Created data source: {url}/data/{ds_id}/")
api = f"/integration/v1/data/{ds_id}/parse_docstore/"


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
my_len = len(logical_metadata.items())
my_size = my_len//5
for n in range(6):
    body = []
    data = ""
    start = n*my_size
    stop = ((n+1)*my_size)
    for k, v in list(logical_metadata.items())[start:stop]:
        d=dict(key=f"{ds_id}.{k}", title=v.get('title'),description=v['description'])
        body.append(d)
        data=data+json.dumps(d)+"\n"

    log_me("Uploading logical metadata for NoSQL")
    r2 = requests.post(f'{url}/api/v1/bulk_metadata/custom_fields/default/doc_schema', data=data, headers=headers)
    log_me(r2.content)

print("ALL DONE")

