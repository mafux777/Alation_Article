import requests
import json
import random
import pandas as pd
import time
import urllib.parse
from collections import defaultdict

# Prepare the REST API call parameters
host = "https://britishtobacco.alationproserv.com"
headers = dict(token='qHWUL2A7imjcGzK0zNsiX8iRhxRVXSUkAcVDzqzKad0')

def api_call_with_job(url, key, headers, params, body):
    r = requests.post(url=url, headers=headers, params=params, json=body)
    time.sleep(.5)

    # -- Get the status of the bulk upload job ---
    status = json.loads(r.content)
    try:
        params=dict(id=status['job_id'])
        url_job = host + "/api/v1/bulk_metadata/job/"

        for _ in range(10):
            r_2 = requests.get(url=url_job, headers=headers, params=params)
            status = (json.loads(r_2.content))
            if status['status'] != 'running':
                result = status.get('result')
                for r in result:
                    mapping = r.get('mapping')
                    if mapping:
                        for m in mapping:
                            if m['key']==key:
                                return m['id']
                    else:
                        print(f"No result in {r}")
            else: # still running
                print(status.get('msg'))
                time.sleep(2)
    except Exception as e:
        print(f"Could not create API job: {e}")
    return


def get_or_create_ds(ds):
    r = requests.get(url=url, headers=headers, verify=True)
    try:
        all_ds = pd.DataFrame(r.json())
    except ValueError as e:
        raise UserWarning("API key?")

    my_ds = all_ds.loc[all_ds.title==ds, 'id']
    if my_ds.empty:
        # need to create the datasource
        params = dict(dbtype="postgresql", title=f"{ds}", is_virtual=True, deployment_setup_complete=True)
        r = requests.post(url=url, headers=headers, json=params, verify=True)

        # get the status
        status = r.json()
        # extract the data source ID
        my_ds = status['id']
        print(f"Created data source: {host}/data/{my_ds}/")
        virtual_ds[ds]=my_ds
    else:
        virtual_ds[ds]=my_ds

    return int(my_ds)

def url_enc(name):
    return urllib.parse.quote(name)

def get_or_create_schema(ds, schema):
    url = f"{host}/integration/v2/schema/"
    params = dict(ds_id=ds, name=schema.lower(), )
    r = requests.get(url=url, headers=headers, params=params, verify=True)
    my_schema = r.json()
    for s in my_schema:
        if schema.lower() == s.get('name').lower():
            return s.get('id')
    key = f"{ds}.{schema}"
    body = [dict(key=key,
                 title=f"Title: {schema}",
                 description=f"Description: {schema}",
                 ),]
    r = api_call_with_job(url=url,
                      key=key,
                      headers=headers,
                      params=dict(ds_id=ds),
                      body=body)
    return r

table_cache = {}

def get_or_create_table(ds_id, schema_id, schema, table):
    url = f"{host}/integration/v2/table/"
    my_table = []
    if schema_id:
        params = dict(ds_id=ds_id, schema_id=schema_id, name__iexact=table)
        # params = dict(ds_id=ds_id, schema_id=schema_id)
        r = requests.get(url=url, headers=headers, params=params, verify=True)
        my_table = r.json()
    else:
        for ds in virtual_ds:
            params = dict(ds_id=virtual_ds[ds])
            r = requests.get(url=url, headers=headers, params=params, verify=True)
            my_table.extend(r.json())

    if len(my_table)>0:
        # print(f"There are {len(my_table)} tables to search through")
        for t in my_table:
            if table.lower() == t.get('name').lower():
                table_cache[table] = t.get('key')
                return t.get('id')
        print(f"Sorry - it looks like {table} is not here")
    key = f"{ds_id}.{schema}.{table}"
    table_cache[table] = key
    body = [dict(key=key,
                 title=f"Title: {table}",
                 description=f"Description: {table}",
                 ),]
    r = api_call_with_job(url=url,
                      key=key,
                      headers=headers,
                      params=dict(ds_id=ds_id),
                      body=body)
    return r

table_to_col_map = defaultdict(int)

def get_or_create_col(ds_id, schema_id, schema, table_id, table, col, title):
    url = f"{host}/integration/v2/column/"
    params = dict(ds_id=ds_id, schema_id=schema_id, table_id=table_id, name__iexact=col)
    r = requests.get(url=url, headers=headers, params=params, verify=True)
    my_col = r.json()
    for c in my_col:
        if col.lower() == c.get('name').lower():
            return c.get('id')
    key_t = f"{ds_id}.{schema}.{table}"
    key = f'{key_t}."{col}"'
    table_to_col_map[key_t] += 1
    body = [dict(key=key,
                 title=title,
                 description=f"Description: {col}",
                 column_type="text",
                 position=table_to_col_map[key_t],
                 ),]
    r = api_call_with_job(url=url,
                          key=key,
                          headers=headers,
                          params=dict(ds_id=ds),
                          body=body)
    return r

df_1 = pd.read_csv("/Users/matthias.funke/Downloads/Lineage Example for Alation-1.csv")
df_2 = pd.read_csv("/Users/matthias.funke/Downloads/Lineage Example for Alation-2.csv")

# Create a data source
url = host + "/integration/v1/datasource/"

# Create a random title
file_key = ''.join(random.sample("ABCDEFGHJKLMNPQRSTUVWXYZ0123456789", 4))
# file_key = "ZGJE"

df_1['Virtual Data Sources Name'] = df_1['Virtual Data Sources Name'].apply(lambda x: f"{file_key} {x}")

grouped = df_1.groupby(list(df_1.columns))
virtual_ds = defaultdict(int)

# create the columns one-by-one (first source, then schema, then table)
for my_index, rest in grouped:
    print(my_index)
    ds = get_or_create_ds(my_index[0])
    schema = get_or_create_schema(ds, my_index[1])
    table = get_or_create_table(ds, schema, my_index[1], my_index[2])
    col = get_or_create_col(ds_id=ds,
                            schema_id=schema,
                            schema=my_index[1],
                            table_id=table,
                            table=my_index[2],
                            col=my_index[3],
                            title=my_index[4])

dataflow_objects = []
paths = []

def key_table(table):
    return dict(otype='table', key=table)

for name, rest in df_2.groupby(['Source Table', 'Target Table']):
    left_key = table_cache.get(name[0])
    right_key = table_cache.get(name[1])
    if not left_key:
        print(f"Could not find left table {name[0]}")
        continue
    if not right_key:
        print(f"Could not find right table {name[1]}")
        continue

    random_id = ''.join(random.sample("ABCDEFGHJKLMNPQRSTUVWXYZ0123456789", 8))
    external_id = f"api/{random_id}"
    my_dataflow = dict(external_id=external_id,
                       title = f"Dataflow {external_id}",
                       description=f"Dataflow from {name[0]} to {name[1]}")
    dataflow_objects.append(my_dataflow)

    my_path = [[key_table(left_key)],[dict(otype='dataflow', key=external_id)],[key_table(right_key)]]
    paths.append(my_path)


url = host + '/integration/v2/lineage/'
body = {
    "dataflow_objects": dataflow_objects,
    "paths": paths
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




