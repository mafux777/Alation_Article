import requests
import json
import sys
from collections import deque
import re
from time import sleep, strftime, localtime
from datetime import datetime


url = "http://18.218.6.215"
api = "/integration/v1/data/45/parse_docstore/"
headers = dict(token='0f3dfbe9-4b94-4003-a704-5a51dc168ba4')

CURRENT_TIME = strftime("%Y-%m-%d %H:%M:%S", localtime())

ikea = dict(folders=[dict(name='root folder', collections=[dict(name=f'root coll {CURRENT_TIME}', schemata=[])])])

list_of_defs = ikea['folders'][0]['collections'][0]['schemata']

with open("/Users/matthias.funke/Downloads/ctm_json.json") as f:
    source = json.load(f)

base = source['schema']['fields']
n=0
list_of_keys = []

# This recursive function is designed to encode an existing dictionary
# and convert it into the format that Generic NoSQL expects
def recursive_field_encode(field):
    global n
    field["Sequence Number"] = n
    field['Processing Timestamp'] = datetime.now().isoformat(sep=' ', timespec='milliseconds')
    field['name'] = f"{field['name']}__{n}__"
    #print(f"{n}/{field['name']}/{field['type']}")
    n+=1
    if field.get('fields'):
        if field['type']=='RECORD':
            # if field['mode'] == 'REPEATED':
            #     d = dict(type='array', items=dict())
            #     for c in field['fields']:
            #         g = recursive_field_encode(field=c)    # <--- recursive call!
            #         k = list(g.keys())
            #         v = list(g.values())
            #         d["items"][k[0]] = v[0]    # <---- aggregating the children into one dict
            #     return {field['name']: d}    # <---- this is likely to be the final return
            # else:
            d = dict(type='object', properties=dict())
            for c in field['fields']:
                g = recursive_field_encode(field=c)        # <--- recursive call!
                k = list(g.keys())
                v = list(g.values())
                d["properties"][k[0]] = v[0] # <---- aggregating the children into one dict
            return {field['name']: d} # <---- this is likely to be the final return
        else:
            print(f"A field with fields but not a record: {field}")
            return {field.pop('name') : field}
    else: # this branch returns a leaf outside
        return {field.pop('name') : field}


for f in base:
    backup = f.copy()
    new_schema = recursive_field_encode(f)
    #new_element = dict(name=f['name'], definition=dict(title=f['name'], type='object', properties=new_schema))
    if f.get('name'):
        new_element = dict(name=f['name'], definition=dict(title=f['name'], type='object',
                                                           properties=new_schema[f['name']]['properties']))
    else:
        new_element = dict(name=backup.pop('name'), definition=f)
    list_of_defs.append(new_element)



params=dict(remove_not_seen=False)
r = requests.post(url + api, json=ikea, headers=headers, params=params)
job_id = json.loads(r.content)['job_id']

while(True):
    print('--------------')
    sleep(2)
    url2 = url + '/api/v1/bulk_metadata/job/'
    params = dict(id=job_id)
    r2 = requests.get(url2, params=params, headers=headers)
    r3 = json.loads(r2.content)
    if r3['status']!='running':
        print(r3['msg'])
        print(r3['result'])
        break






