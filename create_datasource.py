import json
import requests
import random

host = "http://18.218.6.215"

url = host + "/integration/v1/datasource/"
random = ''.join(random.sample("ABCDEFGHJKLMNPQRSTUVWXYZ0123456789", 4))
params=dict(dbtype="customdb",
            host="fennel2.cluster-cingqyuv6npc.us-east-2.rds.amazonaws.com",
            port=5433,
            db_username='postgres',
            db_password='alation123',
            dbname='fennel',
            title=f"DS {random}",
            is_virtual=False,
            deployment_setup_complete=True)
headers = dict(token='a67a92f3-d7b3-419c-b3ec-4433681f6dd5')
r = requests.post(url=url, headers=headers, json=params, verify=False)

status = (json.loads(r.content))
print(status)
ds_id = status['id']
print(f"Created data source: {host}/data/{ds_id}/")

