import requests
import json


api_tokens = [
#    dict(role='Catalog Admin', token = '0bf02afb-db44-48d0-9bd8-2d98aea8873b'),
    dict(role='Server Admin',  token = '9bb3587b-0921-4040-973d-42bf004159a0')#,
#    dict(role='Data Source Admin', token = 'c5a113ce-d975-487e-9b98-bd152df1321e')
]

url = 'http://abok.alationproserv.com'
ds_id = 2

api_list = [
    f'/integration/v1/datasource/{ds_id}',
    f'/integration/v1/schema/?ds_id={ds_id}',
    f'/integration/v1/table/?ds_id={ds_id}',
    f'/integration/v1/column/?ds_id={ds_id}',
    '/integration/v1/article/',
    '/api/v1/bulk_metadata/data_dictionary/table?custom_fields={"Expert":[]}',
    '/integration/v1/custom_template/',
    '/integration/v2/bi/server/'

]
for role in api_tokens:
    print('---------------------')
    print(role['role'])
    print('---------------------')
    for a in api_list:
        url_ = url + a
        r = requests.get(url=url_, headers=dict(token=role['token']))
        print("Request = {}".format(r.request.url))
        r_parsed = json.loads(r.content)
        if isinstance(r_parsed, list):
            print("Found {} items".format(len(r_parsed)))
            if len(r_parsed)<=50:
                for i in r_parsed:
                    print(i)
        else:
            print(r_parsed)
