import requests

base_url = "https://burberry.alationcloud.com/"
headers = dict(token="VunHMdO_fT3_xSF4qdiLgcF7tUwVc3UVMz0FgKf8E38")

api = "/integration/v2/table/"
table_id= 60049

r = requests.get(url=f"{base_url}/{api}/",
                 params=dict(id=table_id),
                 headers=headers)
table_details = r.json()
key_of_my_table =table_details.get('key')
key_of_my_table = '10."_sys_bic"."applications.bers/ca_bers_opt/brandbuy/hier/brandbuy"'

api_2 = "/api/v1/bulk_metadata/custom_fields/default/mixed"
r = requests.post(url=f"{base_url}{api_2}",
                 json=dict(key=key_of_my_table, description="Nice one."),
                 headers=headers)
results = r.json()
print()




print(r)