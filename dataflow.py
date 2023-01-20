import requests
import json
import random

# Prepare the REST API call parameters
host = "https://beta-sandbox.alationproserv.com"
headers = dict(token='gNvDvXODxryOu9OORqG1udVBkOIItMoys-tiVIQ01f8')

# Create a data source
url = host + "/integration/v1/datasource/"

# Create a random title
random = ''.join(random.sample("ABCDEFGHJKLMNPQRSTUVWXYZ0123456789", 4))

# Parameters for creating the data source
params=dict(dbtype="postgresql", title=f"My API DS {random}", is_virtual=True, deployment_setup_complete=True)
r = requests.post(url=url, headers=headers, json=params, verify=False)

# get the status
status = r.json()
# extract the data source ID
ds_id = status['id']
print(f"Created data source: {host}/data/{ds_id}/")

# --- Prepare sample data for the virtual data source
sample_data = [
    {"key": f"{ds_id}.SOURCE"},
    {"key": f"{ds_id}.STAGING"},
    {"key": f"{ds_id}.DM"},
    {"key": f"{ds_id}.SOURCE.Transactions", "table_type": "TABLE"},
    {"key": f"{ds_id}.STAGING.tmp_orders",  "table_type": "TABLE"},
    {"key": f"{ds_id}.STAGING.tmp_product", "table_type": "TABLE"},
    {"key": f"{ds_id}.DM.ORDERS", "table_type": "TABLE"},
    {"key": f"{ds_id}.DM.PRODUCT", "table_type": "TABLE"},
    {"key": f"{ds_id}.SOURCE.Transactions.date", "column_type": "date"},
    {"key": f"{ds_id}.SOURCE.Transactions.orderNumber", "column_type": "int"},
    {"key": f"{ds_id}.SOURCE.Transactions.productCode", "column_type": "int"},
    {"key": f"{ds_id}.SOURCE.Transactions.productName", "column_type": "string"},
    {"key": f"{ds_id}.STAGING.tmp_orders.event_ts", "column_type": "date"},
    {"key": f"{ds_id}.STAGING.tmp_orders.order_id", "column_type": "int"},
    {"key": f"{ds_id}.STAGING.tmp_product.product_id", "column_type": "int"},
    {"key": f"{ds_id}.STAGING.tmp_product.product_name", "column_type": "string"},
    {"key": f"{ds_id}.STAGING.tmp_product.order_id", "column_type": "int"},
    {"key": f"{ds_id}.DM.ORDERS.event_ts", "column_type": "date"},
    {"key": f"{ds_id}.DM.ORDERS.order_id", "column_type": "int"},
    {"key": f"{ds_id}.DM.PRODUCT.product_id", "column_type": "int"},
    {"key": f"{ds_id}.DM.PRODUCT.product_name", "column_type": "string"},
    {"key": f"{ds_id}.DM.PRODUCT.order_id", "column_type": "int"}
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

# -- Prepare the dataflow objects and the paths that link them

url = host + '/integration/v2/lineage/'
body = {
    "dataflow_objects": [
        {
            "external_id": f"api/transform_01_{ds_id}",
            "content": "Transformation refers to the cleansing and aggregation that may need to happen to data to prepare it for analysis."
        },
        {
            "external_id": f"api/transform_02_{ds_id}",
            "content": "Transformation refers to the cleansing and aggregation that may need to happen to data to prepare it for analysis."
        },
        {
            "external_id": f"api/transform_03_{ds_id}",
            "content": "Transformation refers to the cleansing and aggregation that may need to happen to data to prepare it for analysis."
        },
        {
            "external_id": f"api/trans_to_tmp_orders_{ds_id}",
            "content": "Transformation refers to the cleansing and aggregation that may need to happen to data to prepare it for analysis."
        },
        {
            "external_id": f"api/tmp_orders_to_orders_{ds_id}",
            "content": "Transformation refers to the cleansing and aggregation that may need to happen to data to prepare it for analysis."
         },
        {
            "external_id": f"api/trans_to_tmp_product_{ds_id}",
            "content": "Transformation refers to the cleansing and aggregation that may need to happen to data to prepare it for analysis."
         }
    ],
    "paths": [
        [
            [{"otype": "column", "key": f"{ds_id}.SOURCE.Transactions.date"}],
            [{"otype": "dataflow", "key": f"api/trans_to_tmp_orders_{ds_id}"}],
            [{"otype": "column", "key": f"{ds_id}.STAGING.tmp_orders.event_ts"}],
            [{"otype": "dataflow", "key": f"api/tmp_orders_to_orders_{ds_id}"}],
            [{"otype": "column", "key": f"{ds_id}.DM.ORDERS.event_ts"}]
        ],
        [
            [{"otype": "column", "key": f"{ds_id}.SOURCE.Transactions.orderNumber"}],
            [{"otype": "dataflow", "key": f"api/trans_to_tmp_orders_{ds_id}"}],
            [{"otype": "column", "key": f"{ds_id}.STAGING.tmp_orders.order_id"}],
            [{"otype": "dataflow", "key": f"api/tmp_orders_to_orders_{ds_id}"}],
            [{"otype": "column", "key": f"{ds_id}.DM.ORDERS.order_id"}]
        ],
        [
            [{"otype": "column", "key": f"{ds_id}.SOURCE.Transactions.productCode"}],
            [{"otype": "dataflow", "key": f"api/trans_to_tmp_product_{ds_id}"}],
            [{"otype": "column", "key": f"{ds_id}.STAGING.tmp_product.product_id"}],
            [{"otype": "dataflow", "key": f"api/transform_01_{ds_id}"}],
            [{"otype": "column", "key": f"{ds_id}.DM.PRODUCT.product_id"}]
        ],
        [
            [{"otype": "column", "key": f"{ds_id}.SOURCE.Transactions.productName"}],
            [{"otype": "dataflow", "key": f"api/trans_to_tmp_product_{ds_id}"}],
            [{"otype": "column", "key": f"{ds_id}.STAGING.tmp_product.product_name"}],
            [{"otype": "dataflow", "key": f"api/transform_02_{ds_id}"}],
            [{"otype": "column", "key": f"{ds_id}.DM.PRODUCT.product_name"}]
        ],
        [
            [{"otype": "column", "key": f"{ds_id}.SOURCE.Transactions.orderNumber"}],
            [{"otype": "dataflow", "key": f"api/trans_to_tmp_product_{ds_id}"}],
            [{"otype": "column", "key": f"{ds_id}.STAGING.tmp_product.order_id"}],
            [{"otype": "dataflow", "key": f"api/transform_03_{ds_id}"}],
            [{"otype": "column", "key": f"{ds_id}.DM.PRODUCT.order_id"}]
        ]
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




