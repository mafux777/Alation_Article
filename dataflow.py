import requests
import json

ds_id = 9

sample_data = [
    {"key": ".SOURCE"},
    {"key": ".STAGING"},
    {"key": ".DM"},
    {"key": ".SOURCE.Transactions", "table_type": "TABLE"},
    {"key": ".STAGING.tmp_orders", "table_type": "TABLE"},
    {"key": ".STAGING.tmp_product", "table_type": "TABLE"},
    {"key": ".DM.ORDERS", "table_type": "TABLE"},
    {"key": ".DM.PRODUCT", "table_type": "TABLE"},
    {"key": ".SOURCE.Transactions.date", "column_type": "date"},
    {"key": ".SOURCE.Transactions.orderNumber", "column_type": "int"},
    {"key": ".SOURCE.Transactions.productCode", "column_type": "int"},
    {"key": ".SOURCE.Transactions.productName", "column_type": "string"},
    {"key": ".STAGING.tmp_orders.event_ts", "column_type": "date"},
    {"key": ".STAGING.tmp_orders.order_id", "column_type": "int"},
    {"key": ".STAGING.tmp_product.product_id", "column_type": "int"},
    {"key": ".STAGING.tmp_product.product_name", "column_type": "string"},
    {"key": ".STAGING.tmp_product.order_id", "column_type": "int"},
    {"key": ".DM.ORDERS.event_ts", "column_type": "date"},
    {"key": ".DM.ORDERS.order_id", "column_type": "int"},
    {"key": ".DM.PRODUCT.product_id", "column_type": "int"},
    {"key": ".DM.PRODUCT.product_name", "column_type": "string"},
    {"key": ".DM.PRODUCT.order_id", "column_type": "int"}
]

body = ""

for datum in sample_data:
    datum['key'] = str(ds_id) + datum['key']
    body = body + json.dumps(datum) + "\n"

host = "http://18.218.6.215"
url = host + "/api/v1/bulk_metadata/extraction/"+str(ds_id)+"?remove_not_seen=true"
headers = dict(token='9109bf99-2eaa-4eb8-bbec-c21905281ffa')
r = requests.post(url=url, headers=headers, data=body)

status = (json.loads(r.content))

params=dict(name=status['job_name'].replace("#", "%23"))
url_job = host + "/api/v1/bulk_metadata/job/?name=" + params['name']

while(True):
    r_2 = requests.get(url=url_job, headers=headers)
    status = (json.loads(r_2.content))
    if status['status']!='running':
        error_objects = json.loads(status['result'])['error_objects']
        if error_objects:
            for error in error_objects:
                print (error)
        else:
            print (status)
        break

url = host + '/integration/v2/lineage/'
body = {
    "dataflow_objects": [
        {
            "external_id": "api/transform_01_"+str(ds_id),
            "content": "Transformation refers to the cleansing and aggregation that may need to happen to data to prepare it for analysis."
        },
        {
            "external_id": "api/transform_02_"+str(ds_id),
            "content": "Transformation refers to the cleansing and aggregation that may need to happen to data to prepare it for analysis."
        },
        {
            "external_id": "api/transform_03_"+str(ds_id),
            "content": "Transformation refers to the cleansing and aggregation that may need to happen to data to prepare it for analysis."
        },
        {
            "external_id": "api/trans_to_tmp_orders_"+str(ds_id),
            "content": "Transformation refers to the cleansing and aggregation that may need to happen to data to prepare it for analysis."
        },
        {
            "external_id": "api/tmp_orders_to_orders_"+str(ds_id),
            "content": "Transformation refers to the cleansing and aggregation that may need to happen to data to prepare it for analysis."
         },
        {
            "external_id": "api/trans_to_tmp_product_"+str(ds_id),
            "content": "Transformation refers to the cleansing and aggregation that may need to happen to data to prepare it for analysis."
         }
    ],
    "paths": [
        [
            [{"otype": "column", "key": str(ds_id)+".SOURCE.Transactions.date"}],
            [{"otype": "dataflow", "key": "api/trans_to_tmp_orders_"+str(ds_id)}],
            [{"otype": "column", "key": str(ds_id)+".STAGING.tmp_orders.event_ts"}],
            [{"otype": "dataflow", "key": "api/tmp_orders_to_orders_"+str(ds_id)}],
            [{"otype": "column", "key": str(ds_id)+".DM.ORDERS.event_ts"}]
        ],
        [
            [{"otype": "column", "key": str(ds_id)+".SOURCE.Transactions.orderNumber"}],
            [{"otype": "dataflow", "key": "api/trans_to_tmp_orders_"+str(ds_id)}],
            [{"otype": "column", "key": str(ds_id)+".STAGING.tmp_orders.order_id"}],
            [{"otype": "dataflow", "key": "api/tmp_orders_to_orders_"+str(ds_id)}],
            [{"otype": "column", "key": str(ds_id)+".DM.ORDERS.order_id"}]
        ],
        [
            [{"otype": "column", "key": str(ds_id)+".SOURCE.Transactions.productCode"}],
            [{"otype": "dataflow", "key": "api/trans_to_tmp_product_"+str(ds_id)}],
            [{"otype": "column", "key": str(ds_id)+".STAGING.tmp_product.product_id"}],
            [{"otype": "dataflow", "key": "api/transform_01_"+str(ds_id)}],
            [{"otype": "column", "key": str(ds_id)+".DM.PRODUCT.product_id"}]
        ],
        [
            [{"otype": "column", "key": str(ds_id)+".SOURCE.Transactions.productName"}],
            [{"otype": "dataflow", "key": "api/trans_to_tmp_product_"+str(ds_id)}],
            [{"otype": "column", "key": str(ds_id)+".STAGING.tmp_product.product_name"}],
            [{"otype": "dataflow", "key": "api/transform_02_"+str(ds_id)}],
            [{"otype": "column", "key": str(ds_id)+".DM.PRODUCT.product_name"}]
        ],
        [
            [{"otype": "column", "key": str(ds_id)+".SOURCE.Transactions.orderNumber"}],
            [{"otype": "dataflow", "key": "api/trans_to_tmp_product_"+str(ds_id)}],
            [{"otype": "column", "key": str(ds_id)+".STAGING.tmp_product.order_id"}],
            [{"otype": "dataflow", "key": "api/transform_03_"+str(ds_id)}],
            [{"otype": "column", "key": str(ds_id)+".DM.PRODUCT.order_id"}]
        ]
    ]
}

r = requests.post(url=url, headers=headers, json=body)

status = (json.loads(r.content))

params=dict(id=status['job_id'])
url_job = host + "/api/v1/bulk_metadata/job/"

while(True):
    r_2 = requests.get(url=url_job, headers=headers, params=params)
    status = (json.loads(r_2.content))
    if status['status']!='running':
        error_objects = status['result']
        if error_objects:
            #for error in error_objects:
             print (error_objects)
        else:
            print (status)
        break




