from sudoku.AlationInstance import AlationInstance
import config
import pandas as pd

if __name__ == "__main__":
    # --- Log into the source instance
    alation_1 = AlationInstance(config.args['host'],
                                config.args['username'],
                                config.args['password'])

    # GET metadata for table "crypto_large"
    t = alation_1.generic_api_get(api="/integration/v2/table/",
                                  params=dict(name="crypto_large", ds_id=22),
                                  official=True)
    if len(t)==1:
        table_data = t[0]
    else:
        print(f"Found {len(t)} tables. Please check what to do.")

    # GET all columns for that table
    c = alation_1.generic_api_get(api="/integration/v2/column/",
                                  params=dict(table_id=table_data['id'], limit=500),
                                  official=True)

    # Create a virtual data source to host this data
    ds = alation_1.generic_api_get("/integration/v1/datasource/22/", official=True)
    relevant_cols = ['dbtype', 'host', 'port', 'dbname', 'title', 'description', 'deployment_setup_complete',
                     'private', 'is_virtual', 'is_hidden']
    new_ds = {}
    for col in relevant_cols:
        new_ds[col] = ds[col]
    new_ds['is_virtual'] = True
    new_ds['title'] = "Alation API Office Hours Demo"

    vds = alation_1.generic_api_post("/integration/v1/datasource/",
                                     data=new_ds,
                                     official=True)
    ds_id = vds.get('id')

    # Create a schema
    my_schema = dict(key=f"{ds_id}.crypto",
                     title="Crypto Schema",
                     description="This data comes from messari.io",
                     db_comment="A schema with crypto data")
    new_schema = alation_1.generic_api_post("/integration/v2/schema/",
                                            params=dict(ds_id=ds_id),
                                            body=[my_schema],
                                            official=True)

    table_data['key'] = f"{ds_id}.{table_data['schema_name']}.{table_data['name']}"
    new_table = alation_1.generic_api_post("/integration/v2/table/",
                                            params=dict(ds_id=ds_id),
                                            body=[table_data],
                                            official=True)
    for bad_key in new_table['errors'][0]:
        del table_data[bad_key]
    for bad_key in ['id', 'url', 'schema_id', 'schema_name', 'ds_id', 'name']:
        del table_data[bad_key]
    for f in table_data['custom_fields']:
        del f['field_name']

    new_table = alation_1.generic_api_post("/integration/v2/table/",
                                            params=dict(ds_id=ds_id),
                                            body=[table_data],
                                            official=True)

    # Upload column info
    list_of_cols = []
    for col_ in c:
        name = col_['name'].replace(".", "_")
        list_of_cols.append(dict(key=f"{ds_id}.crypto.crypto_large.{name}",
                                 column_type=col_['column_type'],
                                 title=f"Random title for {col_['name']}",
                                 description=f"Random desc for {col_['name']}",
                                 position=col_['position'],
                                 ))
    new_cols = alation_1.generic_api_post("/integration/v2/column/",
                                            params=dict(ds_id=ds_id),
                                            body=list_of_cols,
                                            official=True)

    for my_result in new_cols['result']:
        if my_result.get('mapping'):
            df = pd.DataFrame(my_result['mapping'])
            print(f"Min ID: {df.id.min()} Max ID: {df.id.max()}")




    print("All done.")



