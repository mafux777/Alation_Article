from AlationInstance import AlationInstance
from Article import Article
from alationutil import log_me, extract_files
import config
import pickle
from query import generate_html
import json
import pandas as pd

if __name__ == "__main__":
    # --- Log into the source instance
    alation_1 = AlationInstance(config.args['host'],
                                config.args['username'],
                                config.args['password'], verify=False)
    # custom field API wants this...
    # /api/v1/bulk_metadata/data_dictionary/<otype>?custom_fields=<fields_json>
    def get_values(otype, field_name):
        fields_json = f'{{"{field_name}":[]}}'
        tables = alation_1.generic_api_get(f'/api/v1/bulk_metadata/data_dictionary/{otype}?custom_fields={fields_json}',
                                  official=True)
        tables = tables.decode().split('\n')[0:-1] # last element is empty, remove it
        tables_pd = pd.DataFrame([json.loads(t) for t in tables])
        if 'key' in tables_pd.columns:
            tables_pd.index = tables_pd.key
            del tables_pd['key']
        else:
            print(f"Empty dataframe for {field_name}")
        return tables_pd

    # df1 = get_values('table','steward')
    # df2 = get_values('some multi')
    # df3 = df1.merge(df2, left_index=True, right_index=True)

    # --- use the search API to find tables with a deprecation flag
    otype='table'
    key='flag_types'
    value='Deprecation'
    url = f'/search/?q=&otype={otype}&ff={{"{key}":+"{value}"}}'
    search_result=alation_1.generic_api_get(url)
    print(search_result)