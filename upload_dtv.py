# -*- coding: utf-8 -*-
import pandas as pd
from AlationInstance import AlationInstance
from alationutil import log_me
import random
import math
import json
import requests
import time

# import the necessary packages
import argparse

def cut_str(s, l=25):
    if len(s)>l:
        return s[0:l]
    else:
        return s

def stringify(some_list):
    new_list = []
    for i in some_list:
        new_list.append("{}".format(i))
    return new_list

def add_table_row(key, value):
    return "<tr><td> {} </td><td> {} </td></tr>".format(key, value)

def create_table_html(body):
    return "<table><tbody>{body}</tbody></table>".format(body='\n'.join(body))

if __name__ == "__main__":
    # parse the command line arguments
    ap = argparse.ArgumentParser()
    ap.add_argument("-u", "--username",  required=True,  help="username")
    ap.add_argument("-p", "--password",  required=True,  help="password")
    ap.add_argument("-H", "--host",      required=True,  help="URL of the Alation instance")
    ap.add_argument("-d", "--delete",  action='store_const',
                    const=True, default=False,  required=False, help="delete previous")
    ap.add_argument("-f", "--pickle",    required=False, help="pickle file name")
    args = vars(ap.parse_args())

    # --- Log into the target instance
    url_2    = args['host']
    user_2   = args['username']
    passwd_2 = args['password']
    delete_flag = args['delete']
    target = AlationInstance(url_2, user_2, passwd_2)

    # ---- test query
    body = {}
    body['content'] = "this is a new query"
    body['published_content'] = body['content']
    body['ds_id'] = 1
    body['title'] = "Query Title"
    body['description'] = "Let's make sure every query has a description"
    body['published'] = True
    body['author'] = 1
    r = target.put_single_query(body)
# --------------------------
    #dd = target.download_datadict_r6(1)  # Alation Analytics is 1 on ABOK



    file_key = "(" + ''.join(random.sample("ABCDEFGHJKLMNPQRSTUVWXYZ0123456789", 4)) + ")"
    #file_key = "AYU5"
    desired_template = file_key

    #ds_id = 11

    dtv = pd.read_csv(args['pickle'], sep=';')
    cols = list(dtv.axes[1])

    #downloaded_dd = pd.read_csv('/Users/matthias.funke/Downloads/schema_26_1_2019-12-03T11-17-26-141401.csv')


    # # -- produce a list of tables --
    # tables = dtv.loc[dtv.Table.notna() ,["Table"]].drop_duplicates()
    # tables['key'] = str(ds_id) + "." + tables['Table']
    # #tables['key'] = tables['Table']
    # tables['table_type'] = "Table"
    #
    # #tables = tables.iloc[0:100] # test: only 10 tables to begin with
    #
    # # -- produce a list of cols --
    # cols = dtv.loc[dtv.Table.notna() ,["Table", "Field Name", "Data Type", "Order"]].drop_duplicates()
    # cols['position'] = cols.Order.apply(lambda p: "{}".format(math.floor(p)))
    # cols['key'] = str(ds_id) + "." + cols['Table'] + '.' + cols['Field Name']
    # #cols['key'] = cols['Table'] + '.' + cols['Field Name']
    # cols = cols.rename(columns={"Data Type":"column_type"})
    #
    # # -- produce a DataFrame for the physical metadata
    # physical = tables.loc[:,['key', 'table_type']].append(cols.loc[:, ['key', 'column_type', 'position']])
    #
    # # -- produce the JSON for the Virtual Data Source API
    # body = ""
    # for row in physical.iterrows():
    #     # each row is a tuple with index [0] and payload [1]
    #     body = body + json.dumps(dict(row[1].dropna())) + "\n"
    #
    # # -- produce the logical metadata for the fields...
    # logical = pd.read_csv('/Users/matthias.funke/Downloads/Result#649.csv')
    # logical['key'] = str(ds_id)+'.'+logical['key']
    #
    # n = 500
    # s = logical.shape[0]
    # j = math.floor(s/n) + 1 # how many blocks of 100?
    #
    # host = "https://r5-sandbox.alationproserv.com"
    # url = host + "/api/v1/bulk_metadata/custom_fields/default/mixed"
    # headers = dict(token='7eaa1400-86c8-4221-900b-0c16a3ade831')
    #
    #
    # for b in range(j):
    #     log_me("Starting block {} of {} - total {}".format(b, n, s))
    #     body = ""
    #     for i in range(n):
    #         if i+b*n >= s:
    #             break
    #         art = logical.iloc[i+b*n]
    #         art_not_na = art[art.notna()]
    #         art_as_dict = dict(art_not_na)
    #         art_as_dict['key'] = art_as_dict['key'].lower()
    #         art_as_json = json.dumps(art_as_dict) + "\n"
    #         # The body of the REST request being prepared
    #         body = body + art_as_json
    #
    #     # Send the request to the Alation instance
    #     r = requests.post(url=url, headers=headers, data=body)
    #     # Were there any errors?
    #     status = (json.loads(r.content))
    #
    #     error_objects = status['error_objects']
    #     if error_objects:
    #         for error in error_objects:
    #             print(error)

    # host = "https://r5-sandbox.alationproserv.com"
    # url = host + "/api/v1/bulk_metadata/extraction/" + str(ds_id) + "?remove_not_seen=true"
    # headers = dict(token='7eaa1400-86c8-4221-900b-0c16a3ade831')
    # r = requests.post(url=url, headers=headers, data=body)
    #
    # status = (json.loads(r.content))
    #
    # params = dict(name=status['job_name'].replace("#", "%23"))
    # url_job = host + "/api/v1/bulk_metadata/job/?name=" + params['name']
    #
    # while (True):
    #     r_2 = requests.get(url=url_job, headers=headers)
    #     status = (json.loads(r_2.content))
    #     if status['status'] != 'running':
    #         error_objects = json.loads(status['result'])['error_objects']
    #         if error_objects:
    #             for error in error_objects:
    #                 print(error)
    #         else:
    #             print(status)
    #         break
    #     else:
    #         time.sleep(3)

    print("All cols: {}".format(cols))
    custom_fields = []
    pickers = {}

    ## We go through all columns in the spreadsheet and decide whether it makes sense to create a picker
    ## field for these values. If yes, we create picker field with all the options

    dimension_articles = []

    for col_name in cols:
        # Let's check how many unique values there are, not counting NULLs
        unique_vals__ = dtv[col_name].dropna().unique()
        unique_vals = len(unique_vals__)
        print("------ {} ({})------".format(col_name, unique_vals))

        if(20<unique_vals<=120):
            custom_fields.append(dict(allow_multiple=False,
                                      allowed_otypes=None,
                                      backref_name=None,
                                      backref_tooltip_text=None,
                                      builtin_name=None,
                                      field_type='PICKER',
                                      name_plural=cut_str(file_key + col_name),
                                      name_singular=cut_str(file_key + col_name),
                                      options=stringify(unique_vals__)))
            ## Let's figure out the top 10 and bottom 10 values and add them to an article for that dimension
            val_1 = dtv.groupby(col_name).size()
            pickers[col_name] = unique_vals
            try:
                val_1_top_10 = val_1.sort_values(ascending=False)[0:min(unique_vals, 10)]
                table_in_body = [add_table_row("Value", "Frequency")]
                for k, v in val_1_top_10.items():
                    table_in_body.append(add_table_row(k, v))
                dimension_articles.append(dict(title="({}) {}".format(file_key, col_name),
                                               body=create_table_html(table_in_body)))

            except:
                print("NO SORTING for COL {}".format(col_name))

            # if (unique_vals > 20):
            #     try:
            #         val_1_bot_10 = val_1.sort_values(ascending=True)[0:min(unique_vals, 10)]
            #         #print("---BOTTOM VALUES---")
            #         #print(val_1_bot_10)
            #     except:
            #         pass


    custom_fields_pd = pd.DataFrame(custom_fields)

    # --- Log into the target instance
    # url_2    = args['host']
    # user_2   = args['username']
    # passwd_2 = args['password']
    # delete_flag = args['delete']
    # target = AlationInstance(url_2, user_2, passwd_2)
    #
    # dd = target.download_datadict_r6(1)  # Alation Analytics is 1 on ABOK

    dimension_articles_ = pd.DataFrame(dimension_articles)
    #dimension_articles_ = dimension_articles_.apply(target.postArticle, axis=1)

    # For one of the dimensions, "View", we will create an article as the parent for all the articles that use
    # that "View"



    c_fields = target.put_custom_fields(custom_fields_pd)
    print(c_fields)
    # returns a list of field IDs (existing or new)
    target.put_custom_template(file_key, c_fields)

    n = 100
    s = dtv.shape[0]
    j = math.floor(s/n) + 1 # how many blocks of 100?

    for b in range(j):
        log_me("Starting block {} of {} - total {}".format(b, n, s))
        body = ""
        for i in range(n):
            if i+b*n >= s:
                break
            art = dtv.iloc[i+b*n]
            art_not_na = art[art.notna()]
            art_as_dict = dict(art_not_na)
            new = {}
            table_in_body = []
            for k, v in art_as_dict.items():
                ## Let's create a row in a table for these values
                table_in_body.append(add_table_row(k, v))

                # If the field is a picker, let's populate the field value
                if k in pickers and pickers[k]>1:
                    new[cut_str(file_key+k)] = v
            # Let's give our article a descriptive and unique name
            new['key'] = "{}-{}-{}-{}".format(file_key, art_as_dict['Data_Item'], art_as_dict['Ref_No'],i)
            #new['key'] = art_as_dict['Term']
            # The body of the article will contain a table with all the values
            # This looks good, but means the picker values are shown again (and need to updated separately)
            new['description'] = create_table_html(table_in_body)
            # The Bulk API expects one JSON string per line
            art_as_json = json.dumps(new) + "\n"
            # The body of the REST request being prepared
            body = body + art_as_json

        # Send the request to the Alation instance
        r=target.put_articles_2(body, file_key)
        # Were there any errors?
        r_ = json.loads(r.content)
        if 'error_objects' in r_:
            # Let's print the first 3 (usually they are repetitive)
            for e in r_['error_objects'][0:3]:
                print(e)


