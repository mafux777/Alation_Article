# -*- coding: utf-8 -*-
import time

import urllib3.util
import json
import config
import requests
import pandas as pd
import urllib3
import sqlparse
from datetime import datetime, timezone
import hashlib
import pandas as pd

# to avoid getting a warning for self-signed cert
urllib3.disable_warnings()

"""
# --- put this in a file called config.py
args = {}

args['username'] = '...'
args['password'] = '...'
args['host'] = 'https://...'
args['input_file'] = '....csv'
"""

# Read a CSV file with two cols: Group	User
def load_input_file(fn):
    return pd.read_csv(fn)

# Log in to Alation and obtain credential headers
def login(verify):
    # --- Log in
    URL = host + '/login/'
    s = requests.Session()
    s.get(URL, verify=verify)

    # get the cookie token
    csrftoken = s.cookies.get('csrftoken')

    # login with user name and password (and token)
    payload = {"csrfmiddlewaretoken": csrftoken,
               "ldap_user": config.args['username'],
               "password": config.args['password']}
    headers = {"Referer": URL}
    headers['content-type'] = 'application/x-www-form-urlencoded'
    print("Logging in to {}".format(URL))
    params=dict(next=None)
    r = s.post(URL, data=payload, verify=verify, headers=headers, params=params)

    # get the session ID and store it for all future API calls
    sessionid = s.cookies.get('sessionid')
    if not sessionid:
        print('No session ID, probably wrong user name / password')

    headers = {"X-CSRFToken": csrftoken, "Cookie": f"csrftoken={csrftoken}; sessionid={sessionid}",
               'Referer': f'{host}/groups/'}

    # -- Load all users
    user = requests.get(f"{host}/api/user/", headers=headers, verify=verify).json()
    user_df = pd.DataFrame(user)

    # -- Obtain a new refresh token
    # data = dict(username=config.args['username'],
    #             password=config.args['password'],
    #             name="New Refresh Token")
    # response = requests.post(host + '/integration/v1/createRefreshToken/', json=data, verify=verify)
    # refresh_token = response.json().get('refresh_token')
    # # Find the user ID of this user
    # user_id = response.json().get('user_id')
    # with open("my_token.json", "w") as f:
    #     my_dict = dict(refresh_token=refresh_token, user_id=user_id)
    #     f.write(json.dumps(my_dict))

    # Read refresh token from file
    with open("my_token.json", "r") as f:
        j = f.read()
        my_dict = json.loads(j)
        refresh_token = my_dict.get("refresh_token")
        user_id = my_dict.get("user_id")


    # Obtain a fresh API access token
    data = dict(refresh_token=refresh_token, user_id=user_id)
    response = requests.post(host + '/integration/v1/createAPIAccessToken/', json=data, verify=verify)
    api_token = response.json().get('api_access_token')

    return headers, api_token, s, user_df

from bs4 import BeautifulSoup
from bs4.element import Tag
from sqlparse.sql import Token, Parenthesis, Statement

outputs = []
inputs = []

def recursive_extraction(p0):
    if isinstance(p0, Statement):
        token_seq = [t for t in p0.tokens if not t.is_whitespace]
    else:
        token_seq = [t for t in p0 if not t.is_whitespace]
    for n, t in enumerate(token_seq):
        if hasattr(t, 'tokens') and isinstance(t.tokens[0], Parenthesis):
            sub_tokens = t.tokens[0].tokens
            my_len = len(sub_tokens)
            if not my_len:
                print(f"Error, nothing in there")
            sub_statement = sub_tokens[1:my_len-2]
            recursive_extraction(sub_statement)
        if token_seq[n].normalized == "INSERT" and token_seq[n+1].normalized == "INTO":
            outputs.append(token_seq[n+2])
        elif t.normalized == "FROM":
            inputs.append(token_seq[n + 1])
        elif "JOIN" in t.normalized:
            inputs.append(token_seq[n + 1])


def recursive_extraction_copy(p0):
    if isinstance(p0, Statement):
        token_seq = [t for t in p0.tokens if not t.is_whitespace]
    else:
        token_seq = [t for t in p0 if not t.is_whitespace]
    for n, t in enumerate(token_seq):
        if hasattr(t, 'tokens') and isinstance(t.tokens[0], Parenthesis):
            sub_tokens = t.tokens[0].tokens
            my_len = len(sub_tokens)
            if not my_len:
                print(f"Error, nothing in there")
            sub_statement = sub_tokens[1:my_len-2]
            recursive_extraction(sub_statement)
        if token_seq[n].normalized == "COPY" and token_seq[n+2].normalized == "FROM":
            outputs.append(token_seq[n+1])
            inputs.append(token_seq[n+3])


def parse_schema(headers, token, session):
    global outputs, inputs

    infile = open("/Users/matthias.funke/Downloads/Example_project_file.xml","r")
    contents = infile.read()
    soup = BeautifulSoup(contents,'xml')

    # Amazon Redshift Tasks with a SQL Statement
    amazon_redshift_task = soup.find_all('AmazonRedshiftExecuteSqlTask')
    for a in amazon_redshift_task:
        my_attribs = a.attrs
        if "SqlStatementSource" in my_attribs:
            for my_split in sqlparse.split(my_attribs["SqlStatementSource"]):
                p = sqlparse.parse(my_split)
                for p0 in p:
                    recursive_extraction_copy(p0)
                    input_keys = []
                    output_keys = []
                    for i in inputs:
                        input_keys.append(dict(otype="file", key=i.normalized))
                    for o in outputs:
                        t = validate_table(o.normalized, headers, token, session)
                        if t:
                            output_keys.append(dict(otype="table", key=t))
                        else:
                            output_keys.append(dict(otype="external", key=o.normalized))
                    external_id = hashlib.sha224(p0.value.encode('utf-8')).hexdigest()[0:16]
                    if input_keys and output_keys:
                        create_dataflow(input_keys, output_keys, headers, token, session,
                                        external_id=f"api/{external_id}",
                                        title=f"{a.Connection} {external_id}",
                                        content=p0.value)
                    else:
                        print(f"Not enough data for {external_id}")
                    outputs = []
                    inputs = []

    pipelines = soup.find_all('pipeline')
    for p in pipelines:
        for p0 in p.find_all('property'):
            if p0.attrs.get('name') == 'Query':
                content = p0.text
                external_id = p0.parent.parent.parent.parent.parent.parent.attrs.get("DTS:DTSID")
                title = p0.parent.parent.parent.parent.parent.parent.attrs.get("DTS:ObjectName")
                parsed = sqlparse.parse(content)[0]
                token_seq = [t for t in parsed.tokens if not t.is_whitespace]
                input_keys = None
                for n, t in enumerate(token_seq):
                    if token_seq[n].normalized == "FROM":
                        input_keys = dict(otype="table", key=validate_table(token_seq[n+1].normalized, headers, token, session))

            elif p0.attrs.get('name') == 'TableName':
                my_table = p0.text
                if my_table:
                    t = validate_table(my_table, headers, token, session)
                    if t:
                        create_dataflow([input_keys],
                                    [dict(otype='table', key=t)],
                                    headers,
                                    token,
                                    session,
                                    external_id=f"api/{external_id}",
                                    title=f"{title} {external_id}",
                                    content=content)

    sql_tasks = soup.find_all('SqlTaskData')
    for my_sql_task in sql_tasks:
        statement = my_sql_task.attrs.get('SQLTask:SqlStatementSource')
        if not statement:
            continue
        # print(f"Found SQL tasks: {e.attrs}")
        for my_split in sqlparse.split(statement):
            p = sqlparse.parse(my_split)
            for p0 in p:
                sqltype = p0.get_type()
                if sqltype in ['INSERT', ]:
                    recursive_extraction(p0)
                    input_keys = []
                    output_keys = []
                    for i in inputs:
                        t = validate_table(i.normalized, headers, token, session)
                        if t:
                            input_keys.append(dict(otype="table", key=t))
                    for o in outputs:
                        t = validate_table(o.normalized, headers, token, session)
                        if t:
                            output_keys.append(dict(otype="table", key=t))
                    external_id = my_sql_task.parent.parent.attrs.get('DTS:DTSID')
                    if input_keys and output_keys:
                        create_dataflow(input_keys, output_keys, headers, token, session,
                                        external_id=f"api/{external_id}",
                                        title=f"{my_sql_task.parent.parent.attrs.get('DTS:ObjectName')} {external_id}",
                                        content=p0.value)
                    else:
                        print(f"Not enough data for {external_id}")
                    outputs = []
                    inputs = []


def create_dataflow(input_keys, output_keys, headers, token, session,
                    external_id, title, content):
    body = {
        "dataflow_objects" : [dict(external_id=external_id,
                                   title=title,
                                   description=f"Nothing yet",
                                   content=content)],
        "paths" : [
            [input_keys, [{"otype": "dataflow", "key": external_id}], output_keys]
        ]
    }
    print(f"External ID: {external_id}")
    print(f"Ttitle: {title}")
    r = session.post(host+'/integration/v2/lineage/',
                     headers=dict(token=token),
                     json=body
                     )
    get_job_status(r.json())


def get_job_status(status):
    # -- Get the status and print it
    params = dict(id=status['job_id'])
    url_job = host + "/api/v1/bulk_metadata/job/"
    while (True):
        r_2 = session.get(url=url_job, headers=dict(token=token), params=params)
        status = r_2.json()
        objects = status.get('result')
        if objects:
            print(objects)
            break
        else:
            print(status)
            print(f'Job still running: {datetime.now(timezone.utc).strftime(r"%Y-%m-%d %H:%M:%S.%f")}')
            time.sleep(3)


def validate_table(t, headers, token, session):
    # -- see if the table exists --
    if "." in t:
        components = t.split(".")
        schema = components[0].lower()
        table = components[1].lower()
    else:
        table = t.lower()
        schema = ""
    r = session.get(host+'/integration/v2/table/',
                    headers=dict(token=token),
                    params=dict(name=table))

    if r.status_code:
        try:
            r1 = r.json()
            for my_table in r1:
                # print(f"Key: {my_table.get('key')}")
                # print(f"URL: {host}{my_table.get('url')}")
                if schema in my_table.get('key'):
                    return my_table.get('key')
        except Exception as e:
            print(f"Exception: {e}")

def clean_lineage(host, headers, token, session):
    dataflows = session.get(host+'/integration/v2/dataflow/',
                            headers=dict(token=token)).json()
    df = pd.DataFrame(dataflows.get('dataflow_objects'))
    ids = df.external_id
    deleted_dataflows = session.delete(host+'/integration/v2/dataflow/',
                            headers=dict(token=token),
                            params=dict(keyField="external_id"),
                            json = list(ids)
                                       )
    get_job_status(deleted_dataflows.json())

if __name__ == "__main__":
    # Remember the host name
    host = config.args['host']
    # Set verify to False only if working with self-signed certs
    verify = True
    # Login and remember headers and token
    headers, token, session, users = login(verify)

    clean_lineage(host, headers, token, session)
    parse_schema(headers, token, session)
