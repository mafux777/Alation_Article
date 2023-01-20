# -*- coding: utf-8 -*-
import urllib3.util
import json
import config
import requests
import pandas as pd
import urllib3
import urllib.parse

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
    URL = host + '/login/?nosaml=True'
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
    data = dict(username=config.args['username'],
                password=config.args['password'],
                name="New Refresh Token")
    response = requests.post(host + '/integration/v1/createRefreshToken/', json=data, verify=verify)
    refresh_token = response.json().get('refresh_token')
    # Find the user ID of this user
    user_id = response.json().get('user_id')

    # Obtain a fresh API access token
    data = dict(refresh_token=refresh_token, user_id=user_id)
    response = requests.post(host + '/integration/v1/createAPIAccessToken/', json=data, verify=verify)
    api_token = response.json().get('api_access_token')

    return headers, api_token, s, user_df


if __name__ == "__main__":
    # Remember the host name
    host = config.args['host']
    # Set verify to False only if working with self-signed certs
    verify = False
    # Login and remember headers and token
    headers, token, session, users = login(verify)


    my_search = dict(
        q = "",
        offset=0,
        limit=100,
        row_num=0,
        compute_facets='true',
        show_spelling_suggestions='true',
        filters=json.dumps(dict(
            otypes=["bi_report"],
        ))
    )
    search = requests.get(f"{host}/search/v1", headers=dict(token=token), params=my_search, verify=verify).json()
    # search = session.get(f"{host}/search/v1", headers=headers, params=my_search, verify=verify).json()
    print(f"All done.")