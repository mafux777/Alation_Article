# -*- coding: utf-8 -*-
import urllib3.util
import json
import config
import requests
import pandas as pd
import urllib3

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

# Given a list of groups (from the group API) return only the one with the matching name
def find_group_by_name(groups, name):
    for g in groups:
        if g.get('name')==name:
            return g

# Given a list of users (from the user API) return only the one with the matching name
def find_user_by_name(users, name):
    for u in users:
        if u.get('username')==name:
            return u.get('id')


if __name__ == "__main__":
    # Remember the host name
    host = config.args['host']
    # Set verify to False only if working with self-signed certs
    verify = False
    # Login and remember headers and token
    headers, token, session, users = login(verify)
    headers = dict(token=token)
    # Save existing group mapping
    my_mapping = []
    for _, u in users.iterrows():
        for g in u['groups']:
            if not g['profile']['is_builtin']:
                my_mapping.append(dict(user=u['username'], group=g['profile']['name']))
    mapped = pd.DataFrame(my_mapping)
    # mapped.to_csv("my_users_groups.csv", index=False)

    # Load CSV file, this should have two cols: group	user
    input = pd.read_csv("/Users/matthias.funke/PycharmProjects/Alation_Article/my_users_groups.csv")

    # Load all existing users in Alation
    user = requests.get(f"{host}/api/user/", headers=headers, verify=verify).json()
    print(f"You have {len(user)} users on Alation today.")
    # Load all existing groups in Alation
    pre_groups = requests.get(f"{host}/ajax/group/", headers=headers, verify=verify).json()
    print(f"You have {len(pre_groups)} groups on Alation today.")

    # Group by group, create the group and assign the users
    for my_group, my_users in input.groupby('group'):
        pre_existing_group = find_group_by_name(pre_groups, my_group)
        if pre_existing_group:
            print(f"{my_group} exists.")
            payload = pre_existing_group
        else:
            print(f"Creating {my_group}")
            r = requests.post(f'{host}/ajax/save_group_properties/',
                                        headers=headers,
                                        verify=verify,
                                        data=dict(dn="",
                                                  name=my_group,
                                                  defined_in="builtin",
                                                  group='{"users":[],"otype":"group","user_set":[]}'))
            group = r.json()
            payload = group['group']
        # Initialise payload
        payload['user_set'] = []
        # Iterate through the users in the input file
        for u in my_users.user:
            id = find_user_by_name(user, u)
            if id:
                payload['user_set'].append(id)
            else:
                print(f"No record found for {u}")
        r = requests.put(f'{host}/ajax/group/{payload["id"]}/', headers=headers, json=payload, verify=verify)
        if r.status_code == 200:
            print(f"Added {len(payload['user_set'])} users to {my_group}")


"""
    # POC CODE ONLY
    payload = dict(action="suspend", user_id=11)
    api = "/admin/user_state/"
    headers['Referer'] = host+api
    r = requests.put(host+api, headers=headers, json=payload, verify=verify)


    payload = {"fields": ["user", "display_name", "email", "title", "role", "group"], "bad_rows": "",
               "new_users": [
                                {"user": "purple_herring", "display_name": "purple Herring", "email": "purpleherring@alation.com", "title": "Fish Doctor",
                                 "role": "STEWARD", "group": ""},
                                {"user": "white_marlin", "display_name": "White Marlin", "email": "whitemarlin@alation.com", "title": "Fish Doctor",
                                 "role": "STEWARD", "group": ""},
               ], "changed_users": [], "unchanged_users": [], "skipped_rows": ""}
    separators = (',', ':')
    data = dict(payload=json.dumps(payload, separators=separators),
                overwrite='no',
                csrfmiddlewaretoken=headers['X-CSRFToken'])
    headers['content-type'] = 'application/x-www-form-urlencoded'
    headers['Referer'] = host+'/admin/import_user_profiles/'
    r = requests.post(host+'/admin/import_user_profiles/', headers=headers, data=data, verify=verify)
"""