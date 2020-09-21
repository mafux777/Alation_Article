# -*- coding: utf-8 -*-
import config
import requests
import pandas as pd


"""
# --- put this in a file called config.py
args = {}

args['username'] = '...'
args['password'] = '...'
args['host'] = 'http://...'

args['refresh_token'] = '...'

args['input_file'] = '....csv'
"""

# Read a CSV file with two cols: Group	User
def load_input_file(fn):
    return pd.read_csv(fn)

# Log in to Alation and obtain credential headers
def login():
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
    print("Logging in to {}".format(URL))
    r = s.post(URL, data=payload, verify=verify, headers=headers)

    # get the session ID and store it for all future API calls
    sessionid = s.cookies.get('sessionid')
    if not sessionid:
        print('No session ID, probably wrong user name / password')

    headers = {"X-CSRFToken": csrftoken, "Cookie": f"csrftoken={csrftoken}; sessionid={sessionid}",
               'Referer': f'{host}/groups/'}

    # -- Load all users
    user = requests.get(f"{host}/api/user/", headers=headers).json()
    user_df = pd.DataFrame(user)
    # Find the user ID of this user
    user_id = int(user_df.loc[user_df.username==config.args['username'], 'id'])

    # Obtain a fresh API access token
    data = dict(refresh_token=config.args['refresh_token'], user_id=user_id)
    response = requests.post(host + '/integration/v1/createAPIAccessToken/', json=data)
    api_token = response.json().get('api_access_token')

    return headers, api_token

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
    verify = True
    # Login and remember headers and token
    headers, token = login()
    # Load CSV file, this should have two cols: Group	User
    input = load_input_file(config.args['input_file'])
    # Load all existing users in Alation
    user = requests.get(f"{host}/api/user/", headers=headers).json()
    print(f"You have {len(user)} users on Alation today.")
    # Load all existing groups in Alation
    pre_groups = requests.get(f"{host}/ajax/group/", headers=headers).json()
    print(f"You have {len(pre_groups)} groups on Alation today.")

    # Group by group, create the group and assign the users
    for my_group, my_users in input.groupby('Group'):
        pre_existing_group = find_group_by_name(pre_groups, my_group)
        if pre_existing_group:
            print(f"{my_group} exists.")
            payload = pre_existing_group
        else:
            print(f"Creating {my_group}")
            r = requests.post(f'{host}/ajax/save_group_properties/',
                                        headers=headers,
                                        data=dict(dn="",
                                                  name=my_group,
                                                  defined_in="builtin",
                                                  group='{"users":[],"otype":"group","user_set":[]}'))
            group = r.json()
            payload = group['group']
        # Initialise payload
        payload['user_set'] = []
        # Iterate through the users in the input file
        for u in my_users.User:
            id = find_user_by_name(user, u)
            if id:
                payload['user_set'].append(id)
            else:
                print(f"No record found for {u}")
        r = requests.put(f'{host}/ajax/group/{payload["id"]}/', headers=headers, json=payload)
        if r.status_code == 200:
            print(f"Added {len(payload['user_set'])} users to {my_group}")

