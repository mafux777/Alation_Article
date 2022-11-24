# -*- coding: utf-8 -*-
import urllib3.util
import json
import requests
import pandas as pd
import urllib3

# to avoid getting a warning for self-signed cert
urllib3.disable_warnings()

# Read a CSV file with two cols: Group	User
def load_input_file(fn):
    return pd.read_csv(fn)

host = "https://beta-sandbox.alationproserv.com"
token = "6OP8hhxkswOSP6t1ZJKsY8DNDTxCyxcgYF0ywjNcv7U"

if __name__ == "__main__":
    # Set verify to False only if working with self-signed certs
    verify = False
    # Login and remember headers and token
    headers = dict(token=token)

    # Load all existing users in Alation
    user = requests.get(f"{host}/api/user/", headers=headers, verify=verify).json()
    users = pd.DataFrame(user)
    print(f"You have {len(user)} users on Alation today.")
    # Save existing group mapping
    my_mapping = []
    for _, u in users.iterrows():
        for g in u['groups']:
            if not g['profile']['is_builtin']:
                my_mapping.append(dict(user=u['username'], group=g['profile']['name']))
    mapped = pd.DataFrame(my_mapping)
    # mapped.to_csv("my_users_groups.csv", index=False)

    # Load CSV file, this should have two cols: group	user
    input = pd.read_csv("./my_users_groups.csv")

    # Load all existing groups in Alation
    pre_groups = requests.get(f"{host}/ajax/group/", headers=headers, verify=verify).json()
    print(f"You have {len(pre_groups)} groups on Alation today.")
    groups_df = pd.DataFrame(pre_groups)

    # Group by group, create the group and assign the users
    for my_group, my_users in input.groupby('group'):
        pre_existing_group = groups_df.loc[groups_df.name==my_group]
        if pre_existing_group.shape[0]:
            print(f"{my_group} exists.")
            payload = pre_existing_group.iloc[0]
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
            x = users.loc[users.username==u, 'id']
            if x.shape[0]==1:
                payload['user_set'].append(int(x.iloc[0]))
            else:
                print(f"No record found for {u}")
        my_payload = dict(payload)
        my_payload['id'] = int(my_payload['id'])
        my_payload['is_editable'] = bool(my_payload['is_editable'])
        my_payload['send_notifications'] = bool(my_payload['send_notifications'])
        r = requests.put(f'{host}/ajax/group/{payload["id"]}/', headers=headers, json=my_payload, verify=verify)
        if r.status_code == 200:
            print(f"Added {len(payload['user_set'])} users to {my_group}")
