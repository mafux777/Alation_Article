# -*- coding: utf-8 -*-
import config
import requests

groups_to_create = [{'Builders-2':[2, 3, 5]}, {'Farmers-2': [2, 4, 8]}]

if __name__ == "__main__":
    host = config.args['host']
    verify = True

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

    headers = {"X-CSRFToken": csrftoken, "Cookie": f"csrftoken={csrftoken}; sessionid={sessionid}", "Referer": URL,
               'Referer': f'{host}/groups/'}

    user = requests.get(f"{host}/api/user/", headers=headers).json()
    pre_groups = requests.get(f"{host}/integration/v1/group/", headers=dict(token="Aw0Bp8oSE2RZVrWtnNvW05dJtrk5GrFVCt0A-PZgafE"))
    for my_group in groups_to_create:
        name = list(my_group.keys())[0]
        r = requests.post(f'{host}/ajax/save_group_properties/',
                                        headers=headers,
                                       data=dict(dn="", name=name, defined_in="builtin", group='{"users":[],"otype":"group","user_set":[]}'))
        group = r.json()
        payload = group['group']
        payload['user_set'] = my_group[name]
        for u in payload['user_set']:
            payload['users'].append(user[u-1])
        r = requests.put(f'{host}/ajax/group/{payload["id"]}/', headers=headers, json=payload)
        print(r)

