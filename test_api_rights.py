import requests
import json

import urllib3
urllib3.disable_warnings()

"""
### Django Shell Code to create the test users (run there first)
for i in range(0, 6):
    ...:     username = "__api_user_{id}_@mail.com".format(id=i)
    ...:     email = username
    ...:     u = User.objects.create(username=username, email=email)
    ...:     u.confirm_email(u.confirmation_key)
    ...:     u.set_password("password{id}".format(id=i))
    ...:     u.save()
    ...:     p, _ = UserProfile.create_user_profile(user=u, display_name=username)
    ...:     p.assign_role(i)
"""



users = [
    dict(role='Server Admin', username='__api_user_0_@mail.com', password='password0'),
    dict(role='Steward', username='__api_user_1_@mail.com', password='password1'),
    dict(role='Catalog Admin', username='__api_user_2_@mail.com', password='password2'),
    dict(role='Viewer', username='__api_user_3_@mail.com', password='password3'),
    dict(role='Composer', username='__api_user_4_@mail.com', password='password4'),
    dict(role='Source Admin', username='__api_user_5_@mail.com', password='password5')
]

url = 'https://ec2-18-218-6-215.us-east-2.compute.amazonaws.com'
ds_id = 70

api_list = [
    f'/integration/v1/datasource/{ds_id}',
    f'/integration/v1/schema/?ds_id={ds_id}',
    f'/integration/v1/table/?ds_id={ds_id}',
    f'/integration/v1/column/?ds_id={ds_id}',
    '/integration/v1/article/',
    '/integration/v1/custom_template/',
    '/integration/v2/bi/server/',
    '/integration/v1/user/',
    '/integration/v2/dataflow/'
]
for role in users:
    print('---------------------')
    print(role['role'])
    r = requests.post(url=url+'/integration/v1/createRefreshToken/',
                     json=dict(username=role['username'],
                              password=role['password'],
                              name=f"API Token for {role['username']}"),
                     verify=False)
    refresh_token = r.json().get('refresh_token')
    user_id = r.json().get('user_id')
    print(f"Created a refresh token for user ID {user_id}")
    r = requests.post(url=url+'/integration/v1/createAPIAccessToken/',
                     json=dict(refresh_token=refresh_token,
                              user_id=user_id),
                     verify=False)
    api_access_token = r.json().get('api_access_token')

    print('---------------------')
    for a in api_list:
        url_ = url + a
        r = requests.get(url=url_,
                         headers=dict(token=api_access_token),
                         params=dict(limit=25),
                         verify=False)
        if r.status_code==500:
            print(f"Request = {r.request.url} Status code {r.status_code} with {r.content}.")
            continue
        r_parsed = json.loads(r.content)
        if isinstance(r_parsed, list):
            print(f"Request = {r.request.url} Status code {r.status_code} with {len(r_parsed)} items.")
            # if len(r_parsed)<=50:
            #     for i in r_parsed:
            #         print(i)
        else:
            print(f"Request = {r.request.url} Status code {r.status_code} with {r_parsed}.")
