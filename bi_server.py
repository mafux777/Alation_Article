import requests
import json
import pandas as pd
import time
import random
from AlationInstance import AlationInstance
from alationutil import log_me

file_key = ''.join(random.sample("ABCDEFGHJKLMNPQRSTUVWXYZ0123456789", 4))

base_url = ''
username = ''
password = ''

def add_table_row(key, value):
    return "<tr><td> {} </td><td> {} </td></tr>".format(key, value)

def create_table_html(body):
    return "<table><tbody>{body}</tbody></table>".format(body='\n'.join(body))


alation = AlationInstance(base_url, username, password)

users = alation.getUsers()

# {"Steward":[{"type":"user", "key":"test@example.com"}]}
user_list = list(users.username)
random_users = [dict(Report_Owner=[dict(type='user', key=u)]) for u in user_list]
random_users_2 = [dict(otype='user', oid=u) for u in list(users.id)]
# Create a BI Server, by passing a list of 1 URI

bi_server_details = [{"uri": "https://alation.looker.com/browse"}]

bi_server_url = '/integration/v2/bi/server/'
# bi_server will be populated properly by this...
r = alation.generic_api_post(api=bi_server_url, body=bi_server_details)
# {'Status': 'Success: Created 1 servers.', 'Count': 1, 'Errors': [None], 'Server IDs': [48]}
if r['Count']==1:
    bi_server = r['Server IDs'][0]
    alation.update_custom_field(o_type='bi_server', o_id=bi_server, field_id=3, update=file_key)
    log_me(f'Created server {file_key}: {base_url}/bi/v2/server/{bi_server}/')
else:
    log_me(f"Expected one BI Server to be created: {r}")


#bi_server = 123

# =========== Handling of the input file from Customer, containing reports in folders ===============
report_df = pd.read_csv('reports_full.csv', sep=';')
report_df.index = report_df.ID


# using global variables now, not clean
def create_external_id(folder):
    if folder:
        return f'{file_key}+{bi_server}+{folder}'
    # else it the root folder which does not need an external ID


def create_folder_object(name, external_id, parent_folder=None, desc="N/A"):
    bi_folder_details = {
        "name": name,
        "external_id": external_id,
        "created_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.localtime()),
        "last_updated": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.localtime()),
        "source_url": "https://",
        "bi_object_type": "dashboard",
        "owner": "admin",
        "description_at_source": desc,
        "num_reports": None,
        "num_report_accesses": 10,
        "popularity": 100,
        "parent_folder": create_external_id(parent_folder),
        "subfolders": [],
        "connections": [
            "C_001"
        ],
        "reports": [],
        "datasources": [
            "D_001"
        ]
    }
    #print(f'BI Folder: {bi_folder_details["parent_folder"]} -> {external_id}')
    return bi_folder_details



class Folder():
    # The registry is a dictionary of folders
    # key: external_id
    # value: the Folder object, which contains name and parent Folder
    # ...and gets enriched later by the internal ID
    registry = {}
    def __init__(self, name, parent=None):
        name = name.strip()
        self.name = name # this is the short name
        self.parent = parent # this is the long name of the parent

        reg = f'{self}' # this is a long name, taking advantage of recursion, calling __repr__
        # The external ID includes BI Server ID and a random component, too
        # that will be the key to the registry
        external_id = create_external_id(reg)
        if external_id not in Folder.registry:
            # ---- create BI folder -----------
            bif = create_folder_object(name, external_id, parent_folder=parent)
            api = f'{bi_server_url}{bi_server}/folder/'
            # if parent:
            #     print(f'Creating "{parent}/{name}"')
            # else:
            #     print(f'Creating "{name}"')
            r = alation.generic_api_post(api, body=bif)
            if 'status' in r:
                if r['status'] == 'successful':
                    log_me(f'{external_id}:{r["result"]}')
            # ---- keep track of what was created ----
            Folder.registry[external_id] = self
    def __repr__(self):
        if self.parent:
            return f'{self.parent}/{self.name}'
        else:
            return self.name

    @classmethod
    def get_or_create_from_path(cls, path, parent=None,split_char='/'):
        path_items = path.split(split_char)
        n = len(path_items)
        if n == 1:
            return Folder(path_items[0], parent)
        # reduce phase... make first item parent
        p = cls.get_or_create_from_path(path_items[0], parent=parent)
        # call recursively, but with one less item on the path
        q = cls.get_or_create_from_path(path='/'.join(path_items[1:]), parent=p)
        return q

# preparing to store all reports
bi_reports_created = []

# iterate through all the reports in the spreadsheet
for report in report_df.itertuples():

    # clean up Date Created
    if isinstance(report.DateCreated, float):
        date_created = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    else:
        date_created = report.DateCreated
    # clean up Date Modified
    if isinstance(report.LastModification, float):
        date_modified = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    else:
        date_modified = report.LastModification

    # clean up Portal Path
    portal_path = report.Portal_Path.strip()
    if isinstance(report.Report_Name, float):
        report_name="Unnamed Report" # flag it?
    else:
        report_name = report.Report_Name.strip()
    portal_path_components = portal_path.split('/')
    # grab info from the folder and map to custom fields
    # these need to be created on the instance manually for now
    if portal_path_components:
        report_type = portal_path_components[0]
        region = portal_path_components[-1]

    bi_report_details = {
        "name": report.Report_Name,
        "external_id": f'{file_key}+{bi_server}+{portal_path}+{report_name}',
        "created_at": date_created,
        "last_updated": date_modified,
        "source_url": f'{report.BoLink}',
        "bi_object_type": "Dashboard",
        "report_type": "DASHBOARD",
        "owner": f'{report.Owner}',
        "num_accesses": 20,
        "popularity": 50,
        "parent_folder": f'{file_key}+{bi_server}+{portal_path}',
        "parent_reports": [],
        "sub_reports": [],
        "parent_datasources": [
            "D_001"
        ],
        "report_columns": [
        ]
    }
    # create the folder on the instance
    Folder.get_or_create_from_path(portal_path)
    r = alation.generic_api_post(api=f'{bi_server_url}/{bi_server}/report/', body=bi_report_details)
    # read it back so we can learn the Alation ID
    r = alation.generic_api_get(api=f'{bi_server_url}{bi_server}/report/',
                                params={'keyField': 'external_id',
                                  'oids': [f'{file_key}+{bi_server}+{portal_path}+{report_name}']})
    if len(r)==0:
        log_me(f'Warning: {file_key}+{bi_server}+{portal_path}+{report_name} did not download')

    for report_created in r:
        # grab info from the API and store it
        bi_reports_created.append(report_created)
        o_id = report_created['id']
        # create a table with all the other info about the report
        table_in_body = []
        report_dict = report._asdict()
        # iterate through the report metadata values, e.g. author, folder
        for k, v in report_dict.items():
            ## Let's create a row in a table for these values
            if isinstance(v, float):
                continue
            table_in_body.append(add_table_row(k, v))
        # create the description field
        description=create_table_html(table_in_body)
        alation.update_custom_field(o_type='bi_report', o_id=o_id, field_id=4, update=description)
        # send the report type
        alation.update_custom_field(o_type='bi_report', o_id=o_id, field_id=10087, update=report_type)
        # send the region (in UPPER case)
        alation.update_custom_field(o_type='bi_report', o_id=o_id, field_id=10085, update=region.upper())
        # assign a random user on the instance to be the report owner
        alation.update_custom_field(o_type='bi_report', o_id=o_id, field_id=10086, update=random.sample(random_users_2, 1))
        log_me(f'Report created: {base_url}/bi/v2/report/{o_id}/')

# create a DataFrame of all the reports with their IDS
bi_reports_created_df = pd.DataFrame(bi_reports_created)

# Read all folders back in, so we can learn what their ID is and populate the description
for external_id, folder in Folder.registry.items():
    r = alation.generic_api_get(api=f'{bi_server_url}{bi_server}/folder/',
                                params={'keyField':'external_id', 'oids':[external_id]})
    for returned_folder in r:
        try:
        # we know it is a list with only one time
            #folders_read_back.append(returned_folder)

            oid = returned_folder['id']
            description=""
            # which reports have this as their parent?
            reports_in_this_folder = bi_reports_created_df[bi_reports_created_df.parent_folder==external_id]
            # how many?
            n = reports_in_this_folder.shape[0]
            description=f'<p>There are {n} reports in this folder. Showing max 25.</p>'
            if n:
                # let's go through them but not more than 25
                for bi_report in reports_in_this_folder.iloc[0:min(n, 25),].itertuples():
                    id = bi_report.id
                    # generate the at-mention link
                    mention = f'<p><a data-oid="{id}" data-otype="bi_report" href="/bi/v2/report/{id}/"></a></p>\n'
                    description += mention
                alation.update_custom_field(o_type='bi_folder', o_id=oid, field_id=4, update=description)
        except:
            log_me(f'Issue with {folder}')







