import requests
import time
import pandas as pd
import json
from bs4 import BeautifulSoup
import urllib

from sudoku.alationutil import log_me, list_files, extract_files
import errno
import os
import zipfile
from collections import OrderedDict, deque, defaultdict
from math import isnan
from datetime import datetime, timezone
import uuid
import re




# Safe method for converting a number in str or float format to int
def to_int(n):
    if pd.isnull(n):
        return 0
    if isinstance(n, str):
        try:
            f = float(n)
            rf = round(f)
            return int(rf)
        except Exception as e:
            log_me(f"Could not convert {n} to int.")
            return None
    elif isinstance(n, float):
        rf = round(n)
        return int(rf)
    elif isinstance(n, int):
        return n
    else:
        log_me(f"Could not convert {n} to int.")


# The AlationInstance class is a handle to an Alation server defined by a URL
# A server admin user name and password needs to be provided and all API actions
# will be run as that user
class AlationInstance():
    # The __init__ method is the constructor used for instantiating
    # email: the up to 30 chars user name, often the email, but for long emails could be cut off
    # password: could be the LDAP password, as well
    # verify: Requests verifies SSL certificates for HTTPS requests, just like a web browser.
    # By default, SSL verification is enabled, and Requests will throw a SSLError if it’s unable to verify the certificate
    def __init__(self, host, account, password, refresh_token, user_id,verify=True):
        self.host = host
        self.verify = verify
        self.account = account
        self.refresh_token = refresh_token
        self.user_id = user_id
        self.password = password
        self.token = self.get_token()
        self.headers = dict(token=self.token)
        # self.headers = self.login(account, password)
        self.api_columns = ['otype', 'id']
        self.external_id = {}
        self.bi_folders = None
        self.bi_reports = None
        self.fqn_cache = {}
        log_me("Getting existing custom fields")
        self.existing_fields = self.get_custom_fields() # store existing custom fields
        self.endpoint = dict(bi_folder="folder", bi_report="report", bi_report_column="report/column", )
        log_me("Getting existing templates")
        self.existing_templates = self.get_templates() # store existing templates
        # log_me("Getting existing data sources")
        # self.ds = self.getDataSources()
        # self.articles = pd.DataFrame() # cache for Articles
        # if self.ds.shape[0]:
        #     log_me(self.ds.loc[ : , ['id', 'title']].head(10))

    # The login method is used to obtain a session ID and relevant cookies
    # They are cached in the headers variable
    # account: the up to 30 chars user name, often the email, but for long emails could be cut off
    # password: could be the LDAP password, as well
    def login(self, account, password):
        URL = self.host + '/login/'

        s = requests.Session()
        s.get(URL, verify=self.verify)

        # get the cookie token
        csrftoken = s.cookies.get('csrftoken')

        # login with user name and password (and token)
        # payload = {"csrfmiddlewaretoken": csrftoken, "ldap_user": account, "password": password}
        # headers = {"Referer": URL}
        # log_me("Logging in to {}".format(URL))
        # r = s.post(URL, data=payload, verify=self.verify, headers=headers)
        payload = {"csrfmiddlewaretoken": csrftoken,
                   "ldap_user": account,
                   "password": password}
        headers = {"Referer": URL}
        headers['content-type'] = 'application/x-www-form-urlencoded'
        print("Logging in to {}".format(URL))
        params = dict(next=None)

        r = s.post(URL, data=payload, verify=self.verify, headers=headers, params=params)
        # get the session ID and store it for all future API calls
        sessionid = s.cookies.get('sessionid')
        if not sessionid:
            log_me('No session ID, probably wrong user name / password')
        headers = {"X-CSRFToken": csrftoken,
                   "Cookie": f"csrftoken={csrftoken}; sessionid={sessionid}",
                   "Referer": URL
                   }

        return headers

    # The get_custom_fields method returns a pandas DataFrame with all custom fields
    # The Alation ID will also be the ID of the DataFrame
    def get_custom_fields(self, template='all'): # this method returns a DataFrame
        my_fields = []
        next="/integration/v2/custom_field/"
        while(next):
            r = requests.get(self.host + next, headers=dict(token=self.token), params={}, verify=self.verify)
            next = r.headers.get("x-next-page")
            my_fields.extend(r.json())
        fields = pd.DataFrame(my_fields)
        fields.index = fields.id
        return fields.sort_index()

    # The get_custom_field_id methdod checks whether a field with that name already exists
    # If yes, returns the ID
    # If no, returns 0
    def get_custom_field_id(self, name):
        if name in self.existing_fields.name:
            return self.existing_fields[self.existing_fields.name == name, "id"]
        else:
            return 0


    # The get_articles method downloads all articles of a specific template (if provided)
    # It does this in chunks of 100, transparent to the user
    # template: name of the template or "all"
    # limit: chunk size (optional)
    # returns a pandas DataFrame
    def get_articles(self, template='all', limit=100):
        log_me("Getting Articles from Instance")
        skip = 0
        articles = pd.DataFrame()
        params = {}
        if template != 'all': # find out the numerical template ID for the desired template
            template_id = list(
                self.existing_templates.loc[self.existing_templates.title == template, 'id'])
            # pass the numerical ID as a parameter to the Article API
            params['custom_field_templates'] = "[{}]".format(int(template_id[0]))
            log_me("Filtering on: {}{}".format(template, template_id))
        # Enter an infinite loop until we have no more articles to download
        while True:
            try:
                # use limit and skip parameters to control the articles downloaded
                params['limit'] = limit
                params['skip'] = skip
                t0 = time.time()
                r = self.generic_api_get("/integration/v1/article/", params=params)
                skip = skip + limit
                # create the DataFrame and index it properly
                article_chunk = pd.DataFrame(r)
                # re-use the article ID as the index of the DataFrame
                article_chunk.index = article_chunk.id
                articles = articles.append(article_chunk)
                size = article_chunk.shape[0]
                log_me("Took {} secs for {} items".format(time.time()-t0, size))
                if size < limit: # not enough articles to continue
                    log_me("Total number of articles downloaded: {}".format(skip-limit+size))
                    break
            except:
                break
        self.articles = articles # cache for later!
        return articles

    # The get_tables_by_name method returns an (empty) DataFrame for all tables with the same exact name (if found)
    # name: name to search for, consisting of schema.name
    def get_tables_by_name(self, name):
        components = name.split('.')
        schema_name = components[0]
        table_name  = components[1]
        params = dict(name=table_name, schema_name=schema_name)
        # create the DataFrame and index it properly
        table_with_name = pd.DataFrame(self.generic_api_get("/integration/v1/table/", params=params))
        size = table_with_name.shape[0]
        if size>0:
            table_with_name.index = table_with_name.id
            return table_with_name[table_with_name.name == table_name]
        else:
            #log_me("Could not find table {}".format(name))
            return pd.DataFrame()

    # The get_article_by_id method returns a dictionary with all the article attributes provided by the Article API
    # ID: the numerical ID of an existing article
    def get_article_by_id(self, id):
        # return a dictionary
        article = self.generic_api_get(f'/integration/v1/article/{id}/')
        return article

    # The post_article method creates a new article based on a dictionary with at least title and body
    # and returns a dictionary with all attributes, e.g. id, author, timestamp, etc.
    def post_article(self, article):
        art = self.generic_api_post(api="/integration/v1/article/", body=dict(article))
        return art

    # The del_article method deletes an existing article and returns nothing
    def del_article(self, id):
        url = self.host + "/integration/v1/article/" + str(id) + "/"
        r = requests.delete(url, headers=self.headers, verify=self.verify)

    def del_bi_server(self, id):
        url = self.host + "/integration/v1/article/" + str(id) + "/"
        r = requests.delete(url, headers=self.headers, verify=self.verify)

    # The update_article method updates an article with a specific ID and returns the modified article
    def update_article(self, id, article):
        url = self.host + "/integration/v1/article/" + str(id) + "/"
        article_json = json.dumps(article)
        self.headers['Content-Type'] = "application/json"
        r = requests.put(url, headers=self.headers, verify=self.verify, data=article_json)
        if not r:
            try:
                log_me("Issue with updating article {}...".format(article['title']))
                log_me("... {}".format(r.content))
            except:
                log_me("Formatting issue with article id={}".format(id))
        # return a dictionary
        art = r.json()
        return art

    # The download_datadict method returns a pandas DataFrame with key, title and description for a specific data source
    # This only works until R5 (inclusive)
    def download_datadict(self, ds_id):
        api = f"/data/{ds_id}/download_dict/data/{ds_id}/"
        params = dict(format='json')
        r = self.generic_api_get(api, params=params)
        dd = pd.DataFrame(r[1:]) # skipping first row, no key
        dd.index = dd.key
        log_me(f"This data dict contains {dd.shape[0]} items.")
        return dd.loc[:, ['key', 'title', 'description']]

    # The download_datadict_r6 uses the metadata API to download a data dictionary
    # It will contain key, title, description, and the numerical IDs of the schema, table, column
    # Return a dataframe
    #
    def download_datadict_r6(self, ds_id):
        # The metadata APIs all require a token
        token = self.get_token()
        headers=dict(token=token)
        # Store all elements here
        list_of_elements = list()

        def download_objects(initial_url):
            # Count all items
            n = 0
            log_me(f"Downloading from {initial_url}")
            # download the initial list of objects
            r = requests.get(url=self.host + initial_url, headers=headers)
            if not r.status_code:
                log_me(r.content)
                return None # nothing
            elements = r.json()
            n += len(elements)
            log_me(f"Downloaded {n} elements so far")

            # if there are any more, keep going
            while 'X-Next-Page' in r.headers:
                r = requests.get(url=self.host + r.headers['X-Next-Page'], headers=headers)
                if not r.status_code:
                    log_me(r.content)
                    break
                n += len(r.json())
                # Append each element to the list
                # so the return list does not become nested
                for e in r.json():
                    elements.append(e)
                log_me(f"Downloaded {n} elements so far")

            # All done? Return
            return elements

        # Data Source
        url = self.host + f'/integration/v1/datasource/{ds_id}'
        r = requests.get(url=url, headers=headers)
        ds = r.json()
        list_of_elements.append(dict(
            key=str(ds['id']),
            title=ds['title'],
            description=ds['description']
        ))

        # Schema
        for schema in download_objects(f'/integration/v1/schema/?ds_id={ds_id}'):
            name = schema['name']
            list_of_elements.append(dict(
                key         = f'{ds_id}.{name}',
                title       = schema['title'],
                description = schema['description'],
                schema_id   = str(schema['id'])
            ))

        # Table
        for table in download_objects(f'/integration/v1/table/?ds_id={ds_id}'):
            name = table['name']
            schema_name = table['schema_name']
            list_of_elements.append(dict(
                key         = f'{ds_id}.{schema_name}.{name}',
                title       = table['title'],
                description = table['description'],
                schema_id   = str(table['schema_id']),
                table_id    = str(table['id'])
            ))

        # Column
        for col in download_objects(f'/integration/v1/column/?ds_id={ds_id}'):
            name = col['name']
            table_name = col['table_name'] # includes the schema already
            list_of_elements.append(dict(
                key         = f'{ds_id}.{table_name}.{name}',
                title       = col['title'],
                description = col['description'],
                schema_id   = str(col['schema_id']),
                table_id    = str(col['table_id']),
                column_id   = str(col['id']),
                data_type   = col['data_type']

            ))


        dd = pd.DataFrame(list_of_elements)
        return dd

    def get_token(self):
        data = {
            "refresh_token": self.refresh_token,
            "user_id": self.user_id
        }

        # Get APIAccessToken for user using the RefreshToken.
        res = requests.post(f'{self.host}/integration/v1/createAPIAccessToken/', data=data).json()
        api_token = res.get('api_access_token')
        return api_token

    # The get_templates method returns a DataFrame with all templates, sorted and indexed by ID
    # This includes built-in templates
    def get_templates(self):
        api = "/integration/v1/custom_template/"
        templates = pd.DataFrame(self.generic_api_get(api, official=True))
        templates.index = templates.id
        return templates.sort_index()

    # The delete_customfield method deletes a single custom field by ID
    # Note this may fail if any templates still use the custom field
    def delete_customfield(self, field_id):
        url = self.host + "/ajax/custom_field/" + str(field_id) + "/"
        r = requests.delete(url, headers=self.headers, verify=self.verify) # no longer passing data parameter

    # The add_customfield_to_template method adds a single existing custom field to an existing template
    # It checks whether the field is already there and does nothing if that is the case
    # template_id: numerical ID of the template
    # field_id: numerical ID of the custom field
    # return value: none
    def add_customfield_to_template(self, template_id, field_id): #takes only one field
        # get details about the template first
        template = self.generic_api_get(f"/ajax/custom_template/{template_id}/")
        # copy details of the template into the payload
        keys = ["id", "title", "builtin_name", "field_ids", "template_in_use"]
        payload = {}
        for k in keys:
            payload[k] = template[k]
        # only if the field ID is not already in the list of fields:
        if field_id not in template['field_ids']:
            # create a new list of all field IDs in string format including the new one
            payload['fields'] = [str(x) for x in template['field_ids'] + [field_id]]
            url = self.host + "/ajax/custom_template/" + str(template_id) + "/"
            requests.put(url, data=json.dumps(payload), headers=self.headers, verify=self.verify)

    # The add_customfields_to_template method adds a list of custom field IDs to an existing template
    # It checks whether the field is already there and does nothing if that is the case
    # template_id: numerical ID of the template
    # field_ids: numerical IDs of the custom fields
    # return value: none
    def add_customfields_to_template(self, template_id, field_ids): #takes a list of fields
        # get details about the template first
        #log_me("Adding fields ({}) to template {}".format(field_ids, template_id))
        r = requests.get(self.host + "/ajax/custom_template/" + str(template_id) + "/",
                         headers=self.headers, verify=self.verify)

        template = json.loads(r.text)
        # copy details of the template into the payload
        # this may be simplified by re-using the template variable
        keys = ["id", "title", "builtin_name", "field_ids", "template_in_use"]
        payload = {}
        for k in keys:
            payload[k] = template[k]
        # create the combined list of existing and new field IDs
        new_fields = list(set(list(field_ids) + template['field_ids']))
        payload['fields'] = [str(x) for x in new_fields]
        url = self.host + "/ajax/custom_template/" + str(template_id) + "/"
        requests.put(url, json=payload, headers=self.headers, verify=self.verify)

    # The clear_template method deletes all custom fields from a template
    # The custom fields are untouched
    # template_id: numerical ID of the template
    # return value: none
    def clear_template(self, template_id):
        r = requests.get(self.host + "/ajax/custom_template/" + str(template_id) + "/",
                         headers=self.headers, verify=self.verify)

        template = json.loads(r.text)

        keys = ["id", "title", "builtin_name", "field_ids", "template_in_use"]
        payload = {}
        for k in keys:
            payload[k] = template[k]

        payload['fields'] = []
        url = self.host + "/ajax/custom_template/" + str(template_id) + "/"
        requests.put(url, data=json.dumps(payload), headers=self.headers, verify=self.verify)

    # The create_customfield method creates a new custom field based on the parameters provided:
    # name_singular: the name in singular, for example 'Data Owner'
    # name_plural: for example 'Data Owners'
    # options: the values for a picker, e.g. 'yes', 'no'
    # tooltip: a small text displayed to the end user
    # picker_type: an allowed field type
    # o_type: a list of object types to include in object sets, e.g. article, table
    # return value: the new field's ID
    def create_customfield(self, name_singular, name_plural, options, tooltip, picker_type, o_type=[]):

        if options:
            options_str = json.dumps(
                [{"title": option, "tooltip_text": None, "old_index": None, "article_id": None} for option in options])
        else:
            options_str = None

        payload = {
            "allowed_otypes": o_type,
            "field_otype": "custom_field",
            "field_type": picker_type,
            "name": None,
            "name_plural": name_plural,
            "name_singular": name_singular,
            "options": options_str,
            "tooltip_text": tooltip,
            "value": None,
        }

        log_me(f"Creating a new custom field {name_singular}")

        field = self.generic_api_post("/ajax/custom_field/", body=payload)
        return field['id']

    # The put_custom_fields method takes a DataFrame with custom fields and creates them one by one,
    # returning a list of all the field IDs, existing or newly created
    # In many cases the source of the custom fields DataFrame would be a call to the Template API
    # Otherwise you would use the method above: create_customfield
    def put_custom_fields(self, custom_fields_pd):  # takes a DataFrame obtained from the Template API
        # Go through the custom fields DataFrame row by row. Each row is a dict-like object
        def process_line(custom_f):
            #log_me(f'Processing {custom_f}')
            # Since AlationInstance keeps track of existing custom fields, we can easily check if this field
            # already exists. We use the name_singular field and check for a match.
            # The resulting DataFrame can have 0 matches, 1, or multiple
            match = self.existing_fields.name_singular.eq(custom_f['name_singular'])
            field_exists = self.existing_fields.loc[match]
            # should return only one row if the field exists already
            # no match at all? Let's create the field
            if not match.any():
                log_me("Putting custom field {}".format(custom_f['name_singular']))
                if custom_f['options']:
                # have also changed the JSON handling
                    custom_f['options'] = json.dumps([{"title": option, "tooltip_text": None, "old_index": None, "article_id": None}\
                                           for option in custom_f['options']])
                custom_f['field_otype']='custom_field'
                custom_f['value']=None
                custom_f['name']=None

                body = dict(custom_f)

                field = self.generic_api_post(api="/ajax/custom_field/", body=body)
                # so we don't get a duplicate next time (could be more efficient)
                # the lazy way: just download all fields again
                self.existing_fields = self.get_custom_fields()
                return field['id']
            elif len(field_exists)==1:
                name = field_exists.at[match.idxmax(), 'name']
                id = field_exists.at[match.idxmax(), 'id']

                log_me(f"{id}/{name} already exists (info only)")
                return id  # ID of the field
            else:
                log_me("WARNING -- MULTIPLE FIELDS WITH THE SAME NAME")
                log_me(field_exists.loc[:,['id', 'name']])
                return field_exists.iloc[0, 6] # ID of the first field (hopefully only one anyway)

        return custom_fields_pd.apply(process_line, axis=1)



    # The put_custom_template method creates a new custom template and adds fields (optional)
    # template: name of the new template
    # fields: a list of field IDs
    # returns the ID of the newly created template
    def put_custom_template(self, template, fields=[]):
        match = self.existing_templates.title.eq(template)
        if match.any():
            return self.existing_templates.id[match.idxmax()]
        else:
            payload = dict(fields=[])
            payload['title'] = template
            log_me(f"Putting template {template}")
            t = self.generic_api_post("/ajax/custom_template/", body=payload)
            self.existing_templates = self.get_templates()  # so we don't get a duplicate next time (could be more efficient)
            if len(fields)>0:
                self.add_customfields_to_template(t['id'], fields)
            return t['id']

    # The put_articles method prepares the Articles and uploads via Bulk API
    # article: an instance of the Article class (essentially a pandas DataFrame with all articles
    # template_name: this template will be applied to the articles
    # a list of custom field IDs used in the template
    # NOTE: this method calls a method of the Article class, creating a dependency on that class
    # returns a response object (see requests module)
    def put_articles(self, article, template_name, custom_fields, bulk="/api/v1/bulk_metadata/custom_fields/"):
        #log_me("Putting Articles on Instance")
        # Template name needs to be part of the URL
        template_name = template_name.replace(" ", "%20")
        url = self.host + bulk + template_name + "/article"
        # if custom_fields.empty:
        #     body = article.bulk_api_body()
        # else:
        # Body needs to be a text with one JSON per line
        # Note we are (no longer)sending all custom fields to the function
        custom_fields_pd = self.existing_fields.loc[custom_fields, :]
        body = article.bulk_api_body()
        params = dict(replace_values=True, create_new=True)
        r = requests.post(url, data=body, headers=self.headers, params=params, verify=self.verify)
        if not r.status_code:
            log_me(f'Error uploading articles: {r.content}')
        return r

    # The put_articles_2 method is a simplified version of the method above
    # article: expects a ready made JSON for the body (one JSON per line=article)
    # template_name: this template will be applied to the articles
    # returns a response object (see requests module)
    def put_articles_2(self, article, template_name):
        #log_me("Putting Articles on Instance")
        # Template name needs to be part of the URL
        template_name = template_name.replace(" ", "%20")
        url = self.host + "/api/v1/bulk_metadata/custom_fields/" + template_name + "/article"
        params = dict(replace_values=True, create_new=True)
        try:
            r = requests.post(url, data=article, headers=self.headers, params=params, verify=self.verify)
            if not r.status_code:
                raise Exception(r.text)
            return r
        except IOError as e:
            log_me("I/O error({0}): {1}".format(e.errno, e.strerror))
        except Exception as e:
            log_me(f"Unexpected {e}")

    # The get_custom_fields_from_template method extracts the custom fields out of the template API output
    # desired_template: name of an existing template
    # returns a pandas DataFrame with the custom fields, sorted and indexed by ID
    def get_custom_fields_from_template(self, desired_template):
        dt = self.existing_templates[self.existing_templates.title == desired_template]
        # should return a DataFrame with only one row
        custom_fields = pd.DataFrame(dt.iloc[0, 1])  # only look at first row and second column
        if custom_fields.shape[0] == 0:
            return custom_fields
        custom_fields.index = custom_fields.id
        custom_fields.options = custom_fields.options.apply(lambda x: ([y['title'] for y in x]) if x else None)
        custom_fields['template_id'] = dt.iloc[0, 2]
        return custom_fields.sort_index()

    # The getQueries method downloads all published queries from the instance
    # returns a pandas DataFrame with the queries, sorted and indexed by ID
    def get_queries(self):
        log_me("Getting queries")
        params = dict(limit=1000, saved=True, published=True, deleted=False)
        queries = pd.DataFrame(self.generic_api_get("/api/query/", params=params))
        queries = queries[queries.deleted==False]
        log_me("Total queries found: {}".format(queries.shape[0]))
        if 'id' in queries:
            queries = queries.loc[:, ['id', 'title', 'description', 'published_content', 'ds', 'author']]
            queries.index = queries.id
            return queries.sort_index()
        else:
            return queries

    # The put_single_query method uploads a single query passed as a dictionary
    # The keys of the dictionary should be: 'title', 'description', 'published_content', 'ds_id', 'author'
    # Returns the query as a JSON
    def put_single_query(self, single_query):
        url = self.host + "/api/query/"
        r = requests.post(url, headers=self.headers, verify=self.verify, json=single_query)
        if not r.status_code:
            log_me("Issue with a query upload: {}".format(r.content))
        else:
            return r.text

    # The putQueries method uploads a DataFrame with queries to Alation
    # The DataFrame will have at least the following columns:
    # 'title', 'description', 'published_content', 'ds_id', 'author'
    # If a query with the same title already exists, we don't update (updating remains a TO-DO)
    # If an error occurs, we print one error message and ignore the rest of the queries
    # Data source IDs 1 and 10 have a special meaning for Alation Analytics and HR-VDS, related to ABOK
    # No return value.
    def put_queries(self, queries):
        ex_queries = self.get_queries()
        url = self.host + "/api/query/"

        datasource_with_errors = {}

        log_me("----- Working on Query Uploads -----")
        n = 0
        aa = self.look_up_ds_by_name("Alation Analytics")
        hr = self.look_up_ds_by_name("HR-VDS")

        for single_query in queries.itertuples():
            #log_me(single_query.title)
            ori_ds_id = single_query.ds['id']
            ori_ds_title = single_query.ds['title']

            if "title" in ex_queries:
                match = single_query.title == ex_queries.title
                if match.any():
                    # Query already exists. Let's not duplicate it.
                    query_id = ex_queries.id[match.idxmax()]
                    #log_me("Not updating existing query {}".format(query_id))
                else:
                    body = {}
                    body['content'] = single_query.published_content
                    body['published_content'] = single_query.published_content
                    if ori_ds_id==2 and aa: # Alation Analytics!
                        body['ds_id'] = int(aa)
                    elif ori_ds_id==10 and hr: # HR Database
                        body['ds_id'] = int(hr)
                    else:
                        if ori_ds_id in datasource_with_errors:
                            pass
                        else:
                            log_me("Issue with query...{}".format(single_query.title))
                            log_me("No datasource associated with that query!")
                            datasource_with_errors[ori_ds_id] = ori_ds_title
                        continue
                    body['title'] = single_query.title
                    if not single_query.description:
                        body['description'] = " ... "
                        log_me("Please add a description to query {}".format(single_query.title))
                    else:
                        body['description'] = self.fix_refs_2(single_query.description,
                                                               ori_title=single_query.title,
                                                               queries=queries)
                    body['published'] = True
                    r = requests.post(url, headers=self.headers, verify=self.verify, json=body)
                    if not r.status_code:
                        log_me("Issue with a query upload: {}".format(r.content))
                    else:
                        n = n + 1
        log_me("Uploaded ({}) queries.".format(n))

    # The getUsers method downloads all Alation users as a DataFrame
    def getUsers(self):
        log_me("Getting users")
        url = self.host + "/api/user/"
        r = requests.get(url, headers=self.headers, verify=self.verify)
        users = pd.DataFrame(r.json())
        return users

    # The getDataSources method downloads all data sources (metadata only) as a DataFrame
    def getDataSources(self):
        ds = pd.DataFrame(self.generic_api_get("/integration/v1/datasource/", official=True))
        log_me("Total number of data sources: {}".format(ds.shape[0]))
        return ds

    # The look_up_ds_by_name method returns the data source ID of a data source by title
    # or None if not found
    def look_up_ds_by_name(self, ds_title):
        # look up id of the data source by title
        match = self.ds.title.eq(ds_title)
        if match.any():
            ds_id = self.ds.id[match.idxmax()]
            return ds_id
        else:
            log_me("WARNING - No Data Source found for name {}".format(ds_title))
            return

    # mkdir_p creates a directory on the local disk of the Alation server
    def mkdir_p(self, path):
        try:
            os.makedirs(path)
        except OSError as exc:  # Python >2.5
            if exc.errno == errno.EEXIST and os.path.isdir(path):
                pass
            else:
                raise

    # The get_media_file method downloads all media files into a local file called 'ABOK_media_files.zip'
    # unless the file already exists in the same zip file
    # media_set: a list of URLs with media files
    # basepath: a directory where to expect the zip file
    # returns None
    def get_media_file(self, media_set, basepath):
        # Get a list of files already contained in the local ZIP file
        existing_files = list(list_files(basepath))
        for article_file_list in media_set:
            for article_id, filename in article_file_list:
                try:
                    #log_me(filename)
                    url = urllib.request.urlopen(filename)
                    filename = urllib.parse.urlparse(filename).path
                except ValueError:  # invalid URL - that means it's only a path
                    url = self.host + filename
                except:
                #    # sometimes we get Error 403 (unauth.), no problem
                    url = filename
                    filename = urllib.parse.urlparse(filename).path
                if filename in existing_files:
                    # log_me("Good news - no need to download {}".format(filename))
                    pass
                else:
                    log_me("Downloading and saving {} -> {}".format(article_id, url))
                    r = requests.get(url, headers=self.headers, verify=self.verify)
                    if r.status_code == 200:
                        with zipfile.ZipFile('ABOK_media_files.zip', 'a') as myzip:
                            myzip.writestr(filename, r.content)
                        existing_files.append(filename)
                    else:
                        log_me("WARNING -- NO FILE {}".format(url))

    # The fix_children method should be called after posting articles to an instance,
    # if parent-child relationships exist in the original Article DataFrame
    # source_articles: a DataFrame with articles on the source (for example, ABOK)
    # template: to speed up, can restrict articles fixed to one template (recommended)
    # Returns None
    def fix_children(self, source_articles, template='all'):
        log_me("---- Pass 3: Fixing parent-child relationships ----")
        # Let's read all articles again so we get the IDs, too.
        articles_on_target = self.get_articles(template=template)
        # iterate through all articles with children
        art_with_children = source_articles[source_articles.has_children].sort_index()
        # Let's touch each parent only once
        for a in art_with_children.itertuples():
            # a is a parent
            t = a.title
            id = a.id
            children = a.children # remember these are source IDs
            # see if the article exists on the target and what its ID is
            target_parent = articles_on_target[articles_on_target.title == t]
            new_children = []
            if target_parent.empty:
                log_me("Skipping to next article in the list - make sure to replicate articles first")
            else:
                target_parent_id = int(target_parent['id'])
                target_parent_children = target_parent.at[target_parent_id, 'children']
                if len(target_parent_children) >= 1:
                    target_parent_children_ids = [c['id'] for c in target_parent_children]
                else:
                    target_parent_children_ids = []
                for child in children:
                    try:
                        child_id = child['id']
                        child_title = child['title']

                        match = articles_on_target.title.eq(child_title)

                        # On target machine
                        target_child = articles_on_target[match]
                        if target_child.empty:
                            log_me("No target child: {} -> {}".format(t, child_title))
                        else:
                            target_child_id = target_child.iloc[0, :]['id']
                            if target_child_id not in target_parent_children_ids:
                                new_children.append(target_child_id)
                    except:
                        log_me("Child issue: {} -> {}".format(target_parent_id, child_title))
                if len(new_children)>0:
                    log_me("Parent: {}/{}, Children: {}".format(id, t, [c['id'] for c in children]))
                    log_me("Updating article {}:{} -> {}".format(target_parent_id, t, new_children))
                    # update all children
                    new_article = dict(body=target_parent.loc[target_parent_id, 'body'], title=t, children=
                                       [dict(id=int(new_child), otype="article") for new_child in new_children]
                                       ) # only the required fields...
                    updated_art = self.update_article(int(target_parent_id), new_article)
        log_me("Finished updating parent-child relationships.")


    # The fix_refs method should be called before posting articles to an instance,
    # if references (at-mentions) exist in the original Article DataFrame
    # template: to speed up, can restrict articles fixed to one template (recommended)
    # Returns None
    def fix_refs(self, template):
        log_me("----- Pass 2: Getting all Articles, Queries -----")
        # Get a handle on all the articles on the source instance
        articles = self.get_articles(template=template)
        queries = self.get_queries()
        # Initialise them as not updated
        articles['updated'] = False
        # Go through all the articles which may contain references
        for a in articles.itertuples():
            # Store a few attributes of the current article
            t = a.title
            id = a.id
            # Store the HTML of the body
            soup = BeautifulSoup(a.body, "html5lib")
            update_needed = False
            # Find all Anchors = Hyperlinks
            match = soup.findAll('a')
            # Go through all the hyperlinks to update them
            for m in match:
                # We only care about Alation anchors, identified by the attr data-oid
                if 'data-oid' in m.attrs and 'data-otype' in m.attrs:
                    # Store title, oid, and otype of the current hyperlink
                    oid = m['data-oid']
                    otype = m['data-otype']
                    if 'title' in m.attrs:
                        title = m['title']
                    else:
                        title = m.get_text()
                        log_me("{} somehow got missed in the pre-processing".format(title))
                    # Process links to articles
                    if otype == 'article':
                        try:
                            art_match = articles.title.eq(title)
                            if art_match.any():
                                # m is a reference to somewhere and we need to fix it.
                                oid = articles.id[art_match.idxmax()]
                                m['data-oid'] = oid
                                m['href'] = f'/{otype}/{oid}/'

                                articles.at[id, 'body'] = soup.prettify()  # update the article body
                                articles.at[id, 'updated'] = True  # update the article body
                                update_needed = True
                                # log_me("Article match for {} -> {}/{}".format(t, title, oid))
                            else:
                                log_me(f'No article match for {t}->{title}')
                        except:
                            log_me(f'Exception trying to match {t}->{title}')
                    # Process links to queries
                    elif otype == 'query' and not queries.empty:
                        q_match = queries.title.eq(title)
                        if q_match.any():
                            matching_queries = queries[q_match]
                            # m is a reference to somewhere and we need to fix it.
                            oid = (matching_queries.iloc[-1]).id # -1 means last
                            m['data-oid'] = oid
                            m['href'] = "/{}/{}/".format(otype, oid)
                            articles.at[id, 'body'] = soup.prettify()  # update the article body
                            articles.at[id, 'updated'] = True  # update the article body
                            update_needed = True
                        else:
                            log_me("No query match for {} -> {}".format(t, title))
                    elif otype == 'table':
                        qual_name = title.split()[0]
                        tb = self.get_tables_by_name(qual_name)
                        if not tb.empty:
                            # m is a reference to somewhere and we need to fix it.
                            oid = tb.index[-1]
                            m['data-oid'] = oid
                            m['href'] = "/{}/{}/".format(otype, oid)
                            #log_me("Link to table: {}".format(m))

                            articles.at[id, 'body'] = soup.prettify()  # update the article body
                            articles.at[id, 'updated'] = True  # update the article body
                            update_needed = True
                        else:
                            log_me(f"No match for {title}")
            if update_needed:
                log_me("Updating article {}:{} -> {}".format(id, t, title))
                new_article = dict(body=articles.at[id, 'body'], title=t)  # only the required fields...
                updated_art = self.update_article(int(id), new_article)
        log_me("Finished preparing references. ")


    # The fix_refs_2 method finds at-mentions in a description and sets data-oid to appropriate value (if found)
    # and adds the title of the linked object
    # description: the string which gets searched for at-mentions
    # queries: queries that may be linked (can be empty, but not None)
    # ori_title: where does this reference come from? For example "Data Dictionary"
    # Returns a new HTML string which should be further processed
    def fix_refs_2(self, description, queries, ori_title):
        soup = BeautifulSoup(description, "html5lib")
        # Find all Anchors = Hyperlinks
        match = soup.findAll('a')
        # Go through all the hyperlinks to update them
        for m in match:
            # We only care about Alation anchors, identified by the attr data-oid
            if 'data-oid' in m.attrs and 'data-otype' in m.attrs:
                # Store title, oid, and otype of the current hyperlink
                otype = m['data-otype']
                if 'title' in m.attrs:
                    title = m['title']
                else:
                    title = m.get_text()
                # Process links to articles
                if otype == 'article':
                    try:
                        art_match = self.articles.title==title
                        if art_match.any():
                            # m is a reference to somewhere and we need to fix it.
                            oid = self.articles.id[art_match.idxmax()]
                            m['data-oid'] = oid
                            m['href'] = "/{}/{}/".format(otype, oid)
                        else:
                            log_me("No article match for {} -> {}".format(ori_title, title))
                            m['data-oid'] = 0
                            del m['href']
                    except:
                        log_me("Exception / no article match for {} -> {}".format(ori_title, title))
                # Process links to queries
                elif otype == 'query' and not queries.empty:
                    q_match = queries.title == title
                    if q_match.any():
                        matching_queries = queries[q_match]
                        # m is a reference to somewhere and we need to fix it.
                        oid = (matching_queries.iloc[-1]).id # -1 means last
                        m['data-oid'] = oid
                        m['href'] = "/{}/{}/".format(otype, oid)
                    else:
                        log_me("No query match for {} -> {}".format(ori_title, title))
                        m['data-oid'] = 0
                        del m['href']
                elif otype == 'table':
                    qual_name = title.split()[0]
                    tb = self.get_tables_by_name(qual_name)
                    if not tb.empty:
                        # m is a reference to somewhere and we need to fix it.
                        oid = tb.index[-1]
                        m['data-oid'] = oid
                        m['href'] = "/{}/{}/".format(otype, oid)
                        #log_me("Link to table: {}".format(m))
                    else:
                        #log_me("No table match for {} -> {}".format(ori_title, title))
                        m['data-oid'] = 0
                        del m['href']
        return soup.prettify()


    # The method upload_dd uploads a data dictionary, for example for Alation Analytics
    # dd: DataFrame with the data dictionary (same format as the CSV you download)
    # ds_id: id of the data source for which the dd is intended
    # title of the ds for which the dd is intended (suppy ID or title, not both)
    # articles_created: a DataFrame of articles which are possibly referenced in the data dictionary
    # so that any links in the table descriptions can be fixed to point to the correct article
    # returns the response object
    def upload_dd(self, dd, ds_id=0, ds_title="", articles_created=[]):
        body = ""
        self.articles=pd.DataFrame(articles_created)
        if not ds_id:
            ds_id = self.look_up_ds_by_name(ds_title)
        log_me("Data Dictionary Upload for {}/{}".format(ds_id, ds_title))

        if ds_title == "Alation Analytics":
            log_me("----- Working on DD description fields -----")
            queries = self.get_queries()
            dd['description'] = dd.description.apply(self.fix_refs_2, queries=queries, ori_title="DD")

        for id, obj in dd.iterrows():
            key_from_source = obj['key']
            keys_without_ds = key_from_source.split('.')[1:]
            key_2 = ".".join(keys_without_ds)
            obj['key'] = "{}.{}".format(int(ds_id), key_2)
            r = dict(obj)
            body = body + json.dumps(r) + '\n'
        log_me("Uploading data dictionary")
        # Template name needs to be part of the URL
        bulk = "/api/v1/bulk_metadata/custom_fields/"
        template_name = 'default'
        url = self.host + bulk + template_name + "/mixed"
        params=dict(replace_values=True, create_new=True)
        try:
            r = requests.post(url, data=body, headers=self.headers, params=params, verify=self.verify)
            if not r.status_code:
                raise Exception(r.text)
            return r
        except IOError as e:
            log_me("I/O error({0}): {1}".format(e.errno, e.strerror))

    # This method gets all API resources in a DataFrame
    # The information is limited, so this is only a helper function for
    # entire_api
    def get_api_resource_all(self):
        api = "/integration/v1/api_resource/"
        fields = pd.DataFrame(self.generic_api_get(api=api))
        fields.index = fields.id
        return fields.sort_index()

    # This method untangles the dictionary with the resource fields
    # The result is a flattened list of dictionaries, which can be
    # used to create a DataFrame
    def recursive_field(self, list_so_far, field):
        next_element = field.pop() # this reduces the input
        if "children" in next_element:
            for c in next_element['children']:
                field.append(c)
            del next_element['children']
            list_so_far.append(next_element)
        else:
            list_so_far.append(next_element)
        if len(field)==0:
            return list_so_far # finished
        return self.recursive_field(list_so_far, field)



    # This recursive function is designed to encode an existing dictionary coming from get_api_resource
    # and convert it into the format that post_api_resource expects
    def recursive_field_encode(self, field):
        if "children" in field:
            if field['type']=='object':
                # add a key for each child
                # the key is key
                d = dict(type='object', properties=dict())
                for c in field['children']:
                    g = self.recursive_field_encode(field=c)    # <--- recursive call!
                    k = g.keys()
                    v = g.values()
                    d["properties"][g.keys()[0]] = g.values()[0] # <---- aggregating the children into one dict
                return {field['key']: d} # <---- this is likely to be the final return
            elif field['type']=='array':
                d = dict(type='array', items=dict())
                d['items'] = dict(properties=dict())
                # in the case of an array, skip the Array Item itself
                for c in field['children'][0]['children']:
                    p = self.recursive_field_encode(field=c)
                    k = p.keys()
                    v = p.values()
                    d["items"]["properties"][k[0]] = v[0]
                return {field['key']: d}
            else:
                log_me("Unexpected field type {}".format(field))
        else: # this branch returns a leaf outside
            e = dict()
            if field['examples']:
                e['type'] = field['type']
                e['examples'] = [field['examples']]
            else:
                e['type'] = field['type']
            return {field['key'] : e}


    # This method gets one specific API resource and all the details
    # Just not yet in the format needed to post it back to Alation
    def get_api_resource_1(self, id):
        api = "/integration/v1/api_resource/{}/".format(id)
        url = self.host + api
        try:
            r = requests.get(url, headers=self.headers, verify=self.verify)
            if not r.status_code:
                raise Exception(r.text)
            return r.json()
        except IOError as e:
            log_me("I/O error({0}): {1}".format(e.errno, e.strerror))
        except ValueError:
            log_me("Could not convert data to an integer.")
        except:
            log_me("Unexpected error: {}".format(sys.exc_info()[0]))
            raise
    # This method gets a DataFrame with the Resource Fields only for one specific API resource
    def get_api_resource_2(self, id):
        api = "/integration/v1/api_resource/{}/".format(id)
        url = self.host + api
        api_fields = pd.DataFrame()
        # params=dict(replace_values = True, create_new = True)
        try:
            r = requests.get(url, headers=self.headers, verify=self.verify)
            if not r.status_code:
                raise Exception(r.text)
            a = r.json()
            if a['input_schema']:
                input_schema = a['input_schema']
                in_fields = self.recursive_field([], deque(input_schema))
                in_fields = pd.DataFrame(in_fields)
                in_fields['source'] = 'input'
                api_fields = pd.DataFrame(in_fields)
            if a['output_schema']:
                output_schema = a['output_schema']
                out_fields = self.recursive_field([], deque(output_schema))
                out_fields = pd.DataFrame(out_fields)
                out_fields['source'] = 'output'
                api_fields = api_fields.append(out_fields)

            api_fields.index = api_fields.id
            return api_fields.sort_index()
        except IOError as e:
            log_me("I/O error({0}): {1}".format(e.errno, e.strerror))
        except:
            log_me("Unexpected error")
            raise
    # a utility function to create a list of items and fix list in list
    def flatten(self, l, ltypes=(list, tuple)):
        ltype = type(l)
        l = list(l)
        i = 0
        while i < len(l):
            while isinstance(l[i], ltypes):
                if not l[i]:
                    l.pop(i)
                    i -= 1
                    break
                else:
                    l[i:i + 1] = l[i]
            i += 1
        return ltype(l)

    # A method to extract the logical metadata for use with the bulk API later
    def extract_logical(self, api_dict):
        if "properties" in api_dict:  # like children
            l = []
            for k, v in api_dict['properties'].items():
                v['key'] = k
                l.append(self.extract_logical(v))
            return l
        if "items" in api_dict:
            return self.extract_logical(api_dict['items'])
        else:  # at the end of the tree...
            return api_dict

    # a utility method to detect 'nan' which can appear in a pandas DataFrame when the field is empty
    def is_valid(self, t):
        if isinstance(t, str):
            return True
        elif isinstance(t, float):
            return not isnan(t)
        else:
            print ("Type warning")
            return False

    # This method updates the logical metadata of all resources fields passed in a DataFrame
    def update_api_resource_field(self, resource_field, rtype="api_resource_field"):
        api = "/api/v1/bulk_metadata/custom_fields/default/" + rtype
        url = self.host + api
        body = str()
        params=dict(replace_values = True, create_new = False)
        for id, obj in resource_field.iterrows():
            r = dict(obj)
            t = {}
            if self.is_valid(r['title']):
                t['title'] = r['title']
            if self.is_valid(r['description']):
                t['description'] = r['description']
            t['key'] = str(r['id'])
            if len(t.keys()) > 1:
                body = body + json.dumps(t) + '\n'

        try:
            r = requests.post(url, headers=self.headers, verify=self.verify, data=body, params=params)
            if not r.status_code:
                raise Exception(r.text)
            return r.text
        except IOError as e:
            log_me("I/O error({0}): {1}".format(e.errno, e.strerror))
        except ValueError:
            log_me("Could not convert data to an integer.")
        except:
            log_me("Unexpected error: {}".format(sys.exc_info()[0]))
            raise

    # This method return a DataFrame with all the existing API Resources on the instance
    # Each field where isfolder=False can be used to construct a new API resource
    # But the input and output need to be re-ordered
    def entire_api(self):
        all_apis = self.get_api_resource_all()
        all_apis = all_apis[all_apis.is_folder.eq(False)]
        list_of_apis = []

        for api in all_apis.itertuples():
            api, f = self.get_api_resource_1(api.Index)
            list_of_apis.append(api)
        all_apis_detailed = pd.DataFrame(list_of_apis)
        all_apis_detailed.index =  all_apis_detailed.id
        return all_apis_detailed

    # This method creates a new api_resource
    # You have to pass a dictionary with the keys:
    # - request_type
    # - resource_url
    # - path
    # - request_type
    # - output_schema
    # - input_schema
    # It's been tested with existing API Resources (migrations) and "hard-coded"

    # The post_api_resource method takes an api_resource and posts it to Alation
    # The main value add is that it will also add titles and descriptions contained
    # in the api_resource, which is not the case in the "normal" API
    def post_api_resource(self, api_resource):
        # /integration/v1/api_resource/<HTTP_method>:<url>/
        req_type = api_resource['request_type']
        req_url = api_resource['resource_url']
        req_path = api_resource['path']
        url = self.host + "/integration/v1/api_resource/" + req_type + ":" + req_url + "/"

        payload = dict(path=req_path,
                       response_type=api_resource['request_type'],
                       output_schema=api_resource['output_schema'],
                       input_schema = api_resource['input_schema']
        )
        try:
            r = requests.post(url, json=payload, headers=self.headers, verify=self.verify)
            if not r.status_code:
                raise Exception(r.text)
            # -- output_schema logical metadata ---
            extracted_logical_data_raw = self.extract_logical(api_resource['output_schema'])
            extracted_logical_data = pd.DataFrame(self.flatten(extracted_logical_data_raw))
            resource_fields = self.get_api_resource_2(json.loads(r.text)['id'])
            resource_fields_map = resource_fields.loc[resource_fields.source.eq('output'), ['key', 'id']]
            merged_logical = extracted_logical_data.merge(resource_fields_map, on='key')
            return_text = self.update_api_resource_field(merged_logical)

            # -- input_schema logical metadata ---
            extracted_logical_data_raw = self.extract_logical(api_resource['input_schema'])
            extracted_logical_data = pd.DataFrame(self.flatten(extracted_logical_data_raw))
            resource_fields = self.get_api_resource_2(json.loads(r.text)['id'])
            resource_fields_map = resource_fields.loc[resource_fields.source.eq('input'), ['key', 'id']]
            merged_logical = extracted_logical_data.merge(resource_fields_map, on='key')
            return_text += self.update_api_resource_field(merged_logical)
            return return_text
        except IOError as e:
            log_me("I/O error({0}): {1}".format(e.errno, e.strerror))

    # The update_custom_field method lets you change the value of a single custom field
    # belonging to a single Alation object
    # o_type: Alation Object Type, e.g. "dataflow"
    # o_id: Alation ID, e.g. 1
    # field_id: each custom field has an ID, check self.custom_fields. Title=3, Description=4, etc.
    # update: for a text field, a string. For a reference, a dict
    def update_custom_field(self, o_type, o_id,field_id, update):
        api = f"/api/field/object/{o_type}/{o_id}/{field_id}/commit/"
        body=dict(op='replace', value=update)
        self.generic_api_post(api, body=body)

    # The get_dataflows method downloads all existing DataFlows, assuming the first one has ID 1
    # and there are only 1000.
    # Note this is pretty crude approach, taking about 1/2 second per DataFlow
    # Result is a DataFrame with the following columns:
    # ['id', 'content', 'creation_type', 'external_id', 'data_input', 'data_output', 'fp', 'otype', 'url']
    # def get_dataflows(self):
    #     res = []
    #     log_me("Getting DataFlows")
    #     for i in range(1, 1000):
    #         d = self.generic_api_get(api=f"/api/dataflow/{i}/")
    #         if not 'id' in d:
    #             break
    #         if i == 1:
    #             log_me(f"Available fields: {d.keys()}")
    #         res.append(d)
    #     res_pd = pd.DataFrame(res)
    #     res_pd.index = res_pd.id
    #     log_me(f"Downloaded {i-1} DataFlows")
    #
    #     return res_pd
    # The generic_api_post method posts a request to Alation and if necessary checks the status
    def generic_api_post(self, api, params=None, body=None, data=None, official=False):
        if official:
            headers_final = dict(token=self.token)
        else:
            headers_final = self.headers
            headers_final['Referer'] = self.host + api

        if body:
            r = requests.post(self.host + api, json=body, params=params, headers=headers_final, verify=self.verify)
        elif data:
            r = requests.post(self.host + api, data=data, params=params, headers=headers_final, verify=self.verify)

        if r.status_code:
            r_parsed = r.json()
            # do we need to ask the job status API for help?
            if 'job_id' in r_parsed:
                params = dict(id=r_parsed['job_id'])
                url_job = "/api/v1/bulk_metadata/job/"
                # Let's wait for the job to finish
                while (True):
                    status = self.generic_api_get(api=url_job, params=params, official=True)
                    try:
                        if status['status'] != 'running':
                            objects = status['result']
                            break
                        else:
                            print(f"Job still running: {status}")
                    except:
                        log_me(f"No good status: {r.text}")
                        break
                r_parsed = status
            return r_parsed
        else:
            return r.text

    # The generic_api_post method posts a request to Alation and if necessary checks the status
    def generic_api_put(self, api, params=None, body=None, official=False):
        if official:
            headers_final = dict(token=self.token)
        else:
            headers_final = self.headers
            headers_final['Referer'] = self.host + api

        r = requests.put(self.host + api, json=body, params=params, headers=headers_final, verify=self.verify)
        return r.content

    # The generic_api_get implements a REST get, with API token if official or Cookie if not.
    # If the callers sends header, it needs to contain API or cookie
    def generic_api_get(self, api, headers=None, params=None, official=False):
        if headers:
            # caller has supplied the headers
            headers_final = headers
        else:
            if official:
                headers_final = dict(token=self.token)
            else:
                headers_final = self.headers
                headers_final['Referer'] = self.host + api
        r = requests.get(self.host + api, headers=headers_final, params=params, verify=self.verify)
        if r.status_code in [200, 201]:
            try:
                return r.json()
            except:
                return r.text # for LogicalMetadata API which does not use standard JSON
        else:
            return r.text


    def add_path_to_folders(self, my_folders, folders):
        for i, f in my_folders.iterrows():
            parent_folder = folders.at[i, "parent_folder"]
            if parent_folder:
                parent_folder_path = folders.loc[folders.external_id == parent_folder, 'path'].iloc[0]
                folders.loc[i, 'path'] += f"{parent_folder_path}//{f['name']}"
            else:
                folders.loc[i, 'path'] += f"{f['name']}"
        # now, children
        children = folders.loc[folders.parent_folder.isin(my_folders.external_id)]
        if children.shape[0]:
            self.add_path_to_folders(children, folders)
        return True

    def get_bi_folders(self, bi_server_id):
        my_folders = []
        next = f"/integration/v2/bi/server/{bi_server_id}/folder/"
        while(next):
            log_me(f"Downloading all BI folders from {next}")
            r = requests.get(self.host + next, headers=dict(token=self.token), params={}, verify=self.verify)
            next = r.headers.get("x-next-page")
            j = r.json()
            my_folders.extend(j)
            log_me(f"Downloaded {len(r.json())} items so far.")
        if my_folders:
            folders = pd.DataFrame(my_folders)
            folders['otype'] = "bi_folder"
            folders['path'] = ""
            self.api_columns.extend(list(folders.columns))
            # -- work on the path --
            set_path = self.add_path_to_folders(folders.loc[folders.parent_folder.isna()], folders)
            # cache the results for later use
            self.bi_folders = folders.set_index(['otype', 'id'], drop=True)
            return self.bi_folders

    def get_dataflows(self):
        my_dataflows = []
        batch = 1000
        offset = 0
        while(True):
            my_search = dict(limit=batch,
                             offset=offset,
                             filters=json.dumps(dict(otypes=['dataflow'])))
            r = requests.get(self.host + "/search/v1/", headers=dict(token=self.token), params=my_search, verify=self.verify)
            j = r.json()
            total = j.get('total')
            my_dataflows.extend(j.get('results'))
            log_me(f"Downloaded {batch}/{total} items.")
            if total<offset:
                break
            else:
                offset += batch
        if my_dataflows:
            dataflows = pd.DataFrame(my_dataflows)
            def get_dataflow_details(id):
                r = requests.get(self.host + "/integration/v2/lineage/", headers=dict(token=self.token),
                                 params=dict(otype="dataflow", oid=id, keyField="id"),
                                 verify=self.verify)
                j = r.json()
                return j.get('paths')
            dataflows['paths'] = dataflows.id.apply(get_dataflow_details)
            return dataflows


    def get_bi_reports(self, bi_server_id):
        my_reports = []
        next = f"/integration/v2/bi/server/{bi_server_id}/report/"
        while(next):
            log_me(f"Downloading {next}")
            r = requests.get(self.host + next, headers=dict(token=self.token), params={}, verify=self.verify)
            next = r.headers.get("x-next-page")
            my_reports.extend(r.json())
            log_me(f"Downloaded {len(r.json())} items so far.")
        if my_reports:
            reports = pd.DataFrame(my_reports)
            reports['otype'] = "bi_report"
            self.api_columns.extend(list(reports.columns))
            return reports.set_index(['otype', 'id'], drop=True)

    def get_bi_report_cols(self, bi_server_id):
        my_cols = []
        next = f"/integration/v2/bi/server/{bi_server_id}/report/column/"
        while(next):
            log_me(f"Downloading {next}")
            r = requests.get(self.host + next, headers=dict(token=self.token), params={}, verify=self.verify)
            next = r.headers.get("x-next-page")
            my_cols.extend(r.json())
            log_me(f"Downloaded {len(r.json())} items so far.")
        if my_cols:
            report_cols = pd.DataFrame(my_cols)
            report_cols['otype'] = "bi_report_column"
            self.api_columns.extend(list(report_cols.columns))
            return report_cols.set_index(['otype', 'id'], drop=True)

    def get_custom_field_values_for_oids(self, okeys):
        my_fields = []
        api = f"/integration/v2/custom_field_value/"
        for key in okeys:
            otype = key[0]
            oid = key[1]
            params = dict(otype=otype, oid=oid)
            my_field = {}
            r = requests.get(self.host + api, headers=dict(token=self.token), params=params, verify=self.verify)
            fields = r.json()
            if len(fields)==0:
                continue
            for f in fields:
                field_details = self.get_field(f['field_id'])
                if field_details.field_type=="OBJECT_SET":
                    for v in f['value']:
                        key = f"{field_details.name_singular.lower()}:{v['otype']}"
                        fully_qualified = self.get_fully_qualified_name(v['otype'], v['oid'])
                        if key in my_field:
                            my_field[key] += f";{fully_qualified}"
                        else:
                            my_field[key] = f"{fully_qualified}"
                else:
                    my_field[field_details.name_singular.lower()] = f['value']
            my_field['id'] = oid
            my_field['otype'] = otype
            log_me(my_field)
            my_fields.append(my_field)
        custom_field_values = pd.DataFrame(my_fields)
        if custom_field_values.empty:
            custom_field_values = pd.DataFrame(columns=['otype', 'id'])
        return custom_field_values.set_index(['otype', 'id'])

    def get_field(self, field_id):
        return self.existing_fields.loc[field_id]

    def validate_headers(self, headers):
        validated_fields = {}
        self.existing_fields["name_lower"] = self.existing_fields.name_singular.apply(str.lower)
        for my_header in set(headers) - set(self.api_columns):
            if ":" in my_header:
                components = my_header.split(":")
                field_name = components[0].lower()
                field_type = components[1]
            else:
                field_name = my_header
                field_type = None
            my_field = self.existing_fields.loc[self.existing_fields.name_lower==field_name]
            if my_field.empty:
                # log_me(f"Cannot find a field with name {field_name}")
                continue
            my_field_details = my_field.iloc[0]
            validated_fields[my_header] = my_field_details
        return validated_fields

    def upload_lms(self, validated_df, fields, server_id=None):
        my_payload = []
        for _, my_object in validated_df.iterrows():

            if pd.isnull(my_object.id):
                my_id = str(self.convert_external_id(my_object.otype,
                                                 my_object.external_id,
                                                 server_id))
                if not my_id:
                    log_me(f"Skipping {my_object.external_id}")
                    continue
            else:
                my_id = str(my_object.id)
            try:
                log_me(f"Tagging {self.host}/bi/v2/{my_object.otype[3:]}/{to_int(my_id)} with external ID: {my_object.external_id}")
                tag = self.tag_an_object(my_object.otype, my_id, my_object.external_id)
            except:
                pass
            pre_existing_values = defaultdict(list)
            for k, v in fields.items():
                if pd.isnull(my_object[k]):
                    continue
                if v['field_type']=="MULTI_PICKER":
                    value = []
                    for my_option in v["options"]:
                        if my_option in my_object[k]:
                            value.append(my_option)
                elif v['field_type']=="OBJECT_SET":
                    value = []
                    type = k.split(":")[1]
                    if type in v['allowed_otypes']:
                        pre_value = str(my_object[k])
                        log_me(f"{type}: {pre_value}")
                        for item in pre_value.split(";"):
                            my_obj_id = self.reverse_qualified_name(type, item)
                            if my_obj_id:
                                value.append(dict(otype=type,
                                                  oid=str(to_int(my_obj_id))))
                            else:
                                log_me(f"Could not append item {item}")
                        pre_existing_values[v['id']].extend(value)
                        value = None
                else:
                    value = my_object[k]


                # Only do non-object-sets right now
                if value:
                    payload = dict(
                        field_id = int(v['id']),
                        ts_updated = datetime.now(timezone.utc).isoformat(),
                        otype = my_object.otype,
                        oid = to_int(my_id),
                        # oid = str(my_object.id),
                        value = value
                    )
                    # one by one for now
                    api = "/integration/v2/custom_field_value/"
                    try:
                        r = requests.put(self.host + api, headers=dict(token=self.token), json=[payload], verify=self.verify)
                        j = r.json()
                        updated = j.get("updated_field_values")
                        new_field_values = j.get("new_field_values")
                        errors = j.get("errors")
                        if updated:
                            log_me(f"Updated {k}={my_object[k]}")
                        elif new_field_values:
                            log_me(f"New value {k}={my_object[k]}")
                        if errors:
                            log_me(f"---ERROR--- {k}={my_object[k]} -- {errors}")
                    except Exception as e:
                        log_me(f"Exception for {k}: {e}")

            for field_id, value in pre_existing_values.items():
                field_details = self.get_field(field_id)
                payload = dict(
                    field_id=int(field_id),
                    ts_updated=datetime.now(timezone.utc).isoformat(),
                    otype=my_object.otype,
                    oid=to_int(my_id),
                    value=value
                )
                # one by one for now
                api = "/integration/v2/custom_field_value/"
                # log_me(f"Working on: {k}")
                try:
                    r = requests.put(self.host + api, headers=dict(token=self.token), json=[payload], verify=self.verify)
                    j = r.json()
                    updated = j.get("updated_field_values")
                    new_field_values = j.get("new_field_values")
                    errors = j.get("errors")
                    if updated:
                        log_me(f"Updated {field_details.name_singular}={value}")
                    elif new_field_values:
                        log_me(f"New value {field_details.name_singular}={value}")
                    if errors:
                        log_me(f"---ERROR--- {field_details.name_singular}={value}")
                        log_me(f"{errors}")
                except Exception as e:
                    log_me(f"Exception for {field_id}/{value}: {e}")

    def create_bi_server(self, uri, title):
        r = requests.post(self.host + "/integration/v2/bi/server/",
                          headers=dict(token=self.token),
                          json=[dict(uri=uri, title=title)],
                          verify=self.verify)
        return r.json()

    def validate_folder(self, external_id):
        try:
            if external_id in list(self.bi_folders.external_id):
                # log_me(f"Since {external_id} is cached, it exists!")
                return external_id
            else:
                if pd.isna(external_id):
                    pass
                else:
                    log_me(f"{external_id} is not a known folder.")
                return external_id
        except:
            log_me(f"{external_id} is not a known folder.")
            return external_id

    def valid_str(self, my_str):
        if pd.isnull(my_str) or pd.isna(my_str):
            return "null"
        elif my_str is None:
            return "none"
        elif isinstance(my_str, str):
            return my_str
        else:
            return ""

    def valid_external_id(self, my_str):
        if pd.isnull(my_str) or pd.isna(my_str):
            return str(uuid.uuid4())
        elif my_str is None:
            return str(uuid.uuid4())
        elif isinstance(my_str, str) and str!='':
            return my_str
        else:
            return str(uuid.uuid4())

    def valid_float(self, my_float):
        if pd.isnull(my_float) or pd.isna(my_float):
            return 0.0
        elif my_float is None:
            return 0.0
        elif isinstance(my_float, float):
            return my_float
        else:
            return 0.0

    def valid_reports(self, parents):
        validated_reports = []
        try:
            parents = json.loads(parents.replace("'", '"'))
            for external_id in set(parents).intersection(self.bi_reports.external_id):
                log_me(f"Since {external_id} is cached, assuming it exists!")
                validated_reports.append(external_id)
        except:
            parents = []
        return  validated_reports

    def valid_report_type(self, type):
        try:
            type_lower = type.lower()
            if type_lower == "dashboard":
                return "dashboard"
            else:
                return "simple"
        except:
            return "simple"

    def tag_an_object(self, otype, oid, tag):
        api=f"/integration/tag/{tag}/subject/"

        r = requests.post(self.host + api,
                          headers=dict(token=self.token),
                          json=dict(otype=otype, oid=oid),
                          verify=self.verify)
        if r.status_code:
            return tag

    def sync_bi(self, bi_server_id, df):
        self.bi_server_id = bi_server_id
        self.bi_folders = None
        api_cols = {
            "bi_folder": {"name" : self.valid_str,
                          "external_id" : self.valid_external_id,
                          "created_at" : self.valid_str,
                          "last_updated" : self.valid_str,
                          "source_url" : self.valid_str,
                          "bi_object_type" : self.valid_str,
                          "description_at_source": self.valid_str,
                          "owner": self.valid_str,
                          "num_reports" : to_int,
                          "num_report_accesses" : to_int,
                          "parent_folder": self.validate_folder},
            "bi_report": {"name" : self.valid_str,
                          "external_id" : self.valid_str,
                          "created_at" : self.valid_str,
                          "last_updated" : self.valid_str,
                          "source_url" : self.valid_str,
                          "bi_object_type" : self.valid_str,
                          "description_at_source": self.valid_str,
                          "report_type": self.valid_report_type,
                          "owner": self.valid_str,
                          "num_accesses" : to_int,
                          "popularity" : self.valid_float,
                          "parent_folder": self.valid_str,
                          "parent_reports": self.valid_reports,
                          },
            # "bi_report_column": {"name" : self.valid_str,
            #               "external_id" : self.valid_str,
            #               "created_at" : self.valid_str,
            #               "last_updated" : self.valid_str,
            #               "source_url" : self.valid_str,
            #               "bi_object_type" : self.valid_str,
            #               "description_at_source": self.valid_str,
            #               "data_type": self.valid_str,
            #               "role": self.valid_str,
            #               "expression": self.valid_str,
            #               "report": self.valid_str,
            #               # "values": list
            #               }
        }
        ts_updated = datetime.now(timezone.utc).isoformat()
        df["created_at"] = df.created_at.fillna(ts_updated)
        df["last_updated"] = df.last_updated.fillna(ts_updated)

        # do the folders first
        otype = "bi_folder"
        if self.bi_folders is None:
            self.get_bi_folders(bi_server_id)

        if self.bi_folders is None or self.bi_folders.empty:
            done = []
        else:
            done = list(self.bi_folders.external_id)

        payload_cols = api_cols[otype]
        payload = df.loc[df.otype == otype, payload_cols].sort_values("created_at")
        for col, op in payload_cols.items():
            payload[col] = payload[col].apply(op)
        still_to_do = list(payload.external_id)

        def recursive_folder_work(folders):
            """
            Creates and validates creation of folders, starting with top level
            :param folders: folders to be created right now (they should either not have parents,
            or the parents have been created already)
            Children will be recursively created, too
            :return: True if successful
            """
            # Create the folders passed as parameters
            my_payload = []
            num_rows = folders.shape[0]
            # if num_rows == 0:
            #     # Let's make sure there is no more work to do -- recursively!
            #     recursive_folder_work(payload.loc[payload.external_id.isin(still_to_do)])
            #     return True
            log_me(f"Folders to process: {num_rows}")
            done_this_job = []
            for _, f in folders.iterrows():
                if not still_to_do:
                    break
                if f.external_id in done:
                    log_me(f"{f.name} was already created")
                    if f.external_id not in done_this_job:
                        done_this_job.append(f.external_id)
                    if f.external_id in still_to_do:
                        still_to_do.remove(f.external_id)
                    # this means no updating of folders
                    continue
                if f.external_id in still_to_do:
                    my_dict = dict(f)
                    if pd.isnull(f.parent_folder) or f.parent_folder is None:
                        log_me(f"Assuming {f.name} is top level")
                        del my_dict['parent_folder']
                    # see if the parent folder is already there. otherwise, call this
                    elif f.parent_folder and f.parent_folder not in done:
                        # let's see if have details for the parent folder
                        parent = folders.loc[folders.external_id==f.parent_folder]
                        if parent.shape[0]==1:
                            recursive_folder_work(parent)
                        else:
                            log_me(f"Cannot create a folder without details for the parent!")
                            still_to_do.remove(f.external_id)
                            continue

                    my_payload.append(my_dict)
                    done.append(f.external_id)
                    done_this_job.append(f.external_id)
                    try:
                        still_to_do.remove(f.external_id)
                    except:
                        log_me(f"Unexpected?")
            if my_payload:
                r = requests.post(self.host + f"/integration/v2/bi/server/{bi_server_id}/{self.endpoint[otype]}/",
                                  headers=dict(token=self.token),
                                  json=my_payload,
                                  verify=self.verify)
                if(self.check_job_status(r)):
                    # Now let's ensure the folders actually got created
                    api = f"/integration/v2/bi/server/{bi_server_id}/folder/"
                    r = requests.get(self.host + api, headers=dict(token=self.token),
                                     params=dict(oids=",".join(done_this_job),
                                                 keyField="external_id"),
                                     verify=self.verify)
                    folders_requested = pd.DataFrame(my_payload)
                    folders_created = pd.DataFrame(r.json())
                    folders_created['otype'] = "bi_folder"
                    folders_created['tag'] = folders_created.apply(lambda y:
                                                                   self.tag_an_object(
                                                                       y['otype'],
                                                                       y['id'],
                                                                       y.external_id
                                                                   ), axis=1)

                    folders_created['path'] = ""
                    self.api_columns.extend(list(folders_created.columns))
                    if not folders_created.empty:
                        folders_created = folders_created.set_index(['otype', 'id'], drop=True)
                        self.bi_folders = pd.concat([self.bi_folders, folders_created])
                        # -- work on the path --
                        """We just created some folders, we know what their parents are.
                        We want to add the parent to the newly created folders.
                        We find the information about the parents in the cache,
                        since either they were created earlier or downloaded"""
                        def return_path_of_parent_folder(folder):
                            external_id_of_parent = folder.parent_folder
                            if not external_id_of_parent:
                                log_me(f"{folder['name']} is a top level folder.")
                                return folder['name']
                            index_of_folder = self.bi_folders.loc[self.bi_folders.external_id==external_id_of_parent].index
                            if not index_of_folder.empty:
                                path = self.bi_folders.loc[index_of_folder, "path"].iloc[0]
                                return path + "//" + folder['name']
                            else:
                                log_me(f"{folder} does not seem to have a path.")
                        self.bi_folders['path'] = self.bi_folders.apply(return_path_of_parent_folder, axis=1)
                        # set_path = self.add_path_to_folders(folders_created.loc[folders_created.parent_folder.isna()],
                        #                                     self.bi_folders)
                    print(self.bi_folders.loc[folders_created.index, ['path','name']].sort_index())
                    missing = folders_requested.loc[set(folders_requested.external_id)-set(folders_created.external_id)]
                    if not missing.empty:
                        log_me(f"Some folders are missing:")
                        print(folders_created.loc[:, ['path', 'name']].sort_index())

            # Now let's create some children...
            children = payload.loc[payload.parent_folder.isin(done_this_job)]
            if children.shape[0]:
                recursive_folder_work(children)
            return True

        """
        Two modes of work:
        (1) create folders from scratch, starting with top most
        (2) assume they exist already, process all at once 
        """
        start_with = payload.loc[(payload.parent_folder.isnull()) | (payload.parent_folder=='')]
        if start_with.shape[0]:
            res = recursive_folder_work(start_with)
        else:
            # if there are no folders at root level we can assume folders exist already
            res = recursive_folder_work(payload)

        if res:
            del api_cols[otype]
        else:
            log_me(f"Creation of BI Folders seems to have failed.")

        missing_reports = []
        for otype, payload_cols in api_cols.items():
            payload = df.loc[df.otype==otype, payload_cols].sort_values("created_at")
            if payload.empty:
                log_me(f"Nothing to do for {otype}")
                continue
            for col, op in payload_cols.items():
                payload[col] = payload[col].apply(op)
            if self.bi_reports is None:
                self.bi_reports = self.get_bi_reports(self.bi_server_id)
            my_payload = []
            nrows = payload.shape[0]
            for n, row in payload.reset_index().iterrows():
                if otype=="bi_report":
                    # validate BI report parent folder...
                    parent_folder = row.get('parent_folder')
                    if parent_folder not in list(self.bi_folders.external_id):
                        log_me(f"Report {row['name']} does not have a parent folder.")
                        missing_reports.append(row['external_id'])
                        continue
                if otype=="bi_report_column":
                    report = row.get("report")
                    if report in missing_reports:
                        continue
                    if report not in list(self.bi_reports.external_id):
                        log_me(f"Report col {row['name']} does not have a valid parent report: {report}")
                        missing_reports.append(report)
                        continue

                my_dict = dict(row)
                del my_dict['index']
                my_payload.append(my_dict)
            if my_payload:
                r = requests.post(self.host + f"/integration/v2/bi/server/{bi_server_id}/{self.endpoint[otype]}/",
                                  headers=dict(token=self.token),
                                  json=my_payload,
                                  verify=self.verify)
                if not self.check_job_status(r):
                    log_me(f"{otype} creation was not successful. Please check inputs.")
                    break


    def check_job_status(self, r):
        j = r.json()
        job_id = j.get("job_id")
        if "errors" in j:
            errors = j['errors']
            for e in errors:
                if e:
                    log_me(e)
        while(True):
            r = requests.get(self.host + f"/api/v1/bulk_metadata/job/",
                              headers=dict(token=self.token),
                              params=dict(id=job_id),
                              verify=self.verify)
            j = r.json()
            status = j.get("status", "")
            if "running" in status:
                time.sleep(3)
                continue
            elif "finished" in j.get("msg", ""):
                log_me(j.get("msg"))
                log_me(j.get("result"))
                return True
            elif "failed" in status:
                log_me(j.get("msg"))
                log_me(j.get("result"))
                break
            else:
                log_me(j)
                break

    def delete_bi_object(self, otype, external_id, bi_server_id):
        my_id = self.convert_external_id(otype, external_id, bi_server_id)
        if not my_id:
            log_me(f"Could not delete {otype}/{external_id}")
            return

        if otype=="bi_report_column":
            r = requests.delete(self.host +
                    f"/integration/v2/bi/server/{bi_server_id}/{self.endpoint[otype]}",
                             headers=dict(token=self.token),
                             json=dict(oids=my_id),
                             verify=self.verify)
        elif otype in ["bi_folder", "bi_report"]:
            r = requests.delete(self.host +
                    f"/integration/v2/bi/server/{bi_server_id}/{self.endpoint[otype]}/{my_id}",
                             headers=dict(token=self.token),
                             verify=self.verify)
        else:
            log_me(f"Don't know how to delete {otype}")
            return

        if r.status_code==204:
            log_me(f"Deleted {otype}/{my_id}")
        else:
            log_me(f"Could not delete {otype}/{my_id}")

    def convert_external_id(self, otype, external_id, server_id):
        id = self.external_id.get(external_id)
        if id:
            return id
        try:
            endpoint = self.endpoint.get(otype)
            r = requests.get(self.host + f"/integration/v2/bi/server/{server_id}/{endpoint}/",
                             headers=dict(token=self.token),
                             params=dict(oids=[external_id],
                                         keyField="external_id"),
                             verify=self.verify)

            j = r.json()
            if len(j) == 1:
                self.external_id[external_id] = j[0]['id']
                return int(j[0]['id'])
            else:
                log_me(f"Could not find {otype} with ID {external_id}")
                return
        except Exception as e:
            log_me(f"EXCEPTION ({e}): Could not find {otype} with ID {external_id}")
            return

    def get_fully_qualified_name(self, otype, id):
        if otype=="data":
            return f"{id}"
        elif otype=="schema":
            schema = requests.get(self.host + f"/integration/v2/schema/",
                                 headers=dict(token=self.token),
                                 params=dict(id=id),
                                 verify=self.verify)
            if schema.status_code:
                my_schema = schema.json()[0]
                ds_id = my_schema.get('ds_id')
                schema_name = my_schema.get('name')
                return f"{ds_id}.{schema_name}"
            else:
                log_me(f"Could not find schema {id}")
                return
        elif otype=="table":
            table = requests.get(self.host + f"/integration/v2/table/",
                                 headers=dict(token=self.token),
                                 params=dict(id=id),
                                 verify=self.verify)
            if table.status_code:
                my_table = table.json()[0]
                ds_id = my_table.get('ds_id')
                schema_name = my_table.get('schema_name')
                table_name = my_table.get('name')
                return f"{ds_id}.{schema_name}.{table_name}"
            else:
                log_me(f"Could not find table {id}")
                return
        elif otype=="column" or otype=="attribute":
            col = requests.get(self.host + f"/integration/v2/column/",
                                 headers=dict(token=self.token),
                                 params=dict(id=id),
                                 verify=self.verify)
            if col.status_code:
                my_col = col.json()[0]
                table_id = my_col.get('table_id')
                col_name = my_col.get('name')
                return f"{self.get_fully_qualified_name('table', table_id)}.{col_name}"
            else:
                log_me(f"Could not find col {id}")
                return
        elif otype=="term" or otype=="glossary_term":
            art = requests.get(self.host + f"/integration/v2/term/",
                                 headers=dict(token=self.token),
                                 params=dict(id=id),
                                 verify=self.verify)
            if art.status_code:
                art_title = art.json()[0].get('title')
                return art_title
            else:
                log_me(f"Could not find term {id}")
                return
        elif otype=="article":
            art = requests.get(self.host + f"/integration/v1/article/{id}/",
                                 headers=dict(token=self.token),
                                 verify=self.verify)
            if art.status_code:
                art_title = art.json().get('title')
                return art_title
            else:
                log_me(f"Could not find article {id}")
                return
        elif otype=="user":
            user = requests.get(self.host + f"/integration/v1/user/{id}/",
                                 headers=dict(token=self.token),
                                 verify=self.verify)
            if user.status_code:
                user_display = user.json().get('display_name')
                user_email = user.json().get('email')
                return f"{user_display}/({user_email})"
            else:
                log_me(f"Could not find user {id}")
                return
        elif otype=="groupprofile":
            group = requests.get(self.host + f"/integration/v1/group/{id}/",
                                 headers=dict(token=self.token),
                                 verify=self.verify)
            if group.status_code:
                group_display = group.json().get('display_name')
                # user_email = user.json().get('email')
                return f"{group_display}"
            else:
                log_me(f"Could not find group {id}")
                return
        else:
            log_me(f"Unrecognized otype: {otype}")


    def reverse_qualified_name(self, otype, fqn):
        if fqn in self.fqn_cache:
            return self.fqn_cache.get(fqn)
        if otype=="data":
            return to_int(fqn)
        elif otype=="schema":
            parse = re.match(r"([0-9]+)\.([^.]+)(\.[^.]+)?", fqn)
            ds_id = to_int(parse.group(1))
            if len(parse.groups())==2:
                name = parse.group(2)
            elif len(parse.groups())==3:
                name = parse.group(2)+parse.group(3)
            else:
                log_me(f"Could not parse {otype} {fqn}")
                return
            obj = requests.get(self.host + f"/integration/v2/{otype}/",
                                 headers=dict(token=self.token),
                                 params=dict(ds_id=ds_id, name=name.lower()),
                                 verify=self.verify)
            if obj.status_code and len(obj.json())==1:
                my_obj = obj.json()[0]
                self.fqn_cache[fqn] = my_obj.get('id')
                return my_obj.get('id')
            else:
                log_me(f"Could not find {otype} {fqn}")
                return
        elif otype=="table":
            # parse = re.match(r"([0-9]+)\.([^.]+)(\.[^.]+)?\.?([^.]+)", fqn)
            # ds_id = to_int(parse.group(1))
            # if len(parse.groups()) == 3:
            #     schema_name = parse.group(2)
            #     name = parse.group(3)
            # elif len(parse.groups()) == 4:
            #     schema_name = parse.group(2) + parse.group(3)
            #     name = parse.group(4)
            # else:
            #     log_me(f"Could not parse {otype} {fqn}")
            #     return
            components = fqn.split(".")
            if len(components)==3:
                ds_id = to_int(components[0])
                schema_name = components[1]
                name = components[2]
            elif len(components)==4:
                ds_id = to_int(components[0])
                schema_name = f"{components[1]}.{components[2]}"
                name = components[3]
            else:
                log_me(f"Could not parse {otype} {fqn}")
                return


            obj = requests.get(self.host + f"/integration/v2/{otype}/",
                               headers=dict(token=self.token),
                               params=dict(ds_id=ds_id,
                                           schema_name__iexact=schema_name,
                                           name__iexact=name,
                                           ),
                               verify=self.verify)
            # if obj.status_code and len(obj.json())>=1:
            #     tables_found = pd.DataFrame(obj.json())
            #     tables_found['schema_lower'] = tables_found.schema.apply(str.lower)
            #     iexact = tables_found.loc[tables_found.schema_lower==schema_name.lower()]
            #     if iexact.shape[0]==1:
            #         my_id = iexact.iloc[0]['id']
            #         self.fqn_cache[fqn] = my_id
            #         return my_id
            #     else:
            #         log_me(f"Could not find {otype} {fqn}")
            #         return
            #
            if obj.status_code and len(obj.json())==1:
                my_obj = obj.json()[0]
                self.fqn_cache[fqn] = my_obj.get('id')
                return my_obj.get('id')
            else:
                log_me(f"Could not find {otype} {fqn}")
                return
        elif otype=="column" or otype=="attribute":
            parse = re.match(r"([0-9]+)\.([^.]+)(\.[^.]+)?\.?([^.]+)\.([^.]+)", fqn)
            ds_id = to_int(parse.group(1))
            if len(parse.groups()) == 4:
                schema_name = parse.group(2)
                schema_id = self.reverse_qualified_name("schema", f"{ds_id}.{schema_name}")
                table_name = parse.group(3)
                name = parse.group(4)
            elif len(parse.groups()) == 5:
                schema_name = parse.group(2) + parse.group(3)
                schema_id = self.reverse_qualified_name("schema", f"{ds_id}.{schema_name}")
                table_name = f"{schema_name.lower()}.{(parse.group(4)).lower()}"
                name = parse.group(5)
            else:
                log_me(f"Could not parse {otype} {fqn}")
                return
            obj = requests.get(self.host + f"/integration/v2/column/",
                               headers=dict(token=self.token),
                               params=dict(ds_id=ds_id,
                                           schema_id=schema_id,
                                           table_name=table_name,
                                           name=name,
                                           ),
                               verify=self.verify)
            if obj.status_code and len(obj.json())==1:
                my_obj = obj.json()[0]
                self.fqn_cache[fqn] = my_obj.get('id')
                return my_obj.get('id')
            else:
                log_me(f"Could not find {otype} {fqn}")
                return
        elif otype=="term" or otype=="glossary_term":
            my_search = requests.get(self.host + f"/integration/v1/search/",
                                 headers=dict(token=self.token),
                                 params=dict(q=fqn,
                                             filters='{"otypes":["glossary_term"]}'),
                                 verify=self.verify)
            if my_search.status_code:
                my_result = my_search.json()
                if my_result.get('total')==1:
                    return my_result['results'][0]['id']
                log_me(f"Could not find term {fqn}")
                log_me(f"Search returned: {my_search.content}")
                return
        elif otype=="article":
            art = requests.get(self.host + f"/integration/v1/article/",
                                 headers=dict(token=self.token),
                                 params=dict(title__icontains=fqn),
                                 verify=self.verify)
            if art.status_code:
                if len(art.json()):
                    articles = pd.DataFrame(art.json())
                    exact_article = articles.loc[articles.title==fqn]
                    if exact_article.shape[0]==1:
                        return int(exact_article.at[0, 'id'])
            log_me(f"Could not find article {fqn}")
            return
        elif otype=="user":
            parse = re.search(f"\(([^)]+)\)", fqn)
            if parse:
                user = requests.get(self.host + f"/integration/v1/user/",
                                     headers=dict(token=self.token),
                                     params=dict(email=parse.group(1)),
                                     verify=self.verify)
                if user.status_code and len(user.json())==1:
                    return user.json()[0].get('id')
                else:
                    log_me(f"Could not find user {fqn}")
                    return
        elif otype=="group" or otype=="groupprofile":
            group = requests.get(self.host + f"/integration/v1/group/",
                                 headers=dict(token=self.token),
                                 params=dict(display_name=fqn),
                                 verify=self.verify)
            if group.status_code:
                my_groups = group.json()
                if len(my_groups)==1:
                    return my_groups[0].get('id')
            log_me(f"Could not find group {fqn}")
            return
        else:
            log_me(f"Unrecognized otype: {otype}")


    def post_data_health(self, payload):
        r = requests.post(self.host + "/integration/v1/data_quality/",
                          headers=dict(token=self.token),
                          json=payload)
        r2 = self.check_job_status(r)
        return

    def create_new_policy(self, template_id=None, policy_group_id=None):
        payload = dict(otype="business_policy",
                       owner_id=self.user_id)
        r = requests.post(self.host + "/api/policy/",
                          headers=dict(token=self.token),
                          json=payload)
        j = r.json()
        id = j.get('id')
        if id and template_id:
            r = requests.post(self.host + f"/api/v1/template/object/business_policy/{id}/",
                              headers=dict(token=self.token),
                              json=dict(op="replace",
                                        template_ids=[template_id]))
            if not r.status_code:
                print(f"Problem assigning template ID {template_id}")

        if id and policy_group_id:
            r = requests.post(self.host + f"/api/policy_group/{policy_group_id}/links/",
                              headers=dict(token=self.token),
                              json=dict(otype="business_policy",
                                        oid=str(id)))
            if not r.status_code:
                print(f"Problem assigning to group ID {template_id}")

        return id

    def find_article_by_title(self, title, template_id):
        r = requests.get(self.host + "/integration/v1/article/",
                          headers=dict(token=self.token),
                          params=dict(title=title,
                                      custom_field_templates=f"[{template_id}]"))
        j = r.json()
        if len(j)==1:
            id = j[0].get('id')
            return id
        else:
            log_me(f"Needed exactly one result, but got: {j}")

    def modify_single_field_on_article(self, article_id, template_id, field_name, value):
        # check the old value
        my_art = requests.get(self.host + f"/integration/v1/article/{article_id}/",
                          headers=dict(token=self.token))
        if not my_art:
            log_me(f"Article with ID {article_id} does not seem to exist.")
            return

        my_custom_fields = my_art.json().get("custom_fields")
        if my_custom_fields:
            my_custom_fields_df = pd.DataFrame(my_custom_fields).set_index('field_name')
            old_value = my_custom_fields_df.at[field_name, 'value']

        # validate the template first
        template_name = self.existing_templates.at[template_id, 'title']
        if not template_name:
            log_me(f"No template found for ID {template_id}")
            return
        url_encoded = urllib.parse.quote(template_name)

        # validate field_name
        field = self.existing_fields.loc[self.existing_fields.name_singular==field_name]
        if field.empty:
            log_me(f"No field found for name {field_name}")
            return

        payload = dict(article_id=str(article_id))
        payload[field_name] = value

        r = requests.post(self.host + f"/api/v2/bulk_metadata/custom_fields/{url_encoded}/article",
                          headers=dict(token=self.token),
                          params=dict(create_new=False, replace_values=True),
                          json=payload)
        if not r.status_code:
            log_me(f"Could not update custom field {field_name}")
        else:
            log_me(r.json())


