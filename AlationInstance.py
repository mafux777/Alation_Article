import requests
import time
import pandas as pd
import json
from bs4 import BeautifulSoup
import urllib

from alationutil import log_me
from secure_copy import list_files

import errno
import os
import zipfile
from collections import OrderedDict, deque, defaultdict
from math import isnan

# The AlationInstance class is a handle to an Alation server defined by a URL
# A server admin user name and password needs to be provided and all API actions
# will be run as that user
class AlationInstance():
    # The __init__ method is the constructor used for instantiating
    # email: the up to 30 chars user name, often the email, but for long emails could be cut off
    # password: could be the LDAP password, as well
    # verify: Requests verifies SSL certificates for HTTPS requests, just like a web browser.
    # By default, SSL verification is enabled, and Requests will throw a SSLError if it’s unable to verify the certificate
    def __init__(self, host, email, password, verify=True):
        self.host = host
        self.verify = verify
        self.email = email
        self.password = password
        self.headers = self.login(email, password)
        log_me("Getting existing custom fields")
        self.existing_fields = self.get_custom_fields() # store existing custom fields
        log_me("Getting existing templates")
        self.existing_templates = self.get_templates() # store existing templates
        log_me("Getting existing data sources")
        self.ds = self.getDataSources()
        self.articles = pd.DataFrame() # cache for Articles
        log_me(self.ds.loc[ : , ['id', 'title']].head(10))
    # The login method is used to obtain a session ID and relevant cookies
    # They are cached in the headers variable
    # email: the up to 30 chars user name, often the email, but for long emails could be cut off
    # password: could be the LDAP password, as well
    def login(self, email, password):
        URL = self.host + '/login/'

        s = requests.Session()
        s.get(URL, verify=self.verify)

        # get the cookie token
        csrftoken = s.cookies.get('csrftoken')

        # login with user name and password (and token)
        payload = {"csrfmiddlewaretoken": csrftoken, "ldap_user": email, "password": password}
        headers = {"Referer": URL}
        log_me("Logging in to {}".format(URL))
        r = s.post(URL, data=payload, verify=self.verify, headers=headers)

        # get the session ID and store it for all future API calls
        sessionid = s.cookies.get('sessionid')
        headers = {"X-CSRFToken": csrftoken,
                   "Cookie": "csrftoken=" + csrftoken + "; sessionid=" + sessionid,
                   "Referer": URL
                   }

        return headers

    # The get_custom_fields method returns a pandas DataFrame with all custom fields
    # The Alation ID will also be the ID of the DataFrame
    def get_custom_fields(self, template='all'): # this method returns a DataFrame
        url = self.host + "/ajax/custom_field/"
        payload = {}
        r = requests.get(url, data=json.dumps(payload), headers=self.headers, verify=self.verify)
        if r.status_code == 200:
            fields = pd.DataFrame(json.loads(r.content))
            fields.index = fields.id
            return fields.sort_index()
        else:
            log_me("Could not get custom fields: {}".format(r.content))

    # The get_custom_field_id methdod checks whether a field with that name already exists
    # If yes, returns the ID
    # If no, returns 0
    def get_custom_field_id(self, name):
        if name in self.existing_fields.name:
            return self.existing_fields[self.existing_fields.name==name, "id"]
        else:
            return 0


    # The get_articles method downloads all articles of a specific template (if provided)
    # It does this in chunks of 100, transparent to the user
    # template: name of the template or "all"
    # limit: chunk size (optional)
    # returns a pandas DataFrame
    def get_articles(self, template='all', limit=100):
        log_me("Getting Articles from Instance")
        url = self.host + "/integration/v1/article/"
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
                r = requests.get(url, headers=self.headers, verify=self.verify, params=params)
                skip = skip + limit
                # create the DataFrame and index it properly
                article_chunk = pd.DataFrame(json.loads(r.content))
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
        url = self.host + "/integration/v1/table/"
        components = name.split('.')
        schema_name = components[0]
        table_name  = components[1]
        params = dict(name=table_name, schema_name=schema_name)
        r = requests.get(url, headers=self.headers, verify=self.verify, params=params)
        # create the DataFrame and index it properly
        table_with_name = pd.DataFrame(json.loads(r.content))
        size = table_with_name.shape[0]
        if size>0:
            table_with_name.index = table_with_name.id
            return table_with_name[table_with_name.name==table_name]
        else:
            #log_me("Could not find table {}".format(name))
            return pd.DataFrame()

    # The get_article_by_id method returns a dictionary with all the article attributes provided by the Article API
    # ID: the numerical ID of an existing article
    def get_article_by_id(self, id):
        url = self.host + "/integration/v1/article/" + str(id) + "/"
        r = requests.get(url, headers=self.headers, verify=self.verify)
        # return a dictionary
        article = json.loads(r.content)
        return article

    # The post_article method creates a new article based on a dictionary with at least title and body
    # and returns a dictionary with all attributes, e.g. id, author, timestamp, etc.
    def post_article(self, article):
        url = self.host + "/integration/v1/article/"
        r = requests.post(url, headers=self.headers, verify=self.verify, json=dict(article))
        # return a dictionary
        art = json.loads(r.content)
        return art

    # The del_article method deletes an existing article and returns nothing
    def del_article(self, id):
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
        art = json.loads(r.content)
        return art

    # The download_datadict method returns a pandas DataFrame with key, title and description for a specific data source
    # This only works until R5 (inclusive)
    def download_datadict(self, ds_id):
        url = self.host + "/data/"+str(ds_id)+"/download_dict/data/"+str(ds_id)+"/"
        params = dict(format='json')
        r = requests.get(url, headers=self.headers, verify=self.verify, params=params)
        r_parsed = json.loads(r.content)
        dd = pd.DataFrame(r_parsed[1:]) # skipping first row, no key
        dd.index = dd.key
        log_me("This data dict contains {} items.".format(dd.shape[0]))
        return dd.loc[:, ['key', 'title', 'description']]

    # The download_datadict_r6 uses the metadata API to download a data dictionary
    # It will contain key, title, description, and the numerical IDs of the schema, table, column
    # Return a dataframe
    #
    def download_datadict_r6(self, ds_id):
        # url = self.host + "/data/download_dict/"
        # form = dict(format='json', otype="data", oid=ds_id)
        # r = requests.post(url, headers=self.headers, verify=self.verify, data=form)
        # r_parsed = json.loads(r.content)
        # return r_parsed
        token = self.get_token()
        headers=dict(token=token)
        schemas = dict()

        list_of_elements = list()

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
        url = self.host + f'/integration/v1/schema/?ds_id={ds_id}'
        r = requests.get(url=url, headers=headers)
        for schema in r.json():
            name = schema['name']
            schemas[schema['id']] = name # we need this later
            list_of_elements.append(dict(
                key         = f'{ds_id}.{name}',
                title       = schema['title'],
                description = schema['description'],
                schema_id   = str(schema['id'])
            ))

        # Table
        url = self.host + f'/integration/v1/table/?ds_id={ds_id}'
        r = requests.get(url=url, headers=headers)
        for table in r.json():
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
        url = self.host + f'/integration/v1/column/?ds_id={ds_id}'
        r = requests.get(url=url, headers=headers)
        for col in r.json():
            name = col['name']
            schema_name = schemas[col['schema_id']] # unfortunately, this does not come with the API
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
        change_token = "/api/v1/changeToken/"  # if you already have a token, use this url
        new_token = "/api/v1/getToken/"  # if you have never generated a token, use this url
        data = dict(username=self.email, password=self.password)
        response = requests.post(self.host + new_token, data=data)
        api_token = response.text
        if api_token == "EXISTING":
            response = requests.post(self.host + change_token, data=data)
            api_token = response.text
        return api_token

    # The get_templates method returns a DataFrame with all templates, sorted and indexed by ID
    # This includes built-in templates
    def get_templates(self):
        url = self.host + "/integration/v1/custom_template/"
        params = dict(limit=1000)
        r = requests.get(url, headers=self.headers, verify=self.verify, params=params)
        templates = pd.DataFrame(json.loads(r.content))
        templates.index = templates.id
        return templates.sort_index()

    # The delete_customfield method deleted a single custom field by ID
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
        r = requests.get(self.host + "/ajax/custom_template/" + str(template_id) + "/",
                         headers=self.headers, verify=self.verify)

        template = json.loads(r.text)
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

        log_me("Creating a new custom field {}".format(name_singular))

        url = self.host + "/ajax/custom_field/"

        headers = self.headers
        headers['Referer'] = url

        r = requests.post(url, data=json.dumps(payload), headers=headers, verify=self.verify)
        if (r.status_code != 200):
            raise Exception(r.text)

        field = json.loads(r.text)
        return field['id']

    # The put_custom_fields method takes a DataFrame with custom fields and creates them one by one,
    # returning a list of all the field IDs, existing or newly created
    # In many cases the source of the custom fields DataFrame would be a call to the Template API
    # Otherwise you would use the method above: create_customfield
    def put_custom_fields(self, custom_fields_pd):  # takes a DataFrame obtained from the Template API
        def process_line(custom_f):
            field_exists = self.existing_fields.loc[self.existing_fields.name_singular==custom_f['name_singular']]
            # should return only one row if the field exists already
            if len(field_exists)==0:
                log_me("Putting custom field {}".format(custom_f['name_singular']))
                if custom_f['options']:
                    if isinstance(custom_f['options'], str): # came from CSV
                        custom_f['options'] = custom_f['options'].split(",")
                    custom_f['options'] = json.dumps(
                    [{"title": option, "tooltip_text": None, "old_index": None, "article_id": None} \
                     for option in custom_f['options']])
                custom_f['field_otype']='custom_field'
                custom_f['value']=None
                custom_f['name']=None
                url = self.host + "/ajax/custom_field/"

                headers = self.headers
                headers['Referer'] = url

                r = requests.post(url, data=json.dumps(dict(custom_f)), headers=headers, verify=self.verify)
                if r.status_code != 200:
                    raise Exception(r.text)

                field = json.loads(r.text)
                self.existing_fields = self.get_custom_fields() # so we don't get a duplicate next time (could be more efficient)
                return field['id']
            elif len(field_exists)==1:
                log_me("{} already exists (info only)".format(field_exists.iloc[0, 6]))
                return field_exists.iloc[0]['id']  # ID of the field
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
        url = self.host + "/ajax/custom_template/"

        keys = ["id", "title", "builtin_name", "field_ids", "template_in_use"]
        payload = {}

        payload['fields'] = []
        payload['title'] = template
        url = self.host + "/ajax/custom_template/"
        headers = self.headers
        headers['Referer'] = url
        log_me("Putting template {}".format(template))
        r = requests.post(url, json=payload, headers=headers, verify=self.verify)
        if r.status_code != 200:
            raise Exception(r.text)

        t = json.loads(r.text)
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
        if custom_fields.empty:
            body = article.bulk_api_body()
        else:
        # Body needs to be a text with one JSON per line
        # Note we are sending all custom fields to the function, maybe overkill!
            custom_fields_pd = self.existing_fields.loc[custom_fields, :]
            body = article.bulk_api_body(custom_fields_pd)
        params = dict(replace_values=True, create_new=True)
        try:
            r = requests.post(url, data=body, headers=self.headers, params=params, verify=self.verify)
            if r.status_code != 200:
                raise Exception(r.text)
            return r
        except IOError as e:
            log_me("I/O error({0}): {1}".format(e.errno, e.strerror))
        except Exception as e:
            log_me(f"Unexpected {e}")

    # The put_articles_2 method is a simplified version of the method above
    # article: expects a ready made JSON for the body (one JSON per line=article)
    # template_name: this template will be applied to the articles
    # returns a response object (see requests module)
    def put_articles_2(self, article, template_name):
        #log_me("Putting Articles on Instance")
        # Template name needs to be part of the URL
        template_name = template_name.replace(" ", "%20")
        url = self.host + "/api/v1/bulk_metadata/custom_fields/" + template_name + "/article"
        params=dict(replace_values = True, create_new = True)
        try:
            r = requests.post(url, data=article, headers=self.headers, params=params, verify=self.verify)
            if r.status_code != 200:
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
    def getQueries(self):
        log_me("Getting queries")
        url = self.host + "/api/query/"
        params = dict(limit=1000, saved=True, published=True, deleted=False)
        r = requests.get(url, headers=self.headers, verify=self.verify, params=params)
        queries = pd.DataFrame(json.loads(r.content))
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
    def putQueries(self, queries):
        ex_queries = self.getQueries()
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
                    if ori_ds_id==1 and aa: # Alation Analytics!
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
        users = pd.DataFrame(json.loads(r.content))
        return users

    # The getDataSources method downloads all data sources (metadata only) as a DataFrame
    def getDataSources(self):
        url = self.host + "/ajax/datasource/"
        r = requests.get(url, headers=self.headers, verify=self.verify)
        ds = pd.DataFrame(json.loads(r.content))
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
        queries = self.getQueries()
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
                            art_match = articles.title==title
                            if art_match.any():
                                # m is a reference to somewhere and we need to fix it.
                                oid = articles.id[art_match.idxmax()]
                                m['data-oid'] = oid
                                m['href'] = "/{}/{}/".format(otype, oid)

                                articles.at[id, 'body'] = soup.prettify()  # update the article body
                                articles.at[id, 'updated'] = True  # update the article body
                                update_needed = True
                                # log_me("Article match for {} -> {}/{}".format(t, title, oid))
                            else:
                                log_me("No article match for {}->{}".format(t, title))
                        except:
                            log_me("Exception trying to match {} -> {}".format(t, title))
                    # Process links to queries
                    elif otype == 'query' and not queries.empty:
                        q_match = queries.title == title
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
                            log_me("No match for {}".format(title))
            if update_needed:
                log_me("Updating article {}:{} -> {}".format(id, t, title))
                new_article = dict(body=articles.at[id, 'body'], title=t)  # only the required fields...
                updated_art = self.update_article(int(id), new_article)
        log_me("Finished preparing references. ")


    # The fix_refs_2 method finds at-mentions in a description and sets them to 0
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
            queries = self.getQueries()
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
            if r.status_code != 200:
                raise Exception(r.text)
            return r
        except IOError as e:
            log_me("I/O error({0}): {1}".format(e.errno, e.strerror))
        except ValueError:
            log_me("Could not convert data to an integer.")

    # This method gets all API resources in a DataFrame
    # The information is limited, so this is only a helper function for
    # entire_api
    def get_api_resource_all(self):
        api = "/integration/v1/api_resource/"
        url = self.host + api
        try:
            r = requests.get(url, headers=self.headers, verify=self.verify)
            if r.status_code != 200:
                raise Exception(r.text)
            fields = pd.DataFrame(json.loads(r.content))
            fields.index = fields.id
            return fields.sort_index()
        except IOError as e:
            log_me("I/O error({0}): {1}".format(e.errno, e.strerror))
        except ValueError:
            log_me("Could not convert data to an integer.")
        except:
            log_me("Unexpected error: {}".format(sys.exc_info()[0]))
            raise

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

    # def extract_logical(self, api_dict):
    #     if "properties" in api_dict: # like children
    #         [self.extract_logical(p) for p in api_dict['properties']]
    #     else: # at the end of the tree...
    #         return dict(key=api_dict['key'],
    #                    title=api_dict['title'],
    #                    description=api_dict['description'])


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
            if r.status_code != 200:
                raise Exception(r.text)
            return json.loads(r.content)
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
            if r.status_code != 200:
                raise Exception(r.text)
            a = json.loads(r.content)
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
        except ValueError:
            log_me("Could not convert data to an integer.")
        except:
            log_me("Unexpected error: {}".format(sys.exc_info()[0]))
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
            for k, v in api_dict['properties'].iteritems():
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
            if r.status_code != 200:
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
            api, f = self.get_api_resource(api.Index)
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
            if r.status_code != 200:
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
        except ValueError:
            log_me("Could not convert data to an integer.")
