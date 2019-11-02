import requests
import time
import pandas as pd
import json
from bs4 import BeautifulSoup
import html5lib
import urllib

from alationutil import log_me
from secure_copy import list_files

import errno
import os
import zipfile
from collections import OrderedDict, deque, defaultdict
from math import isnan

class AlationInstance():
    def __init__(self, host, email, password, verify=True):
        self.host = host
        self.verify = verify
        self.headers = self.login(email, password)
        log_me("Getting existing custom fields")
        self.existing_fields = self.getCustomFields() # store existing custom fields
        log_me("Getting existing templates")
        self.existing_templates = self.getTemplates() # store existing templates
        log_me("Getting existing data sources")
        self.ds = self.getDataSources()
        self.articles = pd.DataFrame() # cache for Articles
        log_me(self.ds.loc[ : , ['id', 'title']].head(10))

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

    def delete_all_fields(self):
        for field in json.loads(self.get_customfields()):
            self.delete_customfield(field['id'])

    def get_customfields(self): # this method returns a JSON document
        url = self.host + "/ajax/custom_field/"
        payload = {}
        r = requests.get(url, data=json.dumps(payload), headers=self.headers, verify=self.verify)
        return r.content

    def getCustomFields(self, template='all'): # this method returns a DataFrame
        url = self.host + "/ajax/custom_field/"
        payload = {}
        r = requests.get(url, data=json.dumps(payload), headers=self.headers, verify=self.verify)
        if r.status_code == 200:
            fields = pd.DataFrame(json.loads(r.content))
            fields.index = fields.id
            return fields.sort_index()
        else:
            log_me(u"Could not get custom fields: {}".format(r.content))

    def getCustomFieldID(self, name):
        if name in self.existing_fields.name:
            return self.existing_fields[self.existing_fields.name==name, "id"]
        else:
            return 0

    def getArticles(self, template='all', limit=100):
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
        while True:
            try:
                params['limit'] = limit
                params['skip'] = skip
                t0 = time.time()
                r = requests.get(url, headers=self.headers, verify=self.verify, params=params)
                skip = skip + limit
                # create the DataFrame and index it properly
                article_chunk = pd.DataFrame(json.loads(r.content))
                article_chunk.index = article_chunk.id
                articles = articles.append(article_chunk)
                size = article_chunk.shape[0]
                log_me("Took {} secs for {} items".format(time.time()-t0, size))
                if size < limit: # not enough articles to continue
                    log_me(u"Total number of articles downloaded: {}".format(skip-limit+size))
                    break
            except:
                break
        self.articles = articles # cache for later!
        return articles

    def getTables(self, ds_id, limit=100):
        log_me("Getting Tables from Instance")
        url = self.host + "/integration/v1/table/"
        skip = 0
        tables = pd.DataFrame()
        params = dict(ds_id=ds_id)
        while True:
            try:
                params['limit'] = limit
                params['skip'] = skip
                t0 = time.time()
                r = requests.get(url, headers=self.headers, verify=self.verify, params=params)
                skip = skip + limit
                # create the DataFrame and index it properly
                table_chunk = pd.DataFrame(json.loads(r.content))
                if table_chunk.empty:
                    log_me(u"No tables for data source id {}".format(ds_id))
                    break
                table_chunk.index = table_chunk.id
                tables = tables.append(table_chunk)
                size = table_chunk.shape[0]
                log_me("Took {} secs for {} items".format(time.time()-t0, size))
                if size < limit: # not enough articles to continue
                    log_me(u"Total number of tables downloaded: {}".format(skip-limit+size))
                    break
            except:
                break
        return tables

    def getTablesByName(self, name):
        url = self.host + "/integration/v1/table/"
        components = name.split('.')
        schema_name = components[0]
        table_name  = components[1]
        params = dict(name=table_name, schema_name=schema_name)
        r = requests.get(url, headers=self.headers, verify=self.verify, params=params)
        # create the DataFrame and index it properly
        table_chunk = pd.DataFrame(json.loads(r.content))
        size = table_chunk.shape[0]
        if size>0:
            table_chunk.index = table_chunk.id
            return table_chunk[table_chunk.name==table_name]
        else:
            #log_me(u"Could not find table {}".format(name))
            return pd.DataFrame()


    def getArticleByID(self, id):
        url = self.host + "/integration/v1/article/" + str(id) + "/"
        r = requests.get(url, headers=self.headers, verify=self.verify)
        # return a dictionary
        article = json.loads(r.content)
        return article

    def postArticle(self, article):
        url = self.host + "/integration/v1/article/"
        r = requests.post(url, headers=self.headers, verify=self.verify, json=article)
        # return a dictionary
        art = json.loads(r.content)
        return art

    def delArticle(self, id):
        url = self.host + "/integration/v1/article/" + str(id) + "/"
        r = requests.delete(url, headers=self.headers, verify=self.verify)

    def updateArticle(self, id, article):
        url = self.host + "/integration/v1/article/" + str(id) + "/"
        article_json = json.dumps(article)
        self.headers['Content-Type'] = "application/json"
        r = requests.put(url, headers=self.headers, verify=self.verify, data=article_json)
        if not r:
            try:
                log_me(u"Issue with updating article {}...".format(article['title']))
                log_me(u"... {}".format(r.content))
            except:
                log_me(u"Formatting issue with article id={}".format(id))
            # return a dictionary
        art = json.loads(r.content)
        return art

    def download_datadict(self, ds_id):
        url = self.host + "/data/"+str(ds_id)+"/download_dict/data/"+str(ds_id)+"/"
        params = dict(format='json')
        r = requests.get(url, headers=self.headers, verify=self.verify, params=params)
        r_parsed = json.loads(r.content)
        dd = pd.DataFrame(r_parsed[1:]) # skipping first row, no key
        dd.index = dd.key
        log_me(u"This data dict contains {} items.".format(dd.shape[0]))
        return dd.loc[:, ['key', 'title', 'description']]

    def getTemplates(self):
        url = self.host + "/integration/v1/custom_template/"
        params = dict(limit=1000)
        r = requests.get(url, headers=self.headers, verify=self.verify, params=params)
        templates = pd.DataFrame(json.loads(r.content))
        templates.index = templates.id
        return templates.sort_index()

    def delete_customfield(self, field_id):
        url = self.host + "/ajax/custom_field/" + str(field_id) + "/"
        r = requests.delete(url, headers=self.headers, verify=self.verify) # no longer passing data parameter

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
            # create a new list of all field IDs including the new one
            payload['fields'] = [str(x) for x in template['field_ids'] + [field_id]]
            url = self.host + "/ajax/custom_template/" + str(template_id) + "/"
            requests.put(url, data=json.dumps(payload), headers=self.headers, verify=self.verify)

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
        payload['fields'] = [str(x) for x in new_fields] # not sure if it is necessary to convert to str first?
        url = self.host + "/ajax/custom_template/" + str(template_id) + "/"
        requests.put(url, json=payload, headers=self.headers, verify=self.verify)

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

    def create_richtext(self, name, tooltip):
        return self.create_customfield(name, name, None, tooltip, "RICH_TEXT")

    def create_date(self, name, tooltip):
        return self.create_customfield(name, name, None, tooltip, "DATE")

    def create_multipicker(self, name, options, tooltip):
        return self.create_customfield(name, name, options, tooltip, "MULTI_PICKER")

    def create_picker(self, name, options, tooltip):
        return self.create_customfield(name, name, options, tooltip, "PICKER")

    def create_customfield(self, name_singular, name_plural, options, tooltip, picker_type, o_type=[]):

        if (options):
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

    def putCustomFields(self,custom_fields_pd):  # takes a DataFrame obtained from the Template API
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
                self.existing_fields = self.getCustomFields() # so we don't get a duplicate next time (could be more efficient)
                return field['id']
            elif len(field_exists)==1:
                #log_me("{} already exists (info only)".format(field_exists.iloc[0, 6]))
                return field_exists.iloc[0]['id']  # ID of the field
            else:
                log_me("WARNING -- MULTIPLE FIELDS WITH THE SAME NAME")
                log_me(field_exists.loc[:,['id', 'name']])
                return field_exists.iloc[0, 6] # ID of the first field (hopefully only one anyway)

        return custom_fields_pd.apply(process_line, axis=1)


    def putCustomTemplate(self, template, fields=[]):
        def process_line(custom_t):
            title = template.iloc[0, 3]
            template_exists = self.existing_templates.loc[self.existing_templates.title == title]
            # should return only one row if the template exists already
            if len(template_exists) == 0:
                url = self.host + "/ajax/custom_template/"

                keys = ["id", "title", "builtin_name", "field_ids", "template_in_use"]
                payload = {}

                payload['fields'] = []
                payload['title'] = title
                url = self.host + "/ajax/custom_template/"
                headers = self.headers
                headers['Referer'] = url
                #log_me("Putting template {}:{}".format(template, title))
                r = requests.post(url, json=payload, headers=headers, verify=self.verify)
                if r.status_code != 200:
                    raise Exception(r.text)

                t = json.loads(r.text)
                self.existing_templates = self.getTemplates()  # so we don't get a duplicate next time (could be more efficient)
                return t['id']
            elif len(template_exists) == 1:
                return template_exists.iloc[0, 2]  # ID of the template
            else:
                log_me("WARNING -- MULTIPLE TEMPLATES WITH THE SAME NAME")
                log_me(template_exists)
                return template_exists.iloc[0, 6]  # ID of the first template (hopefully only one anyway)

        t_id = template.apply(process_line, axis=1) # result is a list of template IDs (one item)
        if len(fields)>0:
            for t in t_id:
                self.add_customfields_to_template(t, fields)
        return t_id

    # prepare the Articles and upload via Bulk API
    def putArticles(self, article, template_name, custom_fields, bulk="/api/v1/bulk_metadata/custom_fields/"):
        log_me("Putting Articles on Instance")
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
        params=dict(replace_values = True, create_new = True)
        try:
            r = requests.post(url, data=body, headers=self.headers, params=params, verify=self.verify)
            if r.status_code != 200:
                raise Exception(r.text)
            return r
        except IOError as e:
            log_me(u"I/O error({0}): {1}".format(e.errno, e.strerror))
        except ValueError:
            log_me(u"Could not convert data to an integer.")
        except:
            log_me(u"Unexpected error:".format(sys.exc_info()[0]))
            raise


    def getCustomFields_from_template(self, desired_template):
        dt = self.existing_templates[self.existing_templates.title == desired_template]
        # should return a DataFrame with only one row
        CustomFields = pd.DataFrame(dt.iloc[0, 1])  # only look at first row and second column
        if CustomFields.shape[0] == 0:
            return CustomFields
        CustomFields.index = CustomFields.id
        CustomFields.options = CustomFields.options.apply(lambda x: ([y['title'] for y in x]) if x else None)
        # del CustomFields['id'] # for convenience, keep it?
        CustomFields['template_id'] = dt.iloc[0, 2]
        return CustomFields.sort_index()

    def getQueries(self):
        log_me(u"Getting queries")
        url = self.host + u"/api/query/"
        params = dict(limit=1000, saved=True, published=True, deleted=False)
        r = requests.get(url, headers=self.headers, verify=self.verify, params=params)
        queries = pd.DataFrame(json.loads(r.content))
        queries = queries[queries.deleted==False]
        log_me(u"Total queries found: {}".format(queries.shape[0]))
        if 'id' in queries:
            queries = queries.loc[:, [u'id', u'title', u'description', u'published_content', u'ds', u'author']]
            queries.index = queries.id
            return queries.sort_index()
        else:
            return queries

    def putQueries(self, queries):
        ex_queries = self.getQueries()

        url = self.host + u"/api/query/"

        datasource_with_errors = {}

        log_me(u"----- Working on Query Uploads -----")
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
                    #log_me(u"Not updating existing query {}".format(query_id))
                else:
                    body = {}
                    body[u'content'] = single_query.published_content
                    body[u'published_content'] = single_query.published_content
                    if ori_ds_id==1: # Alation Analytics!
                        body[u'ds_id'] = int(aa)
                    elif ori_ds_id==10 and hr: # HR Database
                        body[u'ds_id'] = int(hr)
                    else:
                        if ori_ds_id in datasource_with_errors:
                            pass
                        else:
                            log_me(u"Issue with query...{}".format(single_query.title))
                            log_me(u"No datasource associated with that query!")
                            datasource_with_errors[ori_ds_id] = ori_ds_title
                        continue
                    body[u'title'] = single_query.title
                    if not single_query.description:
                        body[u'description'] = u" ... "
                        log_me(u"Please add a description to query {}".format(single_query.title))
                    else:
                        body[u'description'] = self.fix_refs_2(single_query.description,
                                                               ori_title=single_query.title,
                                                               queries=queries)
                    body[u'published'] = True
                    r = requests.post(url, headers=self.headers, verify=self.verify, json=body)
                    if not r.status_code:
                        log_me(u"Issue with a query upload: {}".format(r.content))
                    else:
                        n = n + 1
        log_me(u"Uploaded ({}) queries.".format(n))

    def getUsers(self):
        log_me("Getting users")
        url = self.host + "/api/user/"
        r = requests.get(url, headers=self.headers, verify=self.verify)
        users = pd.DataFrame(json.loads(r.content))
        return users

    def getDataSources(self):
        url = self.host + "/ajax/datasource/"
        r = requests.get(url, headers=self.headers, verify=self.verify)
        ds = pd.DataFrame(json.loads(r.content))
        log_me(u"Total number of data sources: {}".format(ds.shape[0]))
        return ds


    def look_up_ds_by_name(self, ds_title):
        # look up id of the data source by title
        match = self.ds.title.eq(ds_title)
        if match.any():
            ds_id = self.ds.id[match.idxmax()]
            return ds_id
        else:
            log_me(u"WARNING - No Data Source found for name {}".format(ds_title))
            return

    def mkdir_p(self, path):
        try:
            os.makedirs(path)
        except OSError as exc:  # Python >2.5
            if exc.errno == errno.EEXIST and os.path.isdir(path):
                pass
            else:
                raise

    def getMediaFile(self, media_set, basepath):
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
                    # log_me(u"Good news - no need to download {}".format(filename))
                    pass
                else:
                    log_me(u"Downloading and saving {} -> {}".format(article_id, url))
                    r = requests.get(url, headers=self.headers, verify=self.verify)
                    if r.status_code == 200:
                        with zipfile.ZipFile('ABOK_media_files.zip', 'a') as myzip:
                            myzip.writestr(filename, r.content)
                        existing_files.append(filename)
                    else:
                        log_me(u"WARNING -- NO FILE {}".format(url))

    def fix_children(self, source_articles, template='all'):
        log_me(u"---- Pass 3: Fixing parent-child relationships ----")
        # Let's read all articles again so we get the IDs, too.
        articles_on_target = self.getArticles(template=template)
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
                log_me(u"Skipping to next article in the list - make sure to replicate articles first")
            else:
                target_parent_id = int(target_parent[u'id'])
                target_parent_children = target_parent.at[target_parent_id, 'children']
                if len(target_parent_children) >= 1:
                    target_parent_children_ids = [c['id'] for c in target_parent_children]
                else:
                    target_parent_children_ids = []
                for child in children:
                    try:
                        child_id = child[u'id']
                        child_title = child[u'title']

                        match = articles_on_target.title.eq(child_title)

                        # On target machine
                        target_child = articles_on_target[match]
                        if target_child.empty:
                            log_me(u"No target child: {} -> {}".format(t, child_title))
                        else:
                            target_child_id = target_child.iloc[0, :]['id']
                            if target_child_id not in target_parent_children_ids:
                                new_children.append(target_child_id)
                    except:
                        log_me(u"Child issue: {} -> {}".format(target_parent_id, child_title))
                if len(new_children)>0:
                    log_me(u"Parent: {}/{}, Children: {}".format(id, t, [c['id'] for c in children]))
                    log_me(u"Updating article {}:{} -> {}".format(target_parent_id, t, new_children))
                    # update all children
                    new_article = dict(body=target_parent.loc[target_parent_id, u'body'], title=t, children=
                                       [dict(id=int(new_child), otype="article") for new_child in new_children]
                                       ) # only the required fields...
                    updated_art = self.updateArticle(int(target_parent_id), new_article)
        log_me(u"Finished updating parent-child relationships.")


    def fix_refs(self, template):
        log_me(u"----- Pass 2: Getting all Articles, Queries -----")
        # Get a handle on all the articles on the source instance
        articles = self.getArticles(template=template)
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
                        log_me(u"{} somehow got missed in the pre-processing".format(title))
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
                                # log_me(u"Article match for {} -> {}/{}".format(t, title, oid))
                            else:
                                log_me(u"No article match for {}->{}".format(t, title))
                        except:
                            log_me(u"Exception trying to match {} -> {}".format(t, title))
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
                            log_me(u"No query match for {} -> {}".format(t, title))
                    elif otype == 'table':
                        qual_name = title.split()[0]
                        tb = self.getTablesByName(qual_name)
                        if not tb.empty:
                            # m is a reference to somewhere and we need to fix it.
                            oid = tb.index[-1]
                            m['data-oid'] = oid
                            m['href'] = "/{}/{}/".format(otype, oid)
                            #log_me(u"Link to table: {}".format(m))

                            articles.at[id, 'body'] = soup.prettify()  # update the article body
                            articles.at[id, 'updated'] = True  # update the article body
                            update_needed = True
                        else:
                            log_me(u"No match for {}".format(title))
            if update_needed:
                log_me(u"Updating article {}:{} -> {}".format(id, t, title))
                new_article = dict(body=articles.at[id, 'body'], title=t)  # only the required fields...
                updated_art = self.updateArticle(int(id), new_article)
        log_me(u"Finished preparing references. ")


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
                            log_me(u"No article match for {} -> {}".format(ori_title, title))
                            m['data-oid'] = 0
                            del m['href']
                    except:
                        log_me(u"Exception / no article match for {} -> {}".format(ori_title, title))
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
                        log_me(u"No query match for {} -> {}".format(ori_title, title))
                        m['data-oid'] = 0
                        del m['href']
                elif otype == 'table':
                    qual_name = title.split()[0]
                    tb = self.getTablesByName(qual_name)
                    if not tb.empty:
                        # m is a reference to somewhere and we need to fix it.
                        oid = tb.index[-1]
                        m['data-oid'] = oid
                        m['href'] = "/{}/{}/".format(otype, oid)
                        #log_me(u"Link to table: {}".format(m))
                    else:
                        #log_me(u"No table match for {} -> {}".format(ori_title, title))
                        m['data-oid'] = 0
                        del m['href']
        return soup.prettify()


    def upload_dd(self, dd, ds_id=0, ds_title=u"", articles_created=[]):
        body = ""
        self.articles=pd.DataFrame(articles_created)
        if not ds_id:
            ds_id = self.look_up_ds_by_name(ds_title)
        log_me(u"Data Dictionary Upload for {}/{}".format(ds_id, ds_title))

        if ds_title == "Alation Analytics":
            log_me(u"----- Working on DD description fields -----")
            queries = self.getQueries()
            dd['description'] = dd.description.apply(self.fix_refs_2, queries=queries, ori_title=u"DD")

        for id, obj in dd.iterrows():
            obj['key'] = "{}.{}".format(int(ds_id), obj['key'])
            r = dict(obj)
            body = body + json.dumps(r) + '\n'
        log_me("Uploading data dictionary")
        # Template name needs to be part of the URL
        bulk = "/api/v1/bulk_metadata/custom_fields/"
        template_name = 'default'
        url = self.host + bulk + template_name + "/mixed"
        params=dict(replace_values=False, create_new=True)
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
            log_me(u"I/O error({0}): {1}".format(e.errno, e.strerror))
        except ValueError:
            log_me(u"Could not convert data to an integer.")
        except:
            log_me(u"Unexpected error: {}".format(sys.exc_info()[0]))
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
            log_me(u"I/O error({0}): {1}".format(e.errno, e.strerror))
        except ValueError:
            log_me(u"Could not convert data to an integer.")
        except:
            log_me(u"Unexpected error: {}".format(sys.exc_info()[0]))
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
            log_me(u"I/O error({0}): {1}".format(e.errno, e.strerror))
        except ValueError:
            log_me(u"Could not convert data to an integer.")
        except:
            log_me(u"Unexpected error: {}".format(sys.exc_info()[0]))
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
        if isinstance(t, basestring):
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
            log_me(u"I/O error({0}): {1}".format(e.errno, e.strerror))
        except ValueError:
            log_me(u"Could not convert data to an integer.")
        except:
            log_me(u"Unexpected error: {}".format(sys.exc_info()[0]))
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
