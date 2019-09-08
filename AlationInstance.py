import json
import sys
import time

import pandas as pd
import requests
from urllib2 import urlopen
from urlparse import urlparse

from alationutil import log_me
from pandas.io.json import json_normalize
from os import walk
from os.path import basename

import errno
import os
from Article import Article
from query import *

class AlationInstance():
    def __init__(self, host, email, password, verify=True):
        self.host = host
        self.verify = verify
        self.headers = self.login(email, password)
        log_me("Getting existing custom fields")
        self.existing_fields = self.getCustomFields() # store existing custom fields
        log_me("Getting existing templates")
        self.existing_templates = self.getTemplates() # store existing templates

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
        fields = pd.DataFrame(json.loads(r.content))
        fields.index = fields.id
        return fields.sort_index()

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
                    break
            except:
                break
        return articles
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

    def updateArticle(self, id, article):
        url = self.host + "/integration/v1/article/" + str(id) + "/"
        r = requests.put(url, headers=self.headers, verify=self.verify, json=article)
        # return a dictionary
        art = json.loads(r.content)
        return art

    def download_datadict(self, ds_id):
        url = self.host + "/data/"+str(ds_id)+"/download_dict/data/"+str(ds_id)+"/"
        params = dict(format='json')
        r = requests.get(url, headers=self.headers, verify=self.verify, params=params)
        r_parsed = json.loads(r.content)
        #dd = pd.DataFrame(r_parsed)
        return r_parsed

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
        log_me("Adding fields ({}) to template {}".format(field_ids, template_id))
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
                return field_exists.iloc[0, 6]  # ID of the field
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
            log_me("I/O error({0}): {1}".format(e.errno, e.strerror))
        except ValueError:
            log_me("Could not convert data to an integer.")
        except:
            log_me("Unexpected error:", sys.exc_info()[0])
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

    def getQueries(self, ds_id):
        log_me(u"Getting queries")
        url = self.host + u"/api/query/"
        params = dict(limit=1000, datasource_id=ds_id, saved=True, published=True)
        r = requests.get(url, headers=self.headers, verify=self.verify, params=params)
        queries = pd.DataFrame(json.loads(r.content))
        queries = queries.loc[:, [u'title', u'description', u'published_content']]
        return queries.sort_index()

    def putQueries_alt(self, ds_id, queries):
        # -- just in case, grab an API token as well
        token = u'...'
        log_me(u"Putting queries")
        url = self.host + u"/integration/v1/query/"
        for single_query in queries.itertuples():
            body={}
            body[u'content'] = u'-- Title: ' + single_query.title + u'\n' + single_query.published_content
            body[u'datasource_id'] = ds_id
            r = requests.post(url, headers=dict(token=token), verify=self.verify, json=body)

    def putQueries(self, ds_id, queries):
        log_me(u"Putting queries")
        url = self.host + u"/api/query/"
        for single_query in queries.itertuples():
            log_me(single_query.title)
            body={}
            body[u'content'] = single_query.published_content
            body[u'ds_id'] = ds_id
            body[u'title'] = single_query.title
            body[u'description'] = single_query.description
            # body[u'published'] = True
            r = requests.post(url, headers=self.headers, verify=self.verify, json=body)
            h = generate_html(single_query)

    def getUsers(self):
        log_me("Getting users")
        url = self.host + "/api/user/"
        r = requests.get(url, headers=self.headers, verify=self.verify)
        users = pd.DataFrame(json.loads(r.content))
        return users


    def mkdir_p(self, path):
        try:
            os.makedirs(path)
        except OSError as exc:  # Python >2.5
            if exc.errno == errno.EEXIST and os.path.isdir(path):
                pass
            else:
                raise

    def getMediaFile(self, media_set, dir):
        # create the download directory if it doesn't exist already
        self.mkdir_p(dir+'/media/image_bank')
        # get a list of existing files
        (_, _, existing_files) = walk(dir+'/media/image_bank').next()
        for filename in media_set:
            try:
                #log_me(filename)
                url = urlopen(filename)
                filename = urlparse(filename).path
            except ValueError:  # invalid URL - that means it's only a path
                url = self.host + filename
            except:
                # sometimes we get Error 403 (unauth.), no problem
                url = filename
                filename = urlparse(filename).path
            if basename(filename) in existing_files:
                #log_me(u"Good news - no need to download {}".format(filename))
                pass
            else:
                log_me(u"Downloading and saving {}".format(url))
                r = requests.get(url, headers=self.headers, verify=self.verify)
                if r.status_code != 200:
                    raise Exception(r.text)
                open(dir + filename, 'wb').write(r.content)

    def fix_children(self, source_articles):
        # Let's read all articles again so we get the IDs, too.
        new_Article = self.getArticles()
        # iterate through all articles with children
        art_with_children = source_articles[source_articles.has_children]
        for a in art_with_children.itertuples():
            # a is a parent
            t = a.title
            id = a.id
            children = a.children
            # see if the article exists on the target and what its ID is
            target_parent = new_Article[new_Article.title==t]
            new_children = []
            if target_parent.empty:
                log_me("Skipping to next article in the list - make sure to replicate articles first")
            else:
                target_parent_id = int(target_parent[u'id'])
                for child in children:
                    child_id = child[u'id']
                    child_title = child[u'title']
                    # On target machine
                    target_child = new_Article[new_Article.title==child_title]
                    if target_child.empty:
                        log_me("No target child: ".format(child_title))
                    else:
                        target_child_id = target_child.iloc[0, :]['id']
                        new_children.append(target_child_id)
                new_article = dict(body=target_parent.loc[target_parent_id, u'body'], title=t, children=
                                   [dict(id=new_child, otype="article") for new_child in new_children]
                                   ) # only the required fields...
                log_me(u"Updating article {}:{}->{}".format(id, t, new_children))
                updated_art = self.updateArticle(target_parent_id, new_article)


    def fix_refs(self):
        log_me("Pass 2: Getting all Articles on Target")
        articles = self.getArticles()
        articles['updated'] = False
        titles = articles.title

        for a in articles.itertuples():
            t = a.title
            id = a.id
            soup = BeautifulSoup(a.body, "html5lib")
            update_needed = False
            # Find all Anchors
            match = soup.findAll('a')
            for m in match:
                # We only care about Alation anchors, identified by the attr data-oid
                if 'data-oid' in m.attrs:
                    oid = m['data-oid']
                    otype = m['data-otype']
                    if 'title' in m.attrs:
                        title = m['title']
                    else:
                        title = m.get_text()
                    # For the moment, we only implement references to Articles
                    if otype == 'article':
                        art_match = articles.title==title
                        if art_match.any():
                            matching_articles = articles[art_match]
                            if matching_articles.shape[0] == 0:
                                log_me("No match for {}".format(title))
                            #elif matching_articles.shape[0] > 1:
                            #    log_me("More than one match for {}".format(m))
                            else:
                                # m is a reference to somewhere and we need to fix it.
                                oid = (matching_articles.iloc[0]).id
                                m['data-oid'] = oid
                                m['href'] = "/{}/{}/".format(otype, oid)

                                articles.at[id, 'body'] = soup.prettify()  # update the article body
                                articles.at[id, 'updated'] = True  # update the article body
                                update_needed = True

                        else:
                            log_me("No match for {}".format(title))
            if update_needed:
                new_article = dict(body=articles.at[id, 'body'], title=t)  # only the required fields...
                log_me(u"Updating article {}:{}->{}".format(id, t, title))
                try:
                    updated_art = self.updateArticle(id, new_article)
                    #log_me(updated_art)
                except:
                    log_me("UNSUCCESSFUL: {}/{}".format(id, new_article))
