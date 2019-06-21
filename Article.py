import json
import re

import pandas as pd

from alationutil import unpack_id, unpack_title, log_me


class Article():
    # This is the constructor
    # Assigns the ID to the index
    # Flattens the custom templates field
    # Flattens the custom fields -- TODO
    def __init__(self, article): # right now expects a DataFrame - but should also work if an Article is passed
        self.references = [] # store references to other Alation objects
        # then you can chain commands
        if 'id' in article:
            article.index = article.id
        # flatten the template field into id and title
        if 'custom_templates' in article:
            article['template_id']    = article.custom_templates.map(unpack_id)
            article['template_title'] = article.custom_templates.map(unpack_title)
            del article['custom_templates']
        if 'custom_fields' in article:
            def extract_custom(art): # article is now one dict per article
                f = art['custom_fields'] # f is now a list with only the custom fields (each is a dict)
                for i in f: # create a new key for custom field
                    if i['value_type'] in ['picker', 'multi_picker', 'date', 'rich_text']:
                        art[i['field_name']] = i['value']
                    else:
                        art[i['value_type'] + ':' + i['field_name']] = i['value']
                    # assumes each custom field only appears once in each article
                return art

            self.article = article.apply(extract_custom, axis=1)
            del self.article['custom_fields']
        else:
            self.article = article


    # returns all articles with the desired template name (may not work for multiple templates)
    def filter(self, template):
        return Article(self.article[self.article.template_title == template])

    # converts the Article into a JSON accepted by the Bulk API
    # mostly removes unwanted fields
    # encapsulates the custom fields (tricky part)
    def bulk_api_body(self, custom_fields): # expects a DataFrame with relevant custom fields
        keep = ['key', 'description']
        # we need this to construct the JSON properly
        # remember the user needs to create the custom fields first, anyway
        body = self.article.copy()
        body['description'] = body.body
        body['key'] = body.title
        # Do something to change the col names for picker, multi-picker, and date
        custom_fields.index = custom_fields.name_singular # we go by field name
        for obj_set_field in custom_fields.index: # go through all object set type fields by name

            def elim_nulls(f):
                if isinstance(f, float):
                    return
                elif f=='nan':
                    return
                elif f:
                    return f
            #try:
            if obj_set_field in body:
                body[obj_set_field] = body[obj_set_field].apply(elim_nulls)
                keep.append(obj_set_field)
            else:
                log_me("Error eliminating null values for {}".format(obj_set_field))

        # The "normal" custom fields are already taken care of
        # Now we just need to massage the values of the special fields.
        # The issue is that we may not have enough info to create the full references needed
        special_fields = custom_fields[custom_fields.allowed_otypes.notna()]
        special_fields.index = special_fields.name_singular # we go by field name
        for obj_set_field in special_fields.index: # go through all object set type fields by name
            allowed_otpyes = special_fields.loc[obj_set_field, 'allowed_otypes'] # could be 'article', 'table'

            def add_otype(field_value, obj_typ):
                if not isinstance(field_value, float):
                    return dict(type=obj_typ, value=field_value)
                else:
                    return
            # note -- may have to write more code to allow for lists
            for ao in allowed_otpyes:  # go through all allowed object types
                # note the following code will overwrite the value if the loop executes more than once
                try:
                    z = ao + ":" + obj_set_field
                except:
                    z = ao[0] + ":" + obj_set_field
                if z in body:
                    body[obj_set_field] = body[z].apply(add_otype, obj_typ=ao)
                    # still need to delete the field with the colon
                    del body[z]
                # else:
                #     log_me("No field called {}".format(z))

        return body.loc[:, keep].apply(lambda row: json.dumps(dict(row)) + '\n', axis=1)

    # Obsolete function ----
    # def from_csv(self, filename, encoding='utf-8'):
    #     self.article=pd.read_csv(filename, encoding=encoding)
    #     pass

    def to_csv(self, filename, encoding='utf-8'):
        csv = self.article.copy() # should be a copy
        #csv.body.apply(lambda x: x.replace("\\n", "\n"))
        #csv['Summary'].apply(lambda x: x.replace("\\n", "\n"))
        #del csv['id'] # the ID is meaningless when uploading, so let's not pretend
        # the ID gets added to the CSV because it is also the index
        #del csv['attachments'] # unfortunately not supported yet by this script
        #csv.author=csv.author.apply(get_user)
        #body,
        #children,
        #del csv['custom_fields'] # user should have already flattened them
        #csv['editors']=csv.editors.apply(get_users)
        #has_children,
        #id,
        #private,
        #title,
        #ts_created,
        #ts_updated,
        #url,
        #template_id,
        #template_title
        #def flatten_custom_fields(fields):
        #    for f in fields
        csv.to_csv(filename, encoding=encoding)
    def head(self):
        print self.article.head()

    def get_references_old(self):
        def find_refs(x):
            m=re.findall('<a \w+-oid="(\d+)" \w+-otype="(\w+)" href="\/\w+\/\d+\/">([a-zA-Z0-9 \(\)._]+)<\/a>', x, flags=0)
            r={}
            for n in m:
                r["{}/{}".format(n[1], n[0])] = n[2] # a key-value pair, e.g. article/27 = "Interesting Article"
            return r
        match = self.article.body.apply(find_refs)
        return match # return a DataSeries indexed by source article ID, each element a list of references


    def get_files(self):
        match = self.article.body.apply(lambda x: re.findall(
            '<img class=\"([/a-z -_0-9]*)\" +src=\"([/a-z -_0-9]*)\" +style=\"width: ([/a-z -_0-9]*)\">', x, flags=0))
        unique_list_files = set()
        for i, m in match.iteritems():
            for n in m:
                relative_url = n[1].replace("https://abok.alationproserv.com", "")
                #log_me("{}:{}".format(i, relative_url))
                unique_list_files.add(relative_url)
        return unique_list_files

    def get_references(self):
        for i, row in self.article.iterrows():
            m = re.findall('<a \w+-oid="(\d+)" \w+-otype="(\w+)" href="\/\w+\/\d+\/">([a-zA-Z0-9 \(\)._]+)<\/a>', row.body,
                           flags=0)
            r = {}
            for n in m:
                url = "{}/{}".format(n[1], n[0])
                r[url] = n[2]  # a key-value pair, e.g. article/27 = "Interesting Article"
                self.references.append({i:url})
                log_me("Found a reference to {} in Article {}({})".format(url, i, row.title))
        return self.references # return a DataSeries indexed by source article ID, each element a list of references
