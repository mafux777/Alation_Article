import json
import re

import pandas as pd
import datetime
import os
import pdfkit
import abok

from alationutil import *
from collections import OrderedDict, deque


class Article():
    # This is the constructor
    # Assigns the ID to the index
    # Flattens the custom templates field
    def __init__(self, article):
        # right now expects a DataFrame - but should also work if an Article is passed
        # then you can chain commands
        if 'id' in article:
            article.index = article.id
        # flatten the template field into id and title
        if 'custom_templates' in article:
            article['template_id']    = article.custom_templates.map(unpack_id)
            article['template_title'] = article.custom_templates.map(unpack_title)
            del article['custom_templates']
        if 'children' in article:
            article['child_id']    = article.children.map(unpack_children)
            #article['template_title'] = article.custom_templates.map(unpack_title)
            #del article['children']
        if 'custom_fields' in article:
            def extract_custom(art): # article is now one dict per article
                f = art['custom_fields'] # f is now a list with only the custom fields (each is a dict)
                for i in f:
                    # create a new key for custom field
                    if i['value_type'] in ['picker', 'multi_picker', 'date', 'rich_text']:
                        art[i['field_name']] = i['value']
                    else:
                        art[i['field_name'] + ':' + 'type'] = i['value_type']
                        art[i['field_name'] + ':' + 'key' ] = i['value']
                    # assumes each custom field only appears once in each article
                return art

            self.article = article.apply(extract_custom, axis=1)
            #del self.article['custom_fields']
        else:
            self.article = article


    # returns all articles with the desired template name (may not work for multiple templates)
    def filter(self, template):
        return Article(self.article[self.article.template_title == template])

    # converts the Article into a JSON accepted by the Bulk API
    # encapsulates the custom fields (tricky part)
    def bulk_api_body(self, custom_fields=pd.DataFrame()): # expects a DataFrame with relevant custom fields
        body = ""
        for id, article in self.article.iterrows():
            d = article['body']
            k = article['title']
            new_row = dict(description=d, key=k)
            new_row[u'Migration Notes'] = u"<ul>"
            # loop thru articles 1 by 1
            for j, field in custom_fields.iterrows():
                # loop thru all fields we expect
                f = article['custom_fields'] # f is now a list with only the custom fields (each is a dict)
                if field['allow_multiple']:
                    name = field['name_singular']
                    new_row[name] = []
                    for t in field['allowed_otypes']:
                        for f0 in f:
                            if f0['field_name'] == field['name_singular'] and f0['value_type'] == t:
                                #new_row[name].append(dict(type=t, key=f0['value']))
                                new_row[u'Migration Notes'] = new_row[u'Migration Notes'] + \
                                    u"<li>Manually add {}={}:{} from source article {}</li>\n".format(name, t, f0['value'], id)
                else:
                    name = field['name_singular']
                    for f0 in f:
                        if f0['field_name'] == field['name_singular']:
                            new_row[name] = f0['value']
            new_row[u'Migration Notes'] = new_row[u'Migration Notes'] + u"\n</ul>"
            body = body + json.dumps(new_row) + '\n'
        return body

    # Obsolete function ----
    # def from_csv(self, filename, encoding='utf-8'):
    #     self.article=pd.read_csv(filename, encoding=encoding)
    #     pass

    def to_csv(self, filename, encoding='utf-8'):
        csv = self.article.copy() # should be a copy
        #csv.body.apply(lambda x: x.replace("\\n", "\n"))
        #csv['Summary'].apply(lambda x: x.replace("\\n", "\n"))
        del csv['id'] # the ID is meaningless when uploading, so let's not pretend
        csv.index=csv['title']
        # the ID gets added to the CSV because it is also the index
        del csv['attachments'] # unfortunately not supported yet by this script
        #csv.author=csv.author.apply(get_user)
        #body,
        #children,
        del csv['custom_fields'] # user should have already flattened them
        #csv['editors']=csv.editors.apply(get_users)
        del csv['has_children']
        del csv['author']
        del csv['editors']
        #del csv['id']
        del csv['private']
        del csv['ts_created']
        del csv['ts_updated']
        del csv['template_id']
        del csv['url']
        del csv['references']
        csv = csv.rename(index=str, columns={
            "template_title": "template_name",
            "title":"key",
            "body":"description"
        })
        csv['object_type']='article'
        csv['create_new']='Yes'
        csv['tags']='migrated'
        csv.to_csv(filename, encoding=encoding)
    def head(self):
        print self.article.head()



    def get_references(self):
        match = self.article.body.apply(lambda x:
            re.finditer(
                r'<a \w+-oid=\"(\d+)\" \w+-otype=\"(\w+)\" href=\"[^>]+\">([^>]+)<\/a>',
                x, flags=0))
        self.article['references'] = match
        return match # return a DataSeries indexed by source article ID, each element an iterator of MatchObjects


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


    def get_article_by_name(self, name):
        match = self.article[self.article.title==name]
        if match.shape[0] == 0:
            log_me("Could not find article with the name '{}'".format(name))
        return match

    def get_users(self):
        authors = list(self.article.author)
        editors = list(self.article.editors)
        users = authors
        for ed in editors:
            for e in ed:
                users.append(e)
        users_pd = pd.DataFrame(users).drop_duplicates()
        users_pd.index = users_pd.id
        return users_pd

    def create_pdf(self):
        # Use pdfkit to create final ABOK pdf file
        # Options for PDFKit (wkhtmltopdf really) to generate the pdf - https://wkhtmltopdf.org/usage/wkhtmltopdf.txt
        bodyoptions = {
            'page-size': 'Letter',
            'footer-line': '',
            'footer-center': 'For use only by Alation customers.  No duplication or transimission without permission is prohibited.',
            'footer-font-size': '9',
            'disable-internal-links': '',
            'disable-external-links': '',
            'dpi': '300',
            'minimum-font-size': '12',
            'disable-smart-shrinking': '',
            'header-left': 'Alation Book of Knowledge',
            'header-line': '',
            'header-font-size': '9',
            'header-spacing': '4',
            'margin-bottom': '15',
            'margin-top': '15',
            'footer-spacing': '4',
            'margin-left': '10',
            'margin-right': '10',
            'footer-right': '[page]/[toPage]',
            'enable-toc-back-links': '',
            'outline': '',
            'quiet': ''
        }
        # Define the location of the created ABOK pdf file
        ABOKpdffilename = u'ABOK' + datetime.datetime.now().strftime(u" %Y-%b-%d %H:%M ") + u'.pdf'
        seq = self.check_sequence(51)
        html = u""
        for i in seq:
            html = html + u'<h1 style="font-family:arial;font-size:14px;">' + self.article.title[i] + u'</h1></p>'
            html = html + self.article.body[i] + u'</p>'
        html2 = abok.clean_up(html)
        pdfkit.from_string(html2, ABOKpdffilename, options=bodyoptions, cover='cover.html', cover_first=True)
        log_me(u'Finished processing')

    def check_sequence(self, first):
        # we need to put the articles in a logical order.
        # we put the first in front
        log_me(u"First is {}/{}".format(
            self.article.id[first],
            self.article.title[first]))
        order = deque([first])
        # the to-do-list is all articles without the first
        to_do_list = deque(self.article.index)
        to_do_list.remove(first)
        #next item should be a child or the next in the to-do-list
        while(to_do_list):
            # get the right most item
            last = order[-1]
            # we either remove a child or the next in the to-do list
            # do we have children?
            current_children = deque(self.article.children[last])
            while(current_children):
                c = current_children.pop()
                try:
                    to_do_list.remove(c['id'])
                    to_do_list.appendleft(c['id'])
                except:
                    log_me(u"Article {}/{} does not appear to be loaded.".format(c['id'], c['title']))
            next = to_do_list.popleft()
            order.append(next) # next one
            log_me(u"Next is {}/{}".format(
                self.article.id[next],
                self.article.title[next]
            ))
        return order