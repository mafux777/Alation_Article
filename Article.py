import json
import re

import pandas as pd
import datetime
import os
import pdfkit
import abok
from bs4 import BeautifulSoup

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
        log_me("Creating Body for Bulk API")
        body = ""
        for id, article in self.article.iterrows():
            d = article['body']
            k = article['title']
            #log_me(u"Working on {}/{}".format(d, k))
            new_row = dict(description=d, key=k)
            new_row[u'Migration Notes'] = u"<ul>"
            # loop thru articles 1 by 1
            for j, field in custom_fields.iterrows():
                #log_me(u"Looping through...{}/{}".format(j, field))
                # loop thru all fields we expect
                f = article['custom_fields'] # f is now a list with only the custom fields (each is a dict)
                if field['allow_multiple']:
                    name = field['name_singular']
                    new_row[name] = []
                    if field['allowed_otypes']:
                        for t in field['allowed_otypes']:
                            #log_me(u"Working on field with otypes {}".format(t))
                            for f0 in f:
                                if f0['field_name'] == field['name_singular'] and f0['value_type'] == t:
                                    #new_row[name].append(dict(type=t, key=f0['value']))
                                    new_row[u'Migration Notes'] = new_row[u'Migration Notes'] + \
                                        u"<li>Manually add {}={}:{} from source article {}</li>\n".format(name, t, f0['value'], id)
                    else:
                        #log_me(u"Working on multi-field {}".format(t))
                        for f0 in f:
                            if f0['field_name'] == field['name_singular'] and f0['value_type'] == t:
                                # new_row[name].append(dict(type=t, key=f0['value']))
                                new_row[u'Migration Notes'] = new_row[u'Migration Notes'] + \
                                                              u"<li>Manually add {}={}:{} from source article {}</li>\n".format(
                                                                  name, t, f0['value'], id)
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
        #log_me(u"Creating file: {}".format(filename))
        csv = self.article.copy() # should be a copy
        del csv[u'id'] # the ID is meaningless when uploading, so let's not pretend
        csv.index=csv[u'title']
        # the ID gets added to the CSV because it is also the index
        del csv[u'attachments'] # unfortunately not supported yet by this script
        del csv[u'custom_fields'] # user should have already flattened them
        del csv[u'has_children']
        del csv[u'author']
        del csv[u'editors']
        del csv[u'private']
        del csv[u'ts_created']
        del csv[u'ts_updated']
        del csv[u'template_id']
        del csv[u'url']
        del csv[u'references']
        csv[u'template_name'] = csv.template_title
        csv[u'key'] = csv.title
        csv[u'description'] = csv.body
        del csv[u'template_name']
        del csv[u'key']
        del csv[u'description']
        # csv = csv.rename(index=str, columns={
        #     u"template_title": u"template_name",
        #     u"title":u"key",
        #     u"body":u"description"
        # })
        csv[u'object_type']=u'article'
        csv[u'create_new']=u'Yes'
        csv[u'tags']=u'migrated'
        csv.to_csv(filename, encoding=encoding)

    def head(self):
        print self.article.head()

    def convert_references(self):
        # First pass: create a DataFrame of target articles with
        # New articles that are being migrated or referenced
        # All references to articles "zero-ed out" - will be re-calculated in Second Pass
        for a in self.article.itertuples():
            soup = BeautifulSoup(a.body, "html5lib")
            # Find all Anchors
            match = soup.findAll('a')
            for m in match:
                # We only care about Alation anchors, identified by the attr data-oid
                if 'data-oid' in m.attrs:
                    oid=m['data-oid']
                    otype=m['data-otype']
                    # For the moment, we only implement references to Articles
                    if otype=='article':
                        try:
                            actual_title = self.article.at[int(oid), 'title']
                        except:
                            log_me("Warning! Ref to article not found {}->{}".format(a.title, m))
                            actual_title=m.get_text()
                        m.string = actual_title
                        m['data-oid'] = 0
                        del m['href']
                        m['title'] = actual_title
                        self.article.at[a.Index, 'body'] = soup.prettify() # update the article body
                    # elif otype=='query':
                    #     m['data-oid'] = 0
                    #     del m['href']
                    #     m['title'] = m.get_text()
                    #     self.article.at[a.Index, 'body'] = soup.prettify() # update the article body
                    else:
                        log_me(m)
                        m['data-oid'] = 0
                        del m['href']
                        m['title'] = m.get_text()
                        self.article.at[a.Index, 'body'] = soup.prettify() # update the article body
                else:
                    log_me(u"External link: {} -> {}".format(a.id, m))


    def get_files(self):
        soup = BeautifulSoup(self.article.body.sum(), "html5lib")
        images = soup.findAll('img')
        src = [i['src'] for i in images]
        return set(src)

    def get_files_old(self):
        match = self.article.body.apply(lambda x: re.findall(
            '<img class=\"([/a-z -_0-9]*)\" +src=\"([/a-z -_0-9]*)\" +style=\"width: ([/a-z -_0-9]*)\">', x, flags=0))
        unique_list_files = set()
        for i, m in match.iteritems():
            for n in m:
                relative_url = n[1].replace("https://abok.alationproserv.com", "") # n[1] previously
                #log_me("{}:{}".format(i, relative_url))
                unique_list_files.add(relative_url)
        return unique_list_files


    def get_article_by_name(self, name):
        match = self.article[self.article.title==name]
        if match.shape[0] == 0:
            return None
        else:
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

    def create_pdf(self, first, additional_html=''):
        now = datetime.datetime.now()
        # Use pdfkit to create final ABOK pdf file
        # Options for PDFKit (wkhtmltopdf really) to generate the pdf - https://wkhtmltopdf.org/usage/wkhtmltopdf.txt
        bodyoptions = {
            'page-size': 'Letter',
            'footer-line': '',
            'footer-center': 'For use only by Alation customers.  No duplication or transmission without permission.',
            'footer-font-size': '9',
            #'disable-internal-links': True,
            #'disable-external-links': True,
            'dpi': '300',
            'minimum-font-size': '12',
            'disable-smart-shrinking': '',
            'header-left': u'Alation Book of Knowledge (Draft)' + now.strftime(u" %Y-%m-%d %H:%M "),
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
        ABOKpdffilename = u'Draft ABOK' + now.strftime(u" %Y-%b-%d %H_%M ") + u'.pdf'
        seq = self.check_sequence(first)
        html = u'<meta http-equiv="Content-Type" content="text/html; charset=utf-8">' +\
               u'<link rel="stylesheet" href="https://use.typekit.net/pno7yrt.css">' +\
               u'<link href="alation.css" rel="stylesheet" type="text/css">'
        for i in seq:
            html = html + u'<h1>' + self.article.title[i] + u'</h1></p>'
            html = html + self.article.body[i] + u'</p>'
        html2 = abok.clean_up(html)
        html2 = html2 + additional_html
        pdfkit.from_string(html2, ABOKpdffilename, options=bodyoptions, css="alation.css", cover='cover.html', cover_first=True)
        log_me(u'pdfkit finished processing')

    def check_sequence(self, first):
        # we need to put the articles in a logical order.
        # we put the first in front
        # log_me(u"First is {}/{}".format(
        #     self.article.id[first],
        #     self.article.title[first]))
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
                    # move to the top of the to-do list
                    to_do_list.remove(c['id'])
                    to_do_list.appendleft(c['id'])
                except:
                    log_me(u"WARNING --- Article {}/{} does not appear to be loaded.".format(c['id'], c['title']))
            next = to_do_list.popleft()
            order.append(next) # next one
            # log_me(u"Next is {}/{}".format(
            #     self.article.id[next],
            #     self.article.title[next]
            # ))
        return order