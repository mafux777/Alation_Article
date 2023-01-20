import pandas as pd
import datetime
# from abok import abok
import json
from bs4 import BeautifulSoup

from sudoku.alationutil import log_me, unpack_id, unpack_title, unpack_children
from collections import deque

# import pdfkit

# The Article class is a DataFrame of Articles with some additional functionality
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
                for field in art['custom_fields']:
                    # create a new key for custom field
                    # the value type is not needed for (multi-)picker, date and rich text
                    # this will be one column with the value
                    if field['value_type'] in ['picker', 'multi_picker', 'date', 'rich_text']:
                        art[field['field_name']] = field['value']
                    else:
                        # This will be two columns, one with the type, and one with the value
                        art[field['field_name'] + ':' + 'type'] = field['value_type']
                        art[field['field_name'] + ':' + 'key' ] = field['value']
                    # assumes each custom field only appears once in each article
                return art

            self.article = article.apply(extract_custom, axis=1)
            #del self.article['custom_fields']
        else:
            self.article = article

    def get_df(self):
        return self.article

    # returns all articles with the desired template name (may not work for multiple templates)
    def filter(self, template):
        return Article(self.article[self.article.template_title == template])

    # converts the Article into a JSON accepted by the Bulk API
    # encapsulates the custom fields (tricky part)
    # this code does not take advantage of the flattening that happened during construction time
    # instead, we use the original contents of the custom fields column
    def bulk_api_body(self):
        log_me("Creating Body for Bulk API")
        body = ""
        # Iterate through all the articles
        for id, article in self.article.iterrows():
            new_row = dict(description=article['body'], key=article['title'])
            # Iterate through the custom fields (caller could have sent fewer)
            for field in article['custom_fields']:
                if field['value_type'] in ['picker', 'multi_picker', 'date', 'rich_text']:
                    new_row[field['field_name']] = field['value']
                else:
                    # In the case of Object Sets and People Sets, this may not be any good
                    log_me(f"Warning: {field['field_name']}/{field['value_type']}/{field['value']}")
                    new_row[field['field_name']] = {field['value_type'] : field['value']}
            body = body + json.dumps(new_row) + '\n'
        return body


    def to_csv(self, filename, encoding='utf-8'):
        #log_me(u"Creating file: {}".format(filename))
        csv = self.article.copy() # should be a copy
        del csv['id'] # the ID is meaningless when uploading, so let's not pretend
        csv.index=csv['title']
        # the ID gets added to the CSV because it is also the index
        del csv['attachments'] # unfortunately not supported yet by this script
        del csv['custom_fields'] # user should have already flattened them
        del csv['has_children']
        del csv['author']
        del csv['editors']
        del csv['private']
        del csv['ts_created']
        del csv['ts_updated']
        del csv['template_id']
        del csv['url']
        #del csv['references']
        csv['template_name'] = csv.template_title
        csv['key'] = csv.title
        csv['description'] = csv.body
        del csv['template_name']
        del csv['key']
        del csv['description']
        csv['object_type']='article'
        csv['create_new']='Yes'
        csv['tags']='migrated'
        csv.to_csv(filename, encoding=encoding)

    def head(self):
        print (self.article.head())

    def convert_references(self):
        # First pass: create a DataFrame of target articles with
        # New articles that are being migrated or referenced
        # All references to articles "zero-ed out" - will be re-calculated in Second Pass
        # The title gets saved in the title attribute of the anchor (safer)
        for a in self.article.itertuples():
            soup = BeautifulSoup(a.body, "html5lib")
            # Find all Anchors
            match = soup.findAll('a')
            for m in match:
                # We only care about Alation anchors, identified by the attr data-oid
                if 'data-oid' in m.attrs:
                    oid=m['data-oid']
                    otype=m['data-otype']
                    if otype=='article':
                        try:
                            actual_title = self.article.at[int(oid), 'title']
                        except:
                            log_me(u"Warning! Ref to article not found {}->{}".format(a.title, m.get_text()))
                            actual_title=m.get_text()
                        m.string = actual_title
                        m['data-oid'] = 0
                        del m['href']
                        m['title'] = actual_title
                        self.article.at[a.Index, 'body'] = soup.prettify() # update the article body
                    else:
                        #log_me(m)
                        m['data-oid'] = 0
                        del m['href']
                        m['title'] = m.get_text()
                        self.article.at[a.Index, 'body'] = soup.prettify() # update the article body
                # else:
                #     try:
                #         log_me(u"External link: {} -> {}".format(a.id, m))
                #     except:
                #         log_me(u"Formatting issue {}".format(a.id))





    # iterate through the bodies of all articles and return a tuple with article ID and media file
    def get_files(self):
        src=[]
        for ind, a in self.article.body.iteritems():
            soup = BeautifulSoup(a, "html5lib")
            images = soup.findAll('img')
            if images:
                src.append([(ind, i['src']) for i in images])
        return src


    def get_article_by_name(self, name):
        match = self.article[self.article.title == name]
        if not match.shape[0]:
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
            'header-left': 'Alation Book of Knowledge' + now.strftime(u" %Y-%m-%d %H:%M "),
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
        ABOKpdffilename = 'ABOK' + now.strftime(u" %Y-%b-%d %H_%M ") + '.pdf'
        seq = self.check_sequence(first)
        html = '<meta http-equiv="Content-Type" content="text/html; charset=utf-8">' +\
               '<link rel="stylesheet" href="https://use.typekit.net/pno7yrt.css">' +\
               '<link href="alation.css" rel="stylesheet" type="text/css">'
        for i in seq:
            html = html + '<h1>' + self.article.title[i] + '</h1></p>'
            html = html + self.article.body[i] + '</p>'
        html2 = abok.clean_up(html)
        html2 = html2 + additional_html
        pdfkit.from_string(html2, ABOKpdffilename, options=bodyoptions, css="css/alation.css")
        log_me('pdfkit finished processing')

    # the following code could be made more elegant if we can guarantee that there are no orphans in the
    # list, i.e. all articles have a parent, except obviously the first (=top)
    def check_sequence(self, first):
        # first is the ID of the first article (top of the hierarchy)
        # it will be the last to be created on the target...
        # we need to put the articles in a logical order.
        # we put the first in front, but we expect it to be pushed all the way to the last when we are done
        order = deque([first])
        # the to-do-list is all articles without the first
        to_do_list = deque(self.article.index)
        to_do_list.remove(first) # we have taken care of the first already
        while to_do_list:
            # get the right most item
            last = order[-1]
            # we either remove a child or the next in the to-do list
            # do we have children?
            current_children = deque(self.article.children[last])
            while current_children:
                c = current_children.pop()
                try:
                    # move to the top of the to-do list
                    to_do_list.remove(c['id'])
                    to_do_list.appendleft(c['id'])
                except:
                    log_me(f"WARNING --- Article {c['id']}/{c['title']} does not appear to be loaded.")
            order.append(to_do_list.popleft()) # next one
        return order