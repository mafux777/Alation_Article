# coding=utf-8
#********************************************************************************************************
#
# Purpose:
# Usage:
# Output:
#
#********************************************************************************************************

import requests
import json
import unicodedata
from bs4 import BeautifulSoup
import re
import sys
import argparse
import pdfkit
from alationutil import *

def generate_html(queries):
    html = u""
    for single_query in queries.itertuples():
        title = u"<h1> " + single_query.title + u"</h1>\n"
        description = u'<p>' + single_query.description + u'</p>'
        body = u'<p class="ace_static_highlight">' + single_query.published_content
        html = html + title + description + body
    html = html.replace(u'\n', u'</p><p class="ace_static_highlight">')
    soup = BeautifulSoup(html, "html5lib")
    html = soup.prettify()
    return html
