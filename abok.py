# coding=utf-8
#********************************************************************************************************
#
# Purpose: Generate ABOK pdf file from the Alation ABOK instance
# Usage: Run manually from the command line
# Output:  It produces an ABOK PDF file with a datetime stamp
#
#********************************************************************************************************

import requests
import json
import unicodedata
from bs4 import BeautifulSoup
import re
import sys
import argparse
#import pdfkit
from alationutil import *

def clean_up(html):
    soup = BeautifulSoup(html, "html5lib")

    #Remove all hrefs using Beautiful soup so dead links are not in the pdf
    links = soup.findAll('a')
    for a in links:
        # Let's remove all hyperlinks to Alation Objects only
        if u"data-otype" in a.attrs:
            if a.string:
                # "m" is a convenience tag which has no effect
                n = a.string.wrap(soup.new_tag("m"))
                a.replace_with(n)

    # images = soup.findAll('img')
    # for i in images:
    #     a = i.attrs
    #     if u'style' in a:
    #         width = re.search(r'width: ([0-9]+)px', a[u'style'], re.UNICODE)
    #         if width:
    #             w = int(width.group(1))
    #             w_max = 800
    #             if w>w_max:
    #                 log_me(u'We have an issue with {} - forcing to {}'.format(i, w_max))
    #                 a[u'style'] = u"width: {}px".format(w_max)


    #Save back from soup object to string and encode back to unicode because pdfkit needs unicode
    html = soup.prettify()

    #// Replace img paths to point to the local image location
    html = html.replace('/media','/Users/matthias.funke/Downloads/media')
    html = html.replace('https://abok.alationproserv.com/media/','/Users/matthias.funke/Downloads/media/')

    # Find some strange chars that happen after . and replace with a single space
    m5 = re.findall(pattern=u'([.]+)([\s]{2})([a-zA-Z]+)',string=html,flags=re.UNICODE)
    m6 = list(set([m[1] for m in m5]))
    for m in m6:
        html = re.sub(pattern=m, repl=u' ', string=html, count=0, flags=re.UNICODE)
    # Fix a funny char and replace with normal apostrophe
    html = re.sub(pattern=u'\u2019', repl=u"'", string=html, count=0, flags=re.UNICODE)
    #Point to cover html file
    cover = 'cover.html'

    return html

from collections import deque

output = deque([])

def recursive_child_seek(article, id):
    # we know the first time we have children, guranteed
    # we want to go down to the childless and take them off the list
    # and add them to a final result
    if article.has_children[id]:
        current_children = article.children[id]
        for c in current_children:
            recursive_child_seek(article, c['id'])
        output.append(id) # parent goes last :)
    else:
        output.appendleft(id)
        return

def check_sequence(article, first):
    recursive_child_seek(article, first)
    return output

if __name__ == "__main__":
    with open('clean_html.html', 'r') as html:
         html_str = html.read()
    t = clean_up(html_str)
    pdfkit.from_string(t, "abok-test.pdf", css="alation.css")
    pass

