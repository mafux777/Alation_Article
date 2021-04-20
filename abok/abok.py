# coding=utf-8
#********************************************************************************************************
#
# Purpose: Generate ABOK pdf file from the Alation ABOK instance
# Usage: Run manually from the command line
# Output:  It produces an ABOK PDF file with a datetime stamp
#
#********************************************************************************************************

import re
import pdfkit
from sudoku.alationutil import *
import os
from pathlib import Path

def clean_up(html):
    soup = BeautifulSoup(html, "html5lib")
    dirpath = Path(os.getcwd())
    known_folders={}

    #Remove all hrefs using Beautiful soup so dead links are not in the pdf
    links = soup.findAll('a')
    for a in links:
        # Let's remove all hyperlinks to Alation Objects only
        if "data-otype" in a.attrs:
            if a.string:
                # "m" is a convenience tag which has no effect
                n = a.string.wrap(soup.new_tag("m"))
                a.replace_with(n)

    images = soup.findAll('img')
    for i in images:
        my_path = Path(i['src'])
        new_path = Path(dirpath / 'media/image_bank' / my_path.parts[-1])
        if new_path.exists():
            i['src'] = new_path
        else:
            log_me('No file found for {}'.format(new_path))
    #log_me('Processed {}'.format(known_folders))


    #Save back from soup object to string and encode back to unicode because pdfkit needs unicode
    html = soup.prettify()

    # Find some strange chars that happen after . and replace with a single space
    spaces = re.findall(pattern=u'([.]+)([\s]{2})([a-zA-Z]+)',string=html,flags=re.UNICODE)
    my_spaces = list(set([m[1] for m in spaces]))
    for m in my_spaces:
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
    try:
        print (article.title[id])
        if article.has_children[id]:
            current_children = article.children[id]
            for c in current_children:
                recursive_child_seek(article, c['id'])
            output.append(id) # parent goes last :)
        else:
            output.appendleft(id)
            return
    except:
        pass

def check_sequence(article, first):
    recursive_child_seek(article, first)
    return output

if __name__ == "__main__":
    with open('clean_html.html', 'r') as html:
         html_str = html.read()
    t = clean_up(html_str)
    pdfkit.from_string(t, "abok-test.pdf", css="css/alation.css")
    pass

