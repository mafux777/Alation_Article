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

def clean_up(html):
    soup = BeautifulSoup(html, features="html.parser")
    print('Cleaning up some html formatting .....')

    #Remove all hrefs using Beautiful soup so dead links are not in the pdf
    for a in soup.findAll('a'):
        del a['href']

    #Save back from soup object to string and encode back to unicode because pdfkit needs unicode
    html = str(soup)
    html = html.decode('utf-8')

    #Remove unicode characters that are not in the ascii range so they don't show up in the pdf
    html = unicodedata.normalize("NFKD", html).encode('ascii','ignore')

    #// Replace img paths to point to the local image location
    html = html.replace('/media','/Users/matthias.funke/media')
    html = html.replace('https://abok.alationproserv.com/media/','/Users/matthias.funke/media/')

    #Fix a bit more formatting - css styles seem to be ignored in all cases no matter whether they are html inline, external or used as attributes in the html
    html = html.replace('<p>','<p style="font-family:arial;font-size:12px;">')
    html = html.replace('<h1>','<h1 style="font-family:arial;font-size:12px;">')
    html = html.replace('<h2>','<h2 style="font-family:arial;font-size:12px;">')
    html = html.replace('<h3>','<h3 style="font-family:arial;font-size:12px;">')
    html = html.replace('<tbody>','<tbody style="font-family:arial;font-size:12px;">')
    html = html.replace('<ul>','<ul style="font-family:arial;font-size:12px;">')
    html = html.replace('<thead>','<thead style="font-family:arial;font-size:12px;">')
    html = html.replace('<table style="width: 100%;">','<table style="width: 100%;border:"1";cellpadding:"2";cellspacing:"2">')
    html = html.replace('<table border="1" cellpadding="0" cellspacing="0"','<table border="1" cellpadding="2" cellspacing="2"')

    #Point to cover html file
    cover = 'cover.html'

    #Fix a bit of formatting on the cover page - css styles seem to be ignored in all cases no matter whether they are html inline, external or used as attributes in the html
    cover = cover.replace('<h1>','<h1 style="font-family:arial;font-size:32px;">')
    cover = cover.replace('<h2>','<h2 style="font-family:arial;font-size:28px;">')
    cover = cover.replace('<p>','<p style="font-family:arial;font-size:16px;">')
    return html






