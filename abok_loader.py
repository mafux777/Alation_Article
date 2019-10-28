# -*- coding: utf-8 -*-
import pandas as pd
from AlationInstance import AlationInstance
from Article import Article
from secure_copy import extract_files#, secure_copy
#from query import *
from alationutil import log_me
from abok import check_sequence

# import the necessary packages
import argparse
import time
import re

if __name__ == "__main__":
    # parse the command line arguments
    ap = argparse.ArgumentParser()
    ap.add_argument("-u", "--username",  required=True,  help="username")
    ap.add_argument("-p", "--password",  required=True,  help="password")
    ap.add_argument("-H", "--host",      required=True,  help="URL of the Alation instance")
    args = vars(ap.parse_args())

    base_path = ''
    desired_template = u"ABOK Article"
    pickle_file = "ABOK.gzip"
    query_file = "AA Queries.gzip"
    dd_file = "AA_dd.gzip"


    # --- Log into the target instance
    url_2    = args['host']
    user_2   = args['username']
    passwd_2 = args['password']
    target = AlationInstance(url_2, user_2, passwd_2)

    allArticles = pd.read_pickle(base_path + pickle_file)
    Art = Article(allArticles)                    # convert to Article class

    templates = target.getTemplates()
    template_id = int(templates[templates.title==desired_template]['id'])

    order = check_sequence(allArticles, first=51)
    dummy = target.postArticle(dict(title="dummy {}".format(
            time.strftime(u"%Y-%b-%d %H:%M:%S", time.localtime()))
                                    , body='Delete this afterwards'))
    offset = int(dummy['id'])+1
    n = len(order)
    od = {}
    for i in range(0, n):
        od[order[i]] = i + offset
    for i in range(0, n): # i will go from 0 to 122
        ii = order[i] # ii will go through all the articles in the right order
        #print(offset+i, ii, allArticles.title[ii]) # expected ID, original ID, title
        ori_body = allArticles.body[ii]
        new_body = ori_body

        def convert(matchobj):
            data_oid = int(matchobj.group(1))
            if data_oid in od:
                new_data_oid = od[data_oid]
            else:
                new_data_oid = 0
                log_me("No match for link: {}".format(matchobj.group(0)))
            return "<a data-oid=\"{0}\" data-otype=\"article\" href=\"/article/{0}/\">".format(new_data_oid)


        regex = (r"<a +data-oid=\"(\d+)\" +data-otype=\"article\" +href=\"/article/\d+/[^>]*\">")
        fi = re.finditer(regex, ori_body, flags=re.MULTILINE)
        for matchNum, match in enumerate(fi, start=1):
            new_link = convert(match)
            old_link = match.group(0) # entire match
            new_body = new_body.replace(old_link, new_link)
            # print("--ORILINK--")
            # print(old_link)
            # print("--NEWLINK--")
            # print(new_link)

        a=dict(title=allArticles.title[ii],
             body=new_body,
             custom_templates=[template_id],
             children=[dict(otype='article', id=od[c['id']]) for c in allArticles.children[ii]])
        b = target.postArticle(a)
        if b['id']==offset+i:
            print("{}->{}:{}".format(ii, b['id'], b['title']))
        else:
            print("Unexpected ID: {}->{}:{}".format(ii, b['id'], b['title']))

    queries = pd.read_pickle(base_path + query_file)


    target.putQueries(queries=queries)

    # Extract the media files zip
    extract_files(base_path)

    # log_me(u"Securely copying media files to remote host")
    # secure_copy(host=u'apt-poodle.alation-test.com',
    #              username=u'matthias.funke',
    #              key_filename=u'/Users/matthias.funke/.ssh/id_rsa',
    #              local_dir=u"media/image_bank/", remote_dir=u"/data/site_data/media/image_bank")



    # Some descriptions in the Alation Analytics data dictionary are links to ABOK articles, so have to do this last
    target.upload_dd(pd.read_pickle(base_path + dd_file), 0, "Alation Analytics")




