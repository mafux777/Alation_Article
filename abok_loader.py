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
import pickle

if __name__ == "__main__":
    # parse the command line arguments
    ap = argparse.ArgumentParser()
    ap.add_argument("-u", "--username",  required=True,  help="username")
    ap.add_argument("-p", "--password",  required=True,  help="password")
    ap.add_argument("-H", "--host",      required=True,  help="URL of the Alation instance")
    ap.add_argument("-d", "--delete",  action='store_const',
                    const=True, default=False,  required=False, help="delete previous")
    args = vars(ap.parse_args())

    base_path = ''
    desired_template = u"ABOK Article"
    pickle_file = "ABOK.gzip"
    #query_file = "AA Queries.gzip"
    #dd_file = "AA_dd.gzip"

    with open(pickle_file, 'rb') as mypickle:
        p = pickle.Unpickler(mypickle)
        pickle_cont = p.load()

    dd = pickle_cont['dd']
    allArticles = pickle_cont['article']
    queries = pickle_cont['queries']
    allTemplates = pickle_cont['template']
    custom_fields = pickle_cont['custom_fields']





    # --- Log into the target instance
    url_2    = args['host']
    user_2   = args['username']
    passwd_2 = args['password']
    delete_flag = args['delete']
    target = AlationInstance(url_2, user_2, passwd_2)
    if delete_flag:
        a = target.getArticles(template=desired_template)
        log_me('Deleting existing articles: {}'.format(a.id))
        a.id.apply(target.delArticle)

    Art = Article(allArticles)                    # convert to Article class

    templates = target.getTemplates()
    template_id = int(templates[templates.title==desired_template]['id'])

    target.putQueries(queries=queries)
    queries = target.getQueries() # this is so we can figure out the number

    order = check_sequence(allArticles, first=51)
    dummy = target.postArticle(dict(title="dummy {}".format(
            time.strftime(u"%Y-%b-%d %H:%M:%S", time.localtime()))
                                    , body='Delete this afterwards'))
    dummy_id = int(dummy['id'])
    target.delArticle(dummy_id)
    offset = dummy_id+1
    n = len(order)
    od = {}

    articles_created = []

    for i in range(0, n):
        od[order[i]] = i + offset
    for i in range(0, n): # i will go from 0 to 122
        ii = order[i] # ii will go through all the articles in the right order
        #print(offset+i, ii, allArticles.title[ii]) # expected ID, original ID, title
        ori_body = allArticles.body[ii]
        new_body = ori_body

        def convert(matchobj):
            data_oid = int(matchobj.group(1))
            if matchobj.group(2)=="article":
                if data_oid in od:
                    new_data_oid = od[data_oid]
                else:
                    new_data_oid = 0
                    log_me("No match for link: {}".format(matchobj.group(0)))
                return "<a data-oid=\"{0}\" data-otype=\"article\" href=\"/article/{0}/\"></a>".format(new_data_oid)
            elif matchobj.group(2)=="query":
                query_title = matchobj.group(4)
                if not queries.empty:
                    q_match = queries.title == query_title
                    if q_match.any():
                        matching_queries = queries[q_match]
                        # m is a reference to somewhere and we need to fix it.
                        oid = (matching_queries.iloc[-1]).id  # -1 means last
                        return "<a data-oid=\"{0}\" data-otype=\"query\" href=\"/query/{0}/\"></a>".format(oid)
            elif matchobj.group(2)=="table":
                qual_name = matchobj.group(4).split()[0]
                tb = target.getTablesByName(qual_name)
                if not tb.empty:
                    oid = tb.index[-1]
                    return "<a data-oid=\"{0}\" data-otype=\"table\" href=\"/table/{0}/\"></a>".format(oid)


        #regex =r"<a +data-oid=\"(\d+)\" +data-otype=\"(article)\" +href=\"/article/\d+/[^>]*\">")
        regex = r"<a +data-oid=\"(\d+)\" +data-otype=\"(article|table|query)\" +href=\"/\2/(\1)/[^>]*\">([^<]*)</a>"
        fi = re.finditer(regex, ori_body, flags=re.MULTILINE)
        for matchNum, match in enumerate(fi, start=1):
            new_link = convert(match)
            old_link = match.group(0) # entire match
            if new_link:
                new_body = new_body.replace(old_link, new_link)
            # print("--ORILINK--")
            # print(old_link)
            # print("--NEWLINK--")
            # print(new_link)

        children = []
        for c in allArticles.children[ii]:
            if c['id'] in od:
                children.append(dict(otype='article', id=od[c['id']]))
            else:
                log_me("No child for {}".format(c))

        a=dict(title=allArticles.title[ii],
             body=new_body,
             custom_templates=[template_id],
             children=children)
        b = target.postArticle(a)
        articles_created.append(b)
        if 'id' in b and b['id']==offset+i:
                print("{}->{}:{}".format(ii, b['id'], b['title']))
        else:
            print("Unexpected: {}->{}".format(ii, b))




    # Extract the media files zip
    #extract_files(base_path)

    # log_me(u"Securely copying media files to remote host")
    # secure_copy(host=u'apt-poodle.alation-test.com',
    #              username=u'matthias.funke',
    #              key_filename=u'/Users/matthias.funke/.ssh/id_rsa',
    #              local_dir=u"media/image_bank/", remote_dir=u"/data/site_data/media/image_bank")



    # Some descriptions in the Alation Analytics data dictionary are links to ABOK articles, so have to do this last
    target.upload_dd(dd, 0, "Alation Analytics", articles_created)




