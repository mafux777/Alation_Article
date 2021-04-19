# -*- coding: utf-8 -*-
from AlationInstance import AlationInstance
from alationutil import log_me
from abok import check_sequence

# import the necessary packages
import argparse
import time
import re
import pickle
import config

def use_dummy_to_get_highest_id():
    dummy = target.post_article(dict(title="dummy {}".format(
            time.strftime(u"%Y-%b-%d %H:%M:%S", time.localtime()))
                                    , body='Delete this afterwards'))
    dummy_id = int(dummy['id'])
    target.del_article(dummy_id)
    return dummy_id

if __name__ == "__main__":
    # Open the pickle file
    with open(config.pickle_file, 'rb') as mypickle:
        p = pickle.Unpickler(mypickle)
        pickle_cont = p.load()
    # extract data dictionary, articles, queries, templates, custom fields from the pickle
    dd = pickle_cont['dd']
    articles = pickle_cont['article']
    queries = pickle_cont['queries']

    # --- Log into the target instance
    target = AlationInstance(config.args['host'],
                             config.args['username'],
                             config.args['password'])
    # -- Make sure ABOK Article template is created
    template_id = target.put_custom_template('ABOK Article')
    # If desired, delete all pre-existing ABOK articles.
    if config.args['delete']:
        a = target.get_articles(template=config.desired_template)
        log_me('Deleting existing articles: {}'.format(a.id))
        a.id.apply(target.del_article)

    # Upload all queries to the instance. Note we implicitly assume here that the only
    # references are to existing objects, e.g. AA tables
    target.put_queries(queries=queries)
    queries = target.get_queries() # this is so we can figure out the ID

    # to-do: check sequence before pickling! Then we can simplify this code even more
    order = check_sequence(articles, first=config.first_abok_article) # order is list of IDs
    n = len(order)

    offset = use_dummy_to_get_highest_id() +1
    # if the order is set beforehand, the mapping dict is no longer required, we would just add the offset
    mapping_dict = {}
    for i in range(0, n):
        mapping_dict[order[i]] = i + offset

    articles_created = []

    for j in order:
        ori_body = articles.body[j]
        new_body = ori_body

        def convert(matchobj):
            data_oid = int(matchobj.group(1))
            if matchobj.group(2)=="article":
                if data_oid in mapping_dict:
                    new_data_oid = mapping_dict[data_oid]
                else:
                    new_data_oid = 0
                    log_me(f"No match for link: {matchobj.group(0)}")
                return f"<a data-oid=\"{new_data_oid}\" data-otype=\"article\" href=\"/article/{new_data_oid}/\"></a>"
            elif matchobj.group(2)=="query":
                query_title = matchobj.group(4)
                if not queries.empty:
                    q_match = queries.title == query_title
                    if q_match.any():
                        matching_queries = queries[q_match]
                        # m is a reference to somewhere and we need to fix it.
                        oid = (matching_queries.iloc[-1]).id  # -1 means last
                        return f"<a data-oid=\"{oid}\" data-otype=\"query\" href=\"/query/{oid}/\"></a>"
            elif matchobj.group(2)=="table":
                qual_name = matchobj.group(4).split()[0]
                tb = target.get_tables_by_name(qual_name)
                if not tb.empty:
                    oid = tb.index[-1]
                    return f"<a data-oid=\"{oid}\" data-otype=\"table\" href=\"/table/{oid}/\"></a>"

        # --- Replace all IDs in at-mentions with new oids
        regex = r"<a +data-oid=\"(\d+)\" +data-otype=\"(article|table|query)\" +href=\"/\2/(\1)/[^>]*\">([^<]*)</a>"
        fi = re.finditer(regex, ori_body, flags=re.MULTILINE)
        for matchNum, match in enumerate(fi, start=1):
            new_link = convert(match)
            old_link = match.group(0) # entire match
            if new_link:
                new_body = new_body.replace(old_link, new_link)

        # --- Construct a new list of children
        children = []
        for c in articles.children[j]:
            if c['id'] in mapping_dict:
                children.append(dict(otype='article', id=mapping_dict[c['id']]))
            else:
                log_me("No child for {}".format(c))

        # --- Construct a new article to post
        a=dict(title=articles.title[j],
               body=new_body,
               custom_templates=[template_id],
               children=children)
        new_article = target.post_article(a)
        articles_created.append(new_article)
        if 'id' in new_article:
            print(f"{j}->{new_article['id']}:{new_article['title']}")
        else:
            print(f"Unexpected: {j}->{new_article}")

    # Some descriptions in the Alation Analytics data dictionary are links to ABOK articles, so have to do this last
    #target.upload_dd(dd, 0, "Alation Analytics", articles_created)
    log_me("NEXT STEP: Ask your server admin to upload the media files.")




