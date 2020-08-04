import pandas as pd
import json
#from elasticsearch import Elasticsearch
from es_pandas import es_pandas
from langdetect import detect, detect_langs
from datetime import datetime
import os
import html
import glob
import time

from elasticsearch import Elasticsearch
# es = Elasticsearch("https://kekmaster:tN8t!c_LRWn@search-raiders-lost-kek-i37ssqiqmlpnt4o2klkbj3wvnq.us-east-2.es.amazonaws.com")
# https://search-raiders-lost-kek-i37ssqiqmlpnt4o2klkbj3wvnq.us-east-2.es.amazonaws.com/

# create a client instance of the library
# https://search-turbo-raiders-544c36viav3v7tyefmlihyg66e.us-east-2.es.amazonaws.com/
# ep2 = es_pandas("https://kekmaster:tN8t!c_LRWn@search-raiders-lost-kek-i37ssqiqmlpnt4o2klkbj3wvnq.us-east-2.es.amazonaws.com")
ep2 = es_pandas("https://kekmaster:tN8t!c_LRWn@search-turbo-raiders-544c36viav3v7tyefmlihyg66e.us-east-2.es.amazonaws.com")

def log_me(txt):
    try:
        print ("{} {}".format(
            time.strftime(u"%Y-%b-%d %H:%M:%S", time.localtime()),
            txt))
    except:
        print ("{} Formatting issue with a log message.".format(
            time.strftime(u"%Y-%b-%d %H:%M:%S", time.localtime())))

approved_ent = ['PERSON',
                'NORP',
                'FAC',
                'ORG',
                'GPE',
                'EVENT',
                'WORK_OF_ART',
                'LAW']

#list_of_files = glob.glob("/home/ec2-user/again-*")
list_of_files = glob.glob("/Users/matthias.funke/???")

def extract_approved_entity(entity):
    ret = []
    for e in entity:
        if e.get('entity_label') in approved_ent:
            ret.append(e.get('entity_text'))
    return set(ret)

for my_file in list_of_files:
    # init template if you want
    # doc_type = 'demo2'
    # ep2.init_es_tmpl(df, doc_type)
    all_posts=list()
    with open(my_file) as f:
        log_me(f"Opening file {my_file}")
        l = f.readlines()
        for my_line in l:
            posts = json.loads(my_line).get('posts')
            # log_me(f"Processing {len(posts)} posts")
            original_post = posts[0]['no']
            for p in posts:
                p['original_post'] = original_post
            # if len(posts)>299:
            #     log_me(f"Adding {len(posts)} posts to DataFrame")
            all_posts.extend(posts)
        log_me("Creating DataFrame")
        my_post_df = pd.DataFrame(all_posts)
        my_post_df.index = my_post_df.no
        log_me(f"Post-processing dataframe {my_post_df.shape}")
        my_post_df['suspected_lang'] = 'en'
        my_post_df['posted'] = my_post_df.time.apply(datetime.fromtimestamp)
        my_post_df['trip'] = my_post_df.trip.loc[pd.notnull].apply(str.lower)
        my_post_df['author'] = my_post_df.trip.loc[pd.notnull].apply(str.lower)
        my_post_df['com'] = my_post_df.com.loc[pd.notnull].apply(html.unescape)
        my_post_df['ent'] = my_post_df.entities.loc[pd.notnull(my_post_df.entities)].apply(extract_approved_entity)

        parts = my_file.split("/")[-1]
        i = "ec2-"+parts
        try:
           log_me(f"Working on {i}")
           # es.indices.delete(index=i, ignore=[400, 404])
           ep2.to_es(my_post_df, i, use_pandas_json=True, use_index=True, thread_count=5, chunk_size=10000)
        except Exception as e:
           print(f"\nException with {my_file}: {e}")
