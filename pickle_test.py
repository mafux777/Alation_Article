# -*- coding: utf-8 -*-
import pandas as pd
from alationutil import log_me

# import the necessary packages
import argparse
import time
import re
import pickle

if __name__ == "__main__":
    # parse the command line arguments
    base_path = ''
    desired_template = u"ABOK Article"
    pickle_file = "ABOK.gzip"

    with open(pickle_file, 'rb') as mypickle:
        p = pickle.Unpickler(mypickle)
        pickle_cont = p.load()

    dd = pickle_cont['dd']
    allArticles = pickle_cont['article']
    queries = pickle_cont['queries']
    allTemplates = pickle_cont['template']
    custom_fields = pickle_cont['custom_fields']

    print("Data Dict contains {} rows".format(dd.shape[0]))
    print("Article contains {} rows".format(allArticles.shape[0]))
    print("Queries contains {} rows".format(queries.shape[0]))

    ct = allArticles.body.str.contains('user')
    print(allArticles.loc[ct, ['id', 'title']])

    print(queries.loc[queries.id==81, 'title'])