# -*- coding: utf-8 -*-
import pandas as pd

from AlationInstance import AlationInstance
from Article import Article
from secure_copy import secure_copy, extract_files
from query import *

# import the necessary packages
import argparse
import pickle

if __name__ == "__main__":
    # parse the command line arguments
    ap = argparse.ArgumentParser()
    ap.add_argument("-u", "--username",  required=True,  help="username")
    ap.add_argument("-p", "--password",  required=True,  help="password")
    ap.add_argument("-H", "--host",      required=True,  help="URL of the Alation instance")
    ap.add_argument("-v", "--username2", required=False, help="username2")
    ap.add_argument("-w", "--password2", required=False, help="password2")
    ap.add_argument("-x", "--host2",     required=False, help="URL of the 2nd Alation instance")
    ap.add_argument("-f", "--pickle",    required=False, help="pickle file name")
    ap.add_argument("-t", "--template",  required=False, help="desired template")
    args = vars(ap.parse_args())

    url_1    = args['host']
    user_1   = args['username']
    passwd_1 = args['password']

    base_path = ''
    desired_template = args['template']
    pickle_file = args['pickle']
    pickle_cont = {}

    # --- Log into the source instance
    alation_1 = AlationInstance(url_1, user_1, passwd_1)
    dd = alation_1.download_datadict(1) # Alation Analytics is 1 on ABOK

    # --- Log into the target instance
    url_2    = args['host2']
    user_2   = args['username2']
    passwd_2 = args['password2']
    #target = AlationInstance(url_2, user_2, passwd_2)


    log_me(u"Getting desired articles")
    allArticles  = alation_1.getArticles(template=desired_template) # download all articles

    Art = Article(allArticles)                    # convert to Article class
    queries = alation_1.getQueries()
    author = queries.author.apply(lambda x: x['id'] not in [1,5])
    queries = queries[author]

    query_html = generate_html(queries)



    # First pass of fixing references
    #target.putQueries(queries=queries)
    Art.convert_references()

    log_me(u"Getting media files via download")
    list_of_files = list(Art.get_files())
    alation_1.getMediaFile(list_of_files, base_path)
    extract_files(base_path)

    # log_me(u"Securely copying media files to remote host")
    # secure_copy(host=u'18.218.6.215',
    #             username=u'ec2-user',
    #             key_filename=u'/Users/matthias.funke/.ssh/PSPersonMachines.pem',
    #             local_dir=u"media/image_bank/", remote_dir=u"/data/site_data/media/image_bank")


    #log_me(u"Creating PDF")
    Art.create_pdf(first=51, additional_html=query_html)

    allTemplates = alation_1.getTemplates()          # download all templates (with their custom fields)
    #allTemplates.to_csv("templates.csv", encoding='utf-8', index=False)
    #allTemplates = pd.read_csv(base_path + "templates.csv")
    # We need to have quite detailed information to create the template!

    custom_fields = alation_1.getCustomFields_from_template(desired_template) # this way we also get the template info

    pickle_cont['dd'] = dd
    pickle_cont['article'] = allArticles
    pickle_cont['queries'] = queries
    pickle_cont['template'] = allTemplates
    pickle_cont['custom_fields'] = custom_fields


    with open(pickle_file, 'wb') as mypickle:
        p = pickle.Pickler(mypickle, pickle.HIGHEST_PROTOCOL)
        p.dump(pickle_cont)
    # create a migration notes field to hold manual migration instructions
    migration_error = dict(allow_multiple=False, allowed_otypes=None, backref_name=None, backref_tooltip_text=None,
                           builtin_name=None, field_type=u'RICH_TEXT',
                           name_plural=u'Migration Notes', name_singular=u'Migration Notes',
                           options=[])

    #if custom_fields.shape[0]>0:
    #    custom_fields = custom_fields.append(migration_error, ignore_index=True)


    # Next, we put the objects we want. We need to start with the custom fields, then the template,
    # then the articles, and finally the glossaries.

    #c_fields = alation_1.putCustomFields(custom_fields) # returns a list of field IDs (existing or new)
    #desired_template_pd = allTemplates[allTemplates.title==desired_template]
    #t = target.putCustomTemplate(desired_template_pd, c_fields) # returns the ID of the (new) template
    #target.putGlossaries(glossaries) --- NOT IMPLEMENTABLE YET DUE TO LACK OF API
    #result = target.putArticles(Art, desired_template, c_fields)
    #log_me(result.content)

    #target.fix_refs(desired_template) # data source for queries (on the target, post-migration)
    #target.fix_children(allArticles, template=desired_template) # passing DataFrame of source articles which contain P-C relationships
    #target.upload_dd(dd, 0, "Alation Analytics")





