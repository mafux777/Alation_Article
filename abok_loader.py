# -*- coding: utf-8 -*-
import pandas as pd
from AlationInstance import AlationInstance
from Article import Article
from secure_copy import extract_files#, secure_copy
#from query import *
from alationutil import log_me

# import the necessary packages
import argparse

if __name__ == "__main__":
    # parse the command line arguments
    ap = argparse.ArgumentParser()
    ap.add_argument("-u", "--username",  required=True,  help="username")
    ap.add_argument("-p", "--password",  required=True,  help="password")
    ap.add_argument("-H", "--host",      required=True,  help="URL of the Alation instance")
    args = vars(ap.parse_args())


    desired_template = u"ABOK Article"
    pickle_file = "ABOK.gzip"
    query_file = "AA Queries.gzip"
    dd_file = "AA_dd.gzip"


    # --- Log into the target instance
    url_2    = args['host']
    user_2   = args['username']
    passwd_2 = args['password']
    target = AlationInstance(url_2, user_2, passwd_2)

    allArticles = pd.read_pickle(pickle_file)
    Art = Article(allArticles)                    # convert to Article class
    queries = pd.read_pickle(query_file)


    target.putQueries(queries=queries)

    # First pass of fixing references: set data-oid to 0 and add title to <a> anchor
    # This means all links need to be re-calculated in Pass 2 to work
    # But also, we can guarantee the links will not point to the wrong page
    #Art.convert_references() -- not needed in the new pickle file??

    # Extract the media files zip
    extract_files()

    # log_me(u"Securely copying media files to remote host")
    # secure_copy(host=u'apt-poodle.alation-test.com',
    #              username=u'matthias.funke',
    #              key_filename=u'/Users/matthias.funke/.ssh/id_rsa',
    #              local_dir=u"media/image_bank/", remote_dir=u"/data/site_data/media/image_bank")


    allTemplates = pd.read_csv("templates.csv")
    # We need to have quite detailed information to create the template!

    custom_fields = pd.read_pickle("custom_fields.gzip")

    # create a migration notes field to hold manual migration instructions
    migration_error = dict(allow_multiple=False, allowed_otypes=None, backref_name=None, backref_tooltip_text=None,
                           builtin_name=None, field_type=u'RICH_TEXT',
                           name_plural=u'Migration Notes', name_singular=u'Migration Notes',
                           options=[])

    if custom_fields.shape[0]>0:
        custom_fields = custom_fields.append(migration_error, ignore_index=True)


    # Next, we put the objects we want. We need to start with the custom fields, then the template,
    # then the articles, and finally the glossaries.

    c_fields = target.putCustomFields(custom_fields) # returns a list of field IDs (existing or new)
    desired_template_pd = allTemplates[allTemplates.title == desired_template]
    t = target.putCustomTemplate(desired_template_pd, c_fields) # returns the ID of the (new) template

    #target.putGlossaries(glossaries) --- NOT IMPLEMENTABLE YET DUE TO LACK OF API
    result = target.putArticles(Art, desired_template, c_fields)
    log_me(result.content)

    target.fix_refs(desired_template)
    target.fix_children(allArticles) # passing DataFrame of source articles which contain P-C relationships

    # Some descriptions in the Alation Analytics data dictionary are links to ABOK articles, so have to do this last
    target.upload_dd(pd.read_pickle(dd_file), 0, "Alation Analytics")




