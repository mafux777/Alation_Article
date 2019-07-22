# -*- coding: utf-8 -*-
import pandas as pd

from AlationInstance import AlationInstance
from Article import Article
from alationutil import log_me
from secure_copy import secure_copy

# import the necessary packages
import argparse
import re
import time
import json

if __name__ == "__main__":
    # construct the argument parse and parse the arguments
    ap = argparse.ArgumentParser()
    ap.add_argument("-u", "--username", required=True, help="username")
    ap.add_argument("-p", "--password", required=True, help="password")
    ap.add_argument("-H", "--host", required=True, help="URL of the Alation instance")
    ap.add_argument("-v", "--username2", required=False, help="username2")
    ap.add_argument("-w", "--password2", required=False, help="password2")
    ap.add_argument("-x", "--host2", required=False, help="URL of the 2nd Alation instance")
    args = vars(ap.parse_args())

    url_1    = args['host']
    user_1   = args['username']
    passwd_1 = args['password']
    alation_1 = AlationInstance(url_1, user_1, passwd_1)

    # queries = alation_1.getQueries(1)
    # queries.to_csv('queries.csv', encoding='utf-8')
    # users = alation_1.getUsers()
    #dd = alation_1.download_datadict(1)
    #dd.to_csv('AA-DD.csv', encoding='utf-8')

    desired_template = "ABOK Article"
    allArticles  = alation_1.getArticles(template=desired_template) # download all articles
    Art = Article(allArticles)                    # convert to Article class
    Art.create_pdf()
    #users= Art.get_users()
    #media = Art.get_files()
    #alation_1.getMediaFile(media)
    # secure_copy(host='3.130.86.60',
    #             username='ec2-user',
    #             key_filename='/Users/matthias.funke/.ssh/LightsailDefaultKey-us-east-2.pem',
    #             local_dir='/Users/matthias.funke/Downloads/media/image_bank/')

    refs = Art.get_references() # we get a series of MatchObjects
    Art.to_csv(desired_template + ".csv")
    allTemplates = alation_1.getTemplates()          # download all templates (with their custom fields)
    # We need to have quite detailed information to create the template!

    custom_fields = alation_1.getCustomFields_from_template(desired_template) # this way we also get the template info

    # create a migration notes field to hold manual migration instructions
    migration_error = dict(allow_multiple=False, allowed_otypes=None, backref_name=None, backref_tooltip_text=None,
                           builtin_name=None, field_type=u'RICH_TEXT',
                           name_plural=u'Migration Notes', name_singular=u'Migration Notes',
                           options=[])

    if custom_fields.shape[0]>0:
        custom_fields = custom_fields.append(migration_error, ignore_index=True)


    # Next, we put the objects we want. We need to start with the custom fields, then the template,
    # then the articles, and finally the glossaries.
    target = AlationInstance(args['host2'], args['username2'], args['password2'])
    c_fields = target.putCustomFields(custom_fields) # returns a list of field IDs (existing or new)
    desired_template_pd = allTemplates[allTemplates.title==desired_template]
    t = target.putCustomTemplate(desired_template_pd, c_fields) # returns the ID of the (new) template
    #target.putGlossaries(glossaries)
    result = target.putArticles(Art, desired_template, c_fields)
    log_me(result.content)


    # Let's read all articles again so we get the IDs, too.
    new_Article = Article(target.getArticles())

    # iterate through all articles with children
    art_with_children = allArticles[allArticles.has_children]
    for parent, children in art_with_children.children.iteritems():
        source_parent = alation_1.getArticleByID(parent)  # get a fresh copy of the article
        source_parent_title = source_parent[u'title']  # get a fresh title
        #log_me("Working on article {} ({}) with children to {}".format(i, source_art_title, r))
        # see if the article exists on the target and what its ID is
        target_parent = new_Article.get_article_by_name(source_parent_title)
        new_children = []
        if target_parent.empty:
            log_me("Skipping to next article in the list - make sure to replicate articles first")
        else:
            target_parent_id = int(target_parent[u'id'])
            for child in children:
                source_child = alation_1.getArticleByID(child[u'id'])
                if not source_child:
                    log_me("Unexpected: source article {} not found".format(child))
                else:
                    target_child_title = source_child[u'title']
                    # On target machine
                    target_child = new_Article.get_article_by_name(target_child_title)
                    if target_child.empty:
                        log_me("No target child: ".format(target_child_title))
                    else:
                        target_child_id = target_child.iloc[0, :]['id']
                        new_children.append(target_child_id)
            new_article = dict(body=target_parent.loc[target_parent_id, u'body'], title=source_parent_title, children=
                               [dict(id=new_child, otype="article") for new_child in new_children]
                               ) # only the required fields...
            log_me("Updating article {} now".format(target_parent_id))
            updated_art = target.updateArticle(target_parent_id, new_article)


    # --- Change references so that they point to the right ID in the target instance...

    # Iterate through all the references found earlier. Each source article may reference several other source articles
    # We need to update the description (body) of that same article on the target machine
    for i, r in refs.iteritems():
        # for each source article, start a to-do list of replacements
        replacements = []
        source_art = alation_1.getArticleByID(i) # get a fresh copy of the article
        source_art_title = source_art[u'title'] # get a fresh title
        log_me(u"Working on article {} ({})".format(i, source_art_title))
        # see if the article exists on the target and what its ID is
        target_art = new_Article.get_article_by_name(source_art_title)
        if target_art.empty:
            log_me(u"Skipping to next article in the list - make sure to replicate articles first")
        else:
            target_art_id = int(target_art[u'id'])
            # Loop through all the references for article i / target_art_id
            for ref in r:
                wholematch = ref.group(0)
                u = ref.group(1) # ID of the article referred to in the source (unicode string)
                log_me(u"Working on article {}/{} ({}) referring to {}".format(i, target_art_id,source_art_title, u))
                # Get a fresh copy of the article referenced
                target_ref = alation_1.getArticleByID(u)
                if not target_ref:
                    log_me(u"Unexpected: source article {} not found".format(u))
                else:
                    target_ref_title = target_ref[u'title']
                    # On target machine
                    t_art = new_Article.get_article_by_name(target_ref_title)
                    if t_art.empty:
                        error_string = u"Missing ref from article {}/{} to {}".format(i, target_art_id, wholematch)
                        # Create a dummy article with the error message
                        # The article then links to the error instead of somewhere random
                        err_art = dict(title=u"(Dummy) {}".format(target_ref_title),
                        body=error_string)
                        t_art_id = target.postArticle(err_art)['id']
                    else:
                        t_art_id = t_art.iloc[0,:]['id']
                    replacements.append((u, t_art_id, wholematch, target_ref_title))

        if replacements:
            log_me(u"Made the list of replacements: {}".format(replacements))
            # -- easy implementation: replace the object ID regardless of the danger of replacing it again accidentally
            new_body = source_art[u'body']
            for replacement in replacements:
                old = replacement[2]
                new = \
                    '<a data-oid="' + str(replacement[1]) + '" data-otype="article" href="/article/' + str(replacement[1]) + '/">' + replacement[3] + "</a>"
                if new_body.find(old):
                    log_me(u"Art {}: Replacing: {} -> {}".format(target_art_id, old, new))
                    new_body = new_body.replace(old, new)
                else:
                    if new_body.find(new):
                        log_me(u"Art {}: Probably already replaced {} previously".format(target_art_id, new))
                    else:
                        log_me(u"Art {}: Nothing to do?".format(target_art_id))
            new_article = dict(body=new_body, title=source_art_title) # only the required fields...
            log_me(u"Updating article {} now".format(target_art_id))
            updated_art = target.updateArticle(target_art_id, new_article)



