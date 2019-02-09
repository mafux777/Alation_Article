# -*- coding: utf-8 -*-
import pandas as pd

from AlationInstance import AlationInstance
from Article import Article
from alationutil import log_me

url_1 = "https://demo-sales.alationcatalog.com"
user = "matthias.funke@alation.com"
passwd = "x7ia8wrEDkZ)=G"

al_in = AlationInstance(url_1, user, passwd)
desired_template = "Aviation Glossary"
allArticles  = al_in.getArticles(template=desired_template) # download all articles
A = Article(allArticles)                    # convert to Article class
#A.to_csv(desired_template+".csv")
allTemplates = al_in.getTemplates()          # download all templates (with their custom fields)
# We need to have quite detailed information to create the template!

custom_fields = al_in.getCustomFields_from_template(desired_template) # this way we also get the template info

# Next, we put the objects we want. We need to start with the custom fields, then the template,
# then the articles, and finally the glossaries.
target = AlationInstance("https://demobeta.alationcatalog.com/", user, passwd)
c_fields = target.putCustomFields(custom_fields) # returns a list of field IDs (existing or new)
desired_template_pd = allTemplates[allTemplates.title==desired_template]
t = target.putCustomTemplate(desired_template_pd, c_fields) # returns the ID of the (new) template
#target.putGlossaries(glossaries)
result = target.putArticles(A, desired_template, c_fields)
log_me(result.content)
