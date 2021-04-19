# cd /opt/alation/django/rosemeta/one_off_scripts
import os
from django.core import serializers
import bootstrap_rosemeta
import pandas as pd
import json
from alation_object_types.enums import ObjectType # get_otype()
from alation_object_type_directory.resources import *
# get_model_class(get_object_type_code('docstore_folder')).objects.get(pk=1).url()
from django.conf import settings
from logical_metadata.models import ValueHistory

article_code = get_object_type_code('article')

from logical_metadata.models import Mark
m=Mark.objects.filter(user_id=1)

my_list_1 = [] # use this for star -> carousel
my_list_2 = [] # use this for watch -> object window row

# -- Grab all marks and convert them to carousel items (my_list_1)
# or object windows rows (my_list_2)
for tile in m:
    my_key = ObjectKey(tile.otype, tile.oid)
    my_model = get_model_class(tile.otype).objects.get(pk=tile.oid)

    try:
        v=ValueHistory.get_history(my_key, field_id=3, requester_id=1, limit=1, offset=0)
        title = v[0]['value']
    except:
        title = my_model.url()

    try:
        v=ValueHistory.get_history(my_key, field_id=4, requester_id=1, limit=1, offset=0)
        desc = v[0]['value']
    except:
        desc = "No description in LMS"

    if tile.otype != article_code:
        my_list_1.append(dict(navigateURL=my_model.url(),
            expandDescriptions=True,
            description=desc,
            image="/static/img/homepage_images/colored_images/ArticleBlue.png",
            title=title))
    else:
        my_list_2.append(dict(otype="{}".format(
                ObjectType.get_otype(tile.otype)),
                oid=int(tile.oid)))


homepage = {
  "action_links" : [
    {
      "title" : "Browse Data Sources",
      "img" : "/static/img/homepage_images/static_actions/BrowseDataSources.png",
      "navigateURL" : "/sources/"
    },
    { "title" : "Search Tables",
      "img" : "/static/img/homepage_images/static_actions/SearchTables.png",
      "navigateURL" : "/search/?otype=table&q="
    },
    { "title" : "Search Business Intelligence this is too long of a long title for two lines",
      "img" : "/static/img/homepage_images/static_actions/SearchBI.png",
      "navigateURL" : "/search/?q=&otype=report_sources"
    },
    { "title" : "Write SQL",
      "img" : "/static/img/homepage_images/static_actions/WriteAQuery.png",
      "navigateURL" : "/compose/"
    },
    {
      "title" : "Write an Article",
      "img" : "/static/img/homepage_images/static_actions/CreateAnArticle.png",
      "navigateURL" : "/article/new"
    }
  ],
  "admin_sections" : [
   
  ]
}

# Put tiles in the carousel(s) or in the object window row(s)
from math import floor

n = 5
items_in_this_list = n
my_max = len(my_list_1)
for j in range(floor(my_max/n) + 1): # the number of carousels
    if n > my_max:
        items_in_this_list = my_max
    new_object = dict(type="carousel",
        title = "Section {}".format(j),
        tiles = my_list_1[j*n : j*n + items_in_this_list])
    #print "{}:{}".format(j, new_object)
    homepage["admin_sections"].append(new_object)
    my_max -= n

for i in range(1, floor(len(my_list_2)/2)):
    new_object = dict(type="object_window_row",
        objects=[my_list_2[i*2], my_list_2[i*2+1]])
    #print "{}:{}".format(i, new_object)
    homepage["admin_sections"].append(new_object)


p=(json.dumps(homepage, indent=4))
#print (p)

with open(os.path.join(settings.SITE_ROOT, 'site_data/homepage.json'), "w") as homepage_configure_file:
    homepage_configure_file.write(p)

