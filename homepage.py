import pandas as pd
import sys
import json

if __name__ == "__main__":
    args = sys.argv

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
        {
          "type": "carousel",
          "title" : "-- Shortcuts --",
          "tiles" : []
        },
        {
          "type": "object_window_row",
          "objects": []
        }
      ]
    }

    admin_sections = pd.read_csv(args[1])
    admin_sections['description'] = ""
    homepage["admin_sections"][0]["tiles"]  =list(admin_sections.apply(lambda x: dict(x), axis=1))
    # delete object window section for now... - to-do
    homepage["admin_sections"] = [homepage["admin_sections"][0]]

    p=(json.dumps(homepage))
    print (p)