from AlationInstance import AlationInstance
from Article import Article
from alationutil import log_me, extract_files
import config
import pickle
from query import generate_html

if __name__ == "__main__":
    desired_template = "ABOK Article"

    # --- Log into the source instance
    alation_1 = AlationInstance(host="http://abok.alationproserv.com",
                                account="matthias.funke@alation.com",
                                password="c8@L92z7hF23CX")


    log_me("Getting desired articles")
    articles  = alation_1.get_articles(template=desired_template) # download all articles
    Art = Article(articles)                    # convert to Article class
    queries = alation_1.get_queries()

    log_me("Getting media files via download")
    list_of_files = list(Art.get_files())
    alation_1.get_media_file(list_of_files, "./")
    extract_files("./")

    log_me("Creating PDF")
    query_html = generate_html(queries)
    Art.create_pdf(first=1889, additional_html=query_html)
