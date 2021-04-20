from sudoku.AlationInstance import AlationInstance
from sudoku.Article import Article
from sudoku.alationutil import log_me, extract_files
import config
import pandas as pd

if __name__ == "__main__":
    desired_template = config.desired_template
    pickle_file = config.pickle_file
    pickle_cont = {}

    # --- Log into the source instance
    alation_1 = AlationInstance(config.args['host'],
                                config.args['username'],
                                config.args['password'])


    log_me("Getting desired articles")
    articles  = alation_1.get_articles(template=desired_template) # download all articles
    Art = Article(articles)                    # convert to Article class

    # log_me("Getting media files via download")
    # list_of_files = list(Art.get_files())
    # alation_1.get_media_file(list_of_files, config.base_path)
    # extract_files(config.base_path)

    # log_me("Creating PDF")
    # query_html = generate_html(queries)
    # Art.create_pdf(first=22, additional_html="")

    templates = alation_1.get_templates()          # download all templates (with their custom fields)
    custom_fields = alation_1.get_custom_fields_from_template(desired_template) # this way we also get the template info

    new_custom_fields = alation_1.generic_api_get("/integration/v2/custom_field/", official=True)
    new_custom_fields_df = pd.DataFrame(new_custom_fields)

    print("All done.")



