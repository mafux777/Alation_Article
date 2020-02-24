from AlationInstance import AlationInstance
from Article import Article
from alationutil import log_me, extract_files
import config
import pickle


if __name__ == "__main__":
    desired_template = config.desired_template
    pickle_file = config.pickle_file
    pickle_cont = {}

    # --- Log into the source instance
    alation_1 = AlationInstance(config.args['host'],
                                config.args['username'],
                                config.args['password'])

    # --- Log into the target instance
    target = AlationInstance(config.args['host2'],
                             config.args['username2'],
                             config.args['password2'])


    log_me("Getting desired articles")
    articles  = alation_1.get_articles(template=desired_template) # download all articles
    articles['body'] = articles.body.apply(lambda x: x.replace('https://abok.alationproserv.com', ''))
    Art = Article(articles)                    # convert to Article class
    #queries = alation_1.get_queries()

    # First pass of fixing references
    #target.put_queries(queries=queries)
    Art.convert_references()

    log_me("Getting media files via download")
    list_of_files = list(Art.get_files())
    alation_1.get_media_file(list_of_files, config.base_path)
    extract_files(config.base_path)

    # log_me("Creating PDF")
    # query_html = generate_html(queries)
    # Art.create_pdf(first=1889, additional_html=query_html)

    templates = alation_1.get_templates()          # download all templates (with their custom fields)

    custom_fields = alation_1.get_custom_fields_from_template(desired_template) # this way we also get the template info

    ds_id = alation_1.look_up_ds_by_name("Alation Analytics")
    pickle_cont['dd'] = alation_1.download_datadict_r6(ds_id)
    pickle_cont['article'] = articles
    #pickle_cont['queries'] = queries
    #pickle_cont['template'] = templates
    #pickle_cont['custom_fields'] = custom_fields


    with open(pickle_file, 'wb') as mypickle:
        p = pickle.Pickler(mypickle, pickle.HIGHEST_PROTOCOL)
        p.dump(pickle_cont)

    # Next, we put the objects we want. We need to start with the custom fields, then the template,
    # then the articles, and finally the glossaries.

    c_fields = target.put_custom_fields(custom_fields) # returns a Series of field IDs (existing or new)
    t = target.put_custom_template(desired_template, c_fields) # returns the ID of the (new) template
    #target.putGlossaries(glossaries) --- NOT IMPLEMENTABLE YET DUE TO LACK OF API
    result = target.put_articles(Art, desired_template, c_fields)
    log_me(result.json())

    target.fix_refs(desired_template) # this corrects references from articles to other articles, queries and tables
    target.fix_children(articles, template=desired_template) # passing DataFrame of source articles which contain P-C relationships
    target.upload_dd(pickle_cont['dd'], ds_title=config.desired_ds)





