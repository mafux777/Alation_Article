from sudoku.AlationInstance import AlationInstance
from sudoku.Article import Article
from sudoku.alationutil import log_me, extract_files
import config
import pickle
# from query import generate_html
from openpyxl.utils.dataframe import dataframe_to_rows
from openpyxl import Workbook
from openpyxl.styles import Font, Fill, Alignment
from openpyxl import load_workbook
from openpyxl.utils.exceptions import IllegalCharacterError
import pandas as pd
import json
import re
import random

def open_source_file(fn):
    wb = load_workbook(fn)
    data = wb.active.values
    cols = next(data)
    data = list(data)
    df = pd.DataFrame(data, columns=cols)
    return df

mapper = {
    "KDEs"              :       "key",
    "Description"       :       "description",
    # "Data Quality Rules":       "Rich Text (001)",
    "consolidated 1":       "Rich Text (001)",
}

def json_row(row):
    d = dict(row)
    dj = json.dumps(d)
    return dj

def consolidate_cols(row):
    d = dict(row)
    rows = [f"<tr><td> {label} </td><td> {cell} </td></tr>" for label, cell in d.items()]
    return f"<table>{''.join(rows)}</table>"

def replace_table_mention(text, alation):
    pattern = r"(?P<table>[A-Z0-9_]+)\.(?P<column>[A-Z0-9_]+)"
    r = re.findall(pattern, text)
    for x in r:
        params = dict(name__iexact=x[0], )
        c = alation.generic_api_get('/integration/v2/column/', params=params, official=True)
        # dummy
        c = [dict(id=random.randint(1, 100))]
        for my_col in c:
            oid=my_col.get('id')
            mention = f'{x[0]}.{x[1]}'
            new_link = (f'<p><a data-oid="{oid}" data-otype="attribute" href="/attribute/{oid}/">'
                        f'{mention}'
                        f'</a></p>')
            text = text.replace(mention, new_link)
    return text

if __name__ == "__main__":
    # Open Excel sheet with inputs
    df = open_source_file(config.input_file)

    # --- Log into the source instance
    alation_1 = AlationInstance(config.args['host'],
                                config.args['username'],
                                config.args['password'])

    look_for_cols = [
                    'SOR AFS table',
                    'EV AFS Table and Field Name',
                    'EV IL Table and Field Name',
                    'EV ML Table ',
                    'PWM TABLE and Field',
                    'RDM Loans Field Name',
                    'RDM - ML',
    ]

    for col in look_for_cols:
        df[col] = df[col].apply(replace_table_mention, alation=alation_1)


    # consolidate certain columns
    # df['consolidated 1'] = df.loc[:, ['KDE No',
    #                                   'Domain Level 2',
    #                                   'Domain Level 1',
    #                                   'KDEs',
    #                                   'Description']].apply(consolidate_cols, axis=1)
    df['consolidated 1'] = df.apply(consolidate_cols, axis=1)

    # Rename certain columns
    df.rename(columns=mapper, inplace=True)

    # Only keep the columns mentioned in the mapper
    cols = list(mapper.values())
    df = df.loc[: , cols]

    # convert dataframe into JSON rows format
    jsr = "\n".join(list(df.apply(json_row, axis=1)))

    desired_template = config.desired_template


    alation_1.put_articles_2(jsr, desired_template)

    log_me("Getting desired articles")
    articles  = alation_1.get_articles(template=desired_template) # download all articles
    Art = Article(articles)                    # convert to Article class

    # First pass of fixing references
    # Art.convert_references()

    templates = alation_1.get_templates()          # download all templates (with their custom fields)
    custom_fields = alation_1.get_custom_fields_from_template(desired_template) # this way we also get the template info


    # Next, we put the objects we want. We need to start with the custom fields, then the template,
    # then the articles, and finally the glossaries.

    c_fields = alation_1.put_custom_fields(custom_fields) # returns a Series of field IDs (existing or new)
    t = alation_1.put_custom_template(desired_template, c_fields) # returns the ID of the (new) template
    #target.putGlossaries(glossaries) --- NOT IMPLEMENTABLE YET DUE TO LACK OF API
    result = alation_1.put_articles(Art, desired_template, c_fields)
    log_me(result.json())
    #
    alation_1.fix_refs(desired_template) # this corrects references from articles to other articles, queries and tables





