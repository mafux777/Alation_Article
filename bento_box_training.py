from sudoku.bento import AlationInstance
from sudoku.alationutil import log_me
import config
import pandas as pd
from datetime import datetime, timezone
import hashlib
import json
from urllib.parse import quote
import requests

def get_bi_source(alation_1, bi_server):
    # Get a list of all BI folders
    bi_folders = alation_1.get_bi_folders(bi_server)

    cfv = alation_1.get_custom_field_values_for_oids(bi_folders.index)
    bi_folders_with_cfv = bi_folders.merge(cfv, left_index=True, right_index=True, how="left")

    # Get a list of all BI reports
    bi_reports = alation_1.get_bi_reports(bi_server)
    cfv = alation_1.get_custom_field_values_for_oids(bi_reports.index)
    bi_reports_with_cfv = bi_reports.merge(cfv, left_index=True, right_index=True, how="left")

    # Get a list of all BI report cols
    #bi_report_cols = alation_1.get_bi_report_cols(bi_server)
    #cfv = alation_1.get_custom_field_values_for_oids(bi_report_cols.index)
    #bi_report_cols_with_cfv = bi_report_cols.merge(cfv, left_index=True, right_index=True, how="left")

    # export = pd.concat([bi_folders_with_cfv, bi_reports_with_cfv, bi_report_cols_with_cfv])
    export = pd.concat([bi_folders_with_cfv, bi_reports_with_cfv])
    return export
    # export_new.to_excel("/Users/matthias.funke/Downloads/bento/bi_objects.xlsx", index=False)

h = hashlib.new('sha256')

def my_hash(text):
    h.update(text.encode())
    my_hex_str = str(h.hexdigest())
    return my_hex_str[0:6]

def get_parent(path):
    components = path.split("//")
    my_len = len(components)
    if my_len==1:
        return
    else:
        return "//".join(components[0:my_len-1])

def get_fqn_for_folder(external_id ,bi_folders):
    parent_folder = bi_folders.loc[bi_folders.external_id==external_id]
    if parent_folder.shape[0]==1:
        fqn = parent_folder.fully_qualified_name.iloc[0]
        return fqn

def get_fqn_for_report(external_id ,bi_reports):
    parent_report = bi_reports.loc[bi_reports.external_id==external_id]
    if parent_report.shape[0]==1:
        fqn = parent_report.fully_qualified_name.iloc[0]
        return fqn

def make_human_readable(df):
    """
    Removes external ID, parent_folder and report
    Creates fully qualified names based on folder / report / col
    :param df:
    :return:
    """
    grouped = df.groupby('otype')

    bi_folders = grouped.get_group("bi_folder")
    bi_folders['fully_qualified_name'] = bi_folders.path
    bi_folders['parent'] = bi_folders.path.apply(get_parent)
    bi_folders['otype'] = "bi_folder"

    bi_reports = grouped.get_group("bi_report")
    bi_reports['parent'] = bi_reports.parent_folder.apply(get_fqn_for_folder, bi_folders=bi_folders)
    bi_reports['fully_qualified_name'] = bi_reports.apply(lambda report: f"{report.parent}||{report['name']}",
                                                          axis=1)
    bi_reports['otype'] = "bi_report"

    # bi_report_columns = grouped.get_group("bi_report_column")
    # bi_report_columns['parent'] = bi_report_columns.report.apply(get_fqn_for_report, bi_reports=bi_reports)
    # bi_report_columns['fully_qualified_name'] = bi_report_columns.apply(lambda col: f"{col.parent}||{col['name']}",
    #                                                       axis=1)
    # bi_report_columns['otype'] = "bi_report_column"
    #
    # df = pd.concat([bi_folders, bi_reports, bi_report_columns])
    df = pd.concat([bi_folders, bi_reports])
    # prove that these are no longer needed
    df['external_id']=""
    df['parent_folder']=""
    df['report']=""
    return df

def create_df_with_external_ids(df):
    """
    Uses the "parent" column to create external IDs by using hash function
    :param df:
    :return:
    """
    # make sure there is a column called action. Implemented: "delete"
    if "action" not in df.columns:
        df["action"]=""

    for i, my_folder in df.loc[df.otype=="bi_folder", :].iterrows():
        log_me(f"Working on {i}:{my_folder['name']}")
        if my_folder.action == "delete":
            alation_1.delete_bi_object("bi_folder", my_folder.external_id, bi_server_id)
            continue
        if pd.isna(my_folder['parent']):
            df.loc[i, "fully_qualified_name"] = f"{my_folder['name']}"
        else:
            try:
                df.loc[i, "fully_qualified_name"] = f"{my_folder['parent']}//{my_folder['name']}"
                df.loc[i, "parent_folder"] = df.loc[df.fully_qualified_name==my_folder['parent'], 'external_id'].iloc[0]
            except:
                log_me(f"Parent for {my_folder['name']} does not seem to exist!")
                continue
        df.loc[i, "external_id"] = my_hash(df.loc[i, "fully_qualified_name"])

    for j, my_report in df.loc[df.otype=="bi_report", :].iterrows():
        if my_report.action == "delete":
            alation_1.delete_bi_object("bi_report", my_report.external_id, bi_server_id)
            continue
        if pd.isna(my_report['parent']):
            log_me(f"Report {my_report} does not have parent.")
        else:
            parent_name = my_report['parent']
            parent_object = df.loc[df.fully_qualified_name==parent_name]
            df.loc[j, "fully_qualified_name"] = (parent_name + "||" + my_report['name'])
            df.loc[j, "parent_folder"] = parent_object['external_id'].iloc[0]
        df.loc[j, "external_id"] = my_hash(df.loc[j, "fully_qualified_name"])

    for k, my_report_col in df.loc[df.otype=="bi_report_column", :].iterrows():
        if my_report_col.action == "delete":
            alation_1.delete_bi_object("bi_report_column", my_report_col.external_id, bi_server_id)
            continue
        if pd.isna(my_report_col['parent']):
            log_me(f"Report col {my_report_col} does not have parent.")
        else:
            parent_name = my_report_col['parent']
            parent_object = df.loc[df.fully_qualified_name==parent_name]
            df.loc[k, "fully_qualified_name"] = (parent_name + "||" + my_report_col['name'])
            df.loc[k, "report"] = parent_object['external_id'].iloc[0]
        df.loc[k, "external_id"] = my_hash(df.loc[k, "fully_qualified_name"])

    log_me(f"Removing {df.loc[df.action=='delete'].shape[0]} deleted objects from data before proceeding...")
    df = df.loc[df.action!="delete"]

    if df.external_id.duplicated().any():
        log_me(f"Cannot proceed with duplicated external IDs. They need to be unique")
        exit(1)
    df['id'] = None
    return df


if __name__ == "__main__":

    # --- Log into the source instance
    alation_1 = AlationInstance(config.args['host'],
                                config.args['refresh_token'],
                                config.args['user_id'],
                                verify=True
                                )

    my_templates = alation_1.get_templates()
    # print(my_templates.title)

    # piece about OCF ds

    # piece about Terms
    term_api = "/integration/v2/term/"
    params = dict(search='los angeles')
    # watch it -- this results in "los" or "angeles"
    my_terms = alation_1.generic_api_get(term_api, params=params)
    my_terms_df = pd.DataFrame(my_terms)

    for t in my_terms_df.itertuples():
        title = t.title
        id = t.id
        custom_info = [f"{f['field_name']}={f['value']}" for f in t.custom_fields]
        custom_info_formatted = ";".join(custom_info)
        glossary_id = t.glossary_ids
        print(f"{id:4} | {title:40} | {custom_info_formatted:40} | {glossary_id}")

    # how to identify the glossary? we have to search for them
    search_param = json.dumps({"otypes": ["glossary_v3"]}, separators=(',', ':'))
    search_result = alation_1.generic_api_get('/integration/v1/search/',
                                              params=dict(filters=search_param))
    my_glossaries = dict()
    for g in search_result.get("results"):
        my_glossaries[int(g.get('id'))] = g.get('title')

    # So now we can do it again with glossary titles and template titles
    for t in my_terms_df.itertuples():
        title = t.title
        id = t.id
        custom_info = [f"{f['field_name']}={f['value']}" for f in t.custom_fields]
        custom_info_formatted = ";".join(custom_info)
        glossary_id = [my_glossaries[g] for g in t.glossary_ids]
        template = my_templates.at[t.template_id, 'title']
        print(f"{id:4} | {title:40} | {custom_info_formatted:40} | {glossary_id} | {template}")

    # Alternative
    my_terms_df['otype'] = 'glossary_term'
    my_terms_df = my_terms_df.set_index(['otype', 'id'])
    cfv = alation_1.get_custom_field_values_for_oids(my_terms_df.index)


    alation_2 = AlationInstance("https://beta-sandbox.alationproserv.com/",
                                refresh_token="sempbnpV6bHIJWUiifmfHtLvqOM5IJjrTZCR_SyPtDxv7_h8NB6jh60hS7ido_LqpAYf21Ml7qk_uRkJ5hG6JA",
                                user_id=9,
                                verify=True
                                )

    ocf = alation_2.generic_api_get("/integration/v2/datasource/")
    ocf_df = pd.DataFrame(ocf)


    existing = ocf_df.loc[6]
    ocf_params = dict(
        uri=existing.uri,
        connector_id=10, # installed this connector as No. 10
        db_username=existing.db_username,
        db_password='secret',
        title=existing.title,
        description=existing.description,
        deployment_setup_complete=True,
        private=True,
        #is_virtual=existing.is_virtual,
        #is_hidden=existing.is_hidden,
    )
    ocf_clone = alation_1.generic_api_post("/integration/v2/datasource/", body=ocf_params)
    # unfortunately no DELETE API yet
    datasource_id = ocf_clone.get('id')
    auth = alation_1.generic_api_get(f"/integration/v1/datasource/{datasource_id}/configuration_check/service_account_authentication/")
    schemas = alation_1.generic_api_get(f"/integration/v2/datasource/{35}/available_schemas/")
    print("All done.")
