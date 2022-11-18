from sudoku.bento import AlationInstance
from sudoku.alationutil import log_me
import config
import pandas as pd
from datetime import datetime, timezone
import hashlib

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
                                config.args['username'],
                                config.args['password'],
                                config.args['refresh_token'],
                                config.args['user_id'],
                                )


    bi_server = alation_1.create_bi_server("http://alation.com", f"V. BI {datetime.now(timezone.utc).isoformat()}")
    bi_server_id = bi_server.get("Server IDs")[0]
    #bi_server_id = 159

    # --- prepare existing download to be more end-user friendly
    df = get_bi_source(alation_1, 159).reset_index()
    df.to_excel("/Users/matthias.funke/Downloads/bento/source_159.xlsx")
    df = make_human_readable(df)
    df.to_excel("/Users/matthias.funke/Downloads/bento/human_readable_159.xlsx", index=False)


    # df = pd.read_excel("/Users/matthias.funke/Downloads/bento/hashed_ext_ids.xlsx")
    # create a unique ID for each object
    df = create_df_with_external_ids(df)
    df.to_excel("/Users/matthias.funke/Downloads/bento/hashed_ext_ids_159.xlsx", index=False)

    # sync_bi relies on external IDs being correct!
    alation_1.sync_bi(bi_server_id, df)

    # Take care of the logical metadata
    validated = alation_1.validate_headers(df.columns)
    pre_validated_df = df.loc[:, list(validated)]
    pre_validated_df['relevant'] = pre_validated_df.apply(lambda x: x.notna().any(), axis=1)

    validated_df = df.loc[pre_validated_df.relevant, ['otype', 'id', 'external_id'] + list(validated)].sort_index()
    alation_1.upload_lms(validated_df, validated, bi_server_id)

    print(f"Check out {alation_1.host}/bi/v2/server/{bi_server_id}/")




