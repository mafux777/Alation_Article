import uuid

from sudoku.bento import AlationInstance
from sudoku.Article import Article
from sudoku.alationutil import log_me, extract_files
import config
import pandas as pd
from itertools import repeat
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
    bi_report_cols = alation_1.get_bi_report_cols(bi_server)
    cfv = alation_1.get_custom_field_values_for_oids(bi_report_cols.index)
    bi_report_cols_with_cfv = bi_report_cols.merge(cfv, left_index=True, right_index=True, how="left")

    export = pd.concat([bi_folders_with_cfv, bi_reports_with_cfv, bi_report_cols_with_cfv])
    # export = pd.concat([bi_folders_with_cfv, bi_reports_with_cfv])
    return export
    # export_new.to_excel("/Users/matthias.funke/Downloads/bento/bi_objects.xlsx", index=False)

h = hashlib.new('sha256')

def my_hash(text):
    h.update(text.encode())
    my_hex_str = str(h.hexdigest())
    return my_hex_str

if __name__ == "__main__":

    # --- Log into the source instance
    alation_1 = AlationInstance(config.args['host'],
                                config.args['username'],
                                config.args['password'],
                                config.args['refresh_token'],
                                config.args['user_id'],
                                )

    # bi_server = alation_1.create_bi_server("http://alation.com", f"V. BI {datetime.now(timezone.utc).isoformat()}")
    # bi_server_id = bi_server.get("Server IDs")[0]

    bi_server_id = 136

    df = pd.read_excel("/Users/matthias.funke/Downloads/bento/upload test weekdays.xlsx")
    validated = alation_1.validate_headers(df.columns)

    # create a unique ID for each object
    bi_folders = alation_1.get_bi_folders(bi_server_id)
    for i, my_folder in df.loc[df.otype=="bi_folder", :].iterrows():
        if pd.isna(my_folder['parent']):
            df.loc[i, "fully_qualified_name"] = f"{bi_server_id}//{my_folder['name']}"
        else:
            df.loc[i, "fully_qualified_name"] = f"{my_folder['parent']}//{my_folder['name']}"
            df.loc[i, "parent_folder"] = df.loc[df.fully_qualified_name==my_folder['parent'], 'external_id'].iloc[0]
        df.loc[i, "external_id"] = my_hash(df.loc[i, "fully_qualified_name"])

    for j, my_report in df.loc[df.otype=="bi_report", :].iterrows():
        if pd.isna(my_report['parent']):
            log_me(f"Report {my_report} does not have parent.")
        else:
            parent_name = my_report['parent']
            parent_object = df.loc[df.fully_qualified_name==parent_name]
            df.loc[j, "fully_qualified_name"] = (parent_name + "||" + my_report['name'])
            df.loc[j, "parent_folder"] = parent_object['external_id'].iloc[0]
        df.loc[j, "external_id"] = my_hash(df.loc[j, "fully_qualified_name"])

    for k, my_report_col in df.loc[df.otype=="bi_report_column", :].iterrows():
        if pd.isna(my_report_col['parent']):
            log_me(f"Report col {my_report_col} does not have parent.")
        else:
            parent_name = my_report_col['parent']
            parent_object = df.loc[df.fully_qualified_name==parent_name]
            df.loc[k, "fully_qualified_name"] = (parent_name + "||" + my_report_col['name'])
            df.loc[k, "report"] = parent_object['external_id'].iloc[0]
        df.loc[k, "external_id"] = my_hash(df.loc[k, "fully_qualified_name"])

    df.to_excel("hashed_ext_ids.xlsx", index=False)

    if df.external_id.duplicated().any():
        log_me(f"Cannot proceed with duplicated external IDs. They need to be unique")
        exit(1)
    df['id'] = None

    alation_1.sync_bi(bi_server_id, df)
    pre_validated_df = df.loc[:, list(validated)]
    pre_validated_df['relevant'] = pre_validated_df.apply(lambda x: x.notna().any(), axis=1)

    validated_df = df.loc[pre_validated_df.relevant, ['otype', 'id', 'external_id'] + list(validated)].sort_index()
    alation_1.upload_lms(validated_df, validated, bi_server_id)

    print("All done.")




