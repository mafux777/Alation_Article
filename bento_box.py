import uuid

from sudoku.bento import AlationInstance
from sudoku.Article import Article
from sudoku.alationutil import log_me, extract_files
import config
import pandas as pd
from itertools import repeat
from datetime import datetime, timezone

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

if __name__ == "__main__":

    # --- Log into the source instance
    alation_1 = AlationInstance(config.args['host'],
                                config.args['username'],
                                config.args['password'],
                                config.args['refresh_token'],
                                config.args['user_id'],
                                )

    # d = alation_1.get_fully_qualified_name("data", 1)
    # s = alation_1.get_fully_qualified_name("schema", 10)
    # s1 = alation_1.reverse_qualified_name("schema", "3.SUPERSTORE.PUBLIC")
    # s2 = alation_1.reverse_qualified_name("schema", "11.big-query-lab-331019.myDataset")
    # t1 = alation_1.reverse_qualified_name("table", "11.big-query-lab-331019.myDataset.Personnel Nested")
    # t2 = alation_1.reverse_qualified_name("table", "3.SUPERSTORE.PUBLIC.SUPERSTORE_REPORTING")
    # a1 = alation_1.reverse_qualified_name("column", "3.SUPERSTORE.PUBLIC.SUPERSTORE_REPORTING.ORDER_DATE")
    # a2 = alation_1.reverse_qualified_name("column", "11.big-query-lab-331019.myDataset.myTable.word")
    # a = alation_1.reverse_qualified_name("article", "Norwich International Airport")
    # t = alation_1.reverse_qualified_name("term", "London Stansted")
    # u = alation_1.reverse_qualified_name("user", "His Excellency (jon.lanham@alation.com)")
    # g = alation_1.reverse_qualified_name("group", "Fishermen")
    # t = alation_1.get_fully_qualified_name("table", 3)
    # c = alation_1.get_fully_qualified_name("column", 6918)
    # a = alation_1.get_fully_qualified_name("article", 5)
    # z = alation_1.get_fully_qualified_name("term", 91538)
    # u = alation_1.get_fully_qualified_name("user", 4)
    # g = alation_1.get_fully_qualified_name("groupprofile", 8)

    # export = get_bi_source(alation_1, 115)
    # export.reset_index().to_excel("/Users/matthias.funke/Downloads/bento/bi_server_115.xlsx", index=False)

    df = pd.read_excel("/Users/matthias.funke/Downloads/bento/bi_server_115_up.xlsx")
    validated = alation_1.validate_headers(df.columns)

    # see if there are any reports missing...
    # df.loc[(df.otype=="bi_report")&(df.name.isin(export.loc[export.otype=="bi_report", "name"])), :]

    mapper = {}
    def map_to_uuid(external_id):
        if external_id in mapper:
            return mapper.get(external_id)
        else:
            mapper[external_id] = str(uuid.uuid4())
            return mapper.get(external_id)

    df['external_id'] = df.external_id.apply(lambda x: str(uuid.uuid4()) if pd.isna(x) else x)
    if df.external_id.duplicated().any():
        log_me(f"Cannot proceed with duplicated external IDs. They need to be unique")
        exit(1)
    df['id'] = None
    # df['external_id'] = df.external_id.apply(map_to_uuid)
    # df['parent_folder'] = df.parent_folder.apply(lambda f: mapper.get(f))
    # df['subfolders'] = df.loc[df.otype=='bi_folder', "subfolders"].apply(lambda f: [mapper.get(f0, 'unknown') for f0 in f])
    # df['parent_reports'] = df.loc[df.otype=='bi_report', "parent_reports"].apply(lambda f: [mapper.get(f0) for f0 in f])
    # df['report'] = df.loc[df.otype=='bi_report_column', 'report'].apply(lambda f: mapper.get(f, ''))
    # df.to_excel("/Users/matthias.funke/Downloads/bento/bi_server_115_up.xlsx", index=False)
    # df.to_excel("/Users/matthias.funke/Downloads/bento/output.xlsx")
    #
    bi_server_id = 124
    # bi_server = alation_1.create_bi_server("http://alation.com", f"V. BI {datetime.now(timezone.utc).isoformat()}")
    # bi_server_id = bi_server.get("Server IDs")[0]
    alation_1.sync_bi(bi_server_id, df)
    pre_validated_df = df.loc[:, list(validated)]
    pre_validated_df['relevant'] = pre_validated_df.apply(lambda x: x.notna().any(), axis=1)

    validated_df = df.loc[pre_validated_df.relevant, ['otype', 'id', 'external_id'] + list(validated)].sort_index()
    alation_1.upload_lms(validated_df, validated, bi_server_id)

    print("All done.")




