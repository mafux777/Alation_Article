from sudoku.bento import AlationInstance
from sudoku.alationutil import log_me
import config
import pandas as pd
from datetime import datetime, timezone
import hashlib
from sudoku.bento import AlationInstance
from sudoku.alationutil import log_me
import config
import pandas as pd
from datetime import datetime, timezone
import hashlib


if __name__ == "__main__":

    # --- Log into the source instance
    alation_1 = AlationInstance(config.args['host'],
                                config.args['username'],
                                config.args['password'],
                                config.args['refresh_token'],
                                config.args['user_id'],
                                )
    df = alation_1.get_dataflows()
    tables = []
    for i, my_dataflow in df.iterrows():
        for p in my_dataflow['paths']:
            log_me(p)
            for input in p[0]:
                if input['otype']=='table' and not input.get('is_temp'):
                    tables.append(dict(key=input['key'],
                                       table_url=f"/table/{alation_1.reverse_qualified_name('table', input['key'])}/",
                                       dataflow_key=p[1][0]['key'],
                                       dataflow_url=my_dataflow['full_url'],
                                       dataflow_title=my_dataflow['title'],
                                       ))
            for output in p[2]:
                if output['otype']=='table' and not output.get('is_temp'):
                    tables.append(dict(key=output['key'],
                                       table_url=f"/table/{alation_1.reverse_qualified_name('table', output['key'])}/",
                                       dataflow_key=p[1][0]['key'],
                                       dataflow_url=my_dataflow['full_url'],
                                       dataflow_title=my_dataflow['title'],
                                       ))

    tables_df = pd.DataFrame(tables)
    tables_df.to_excel("./my_tables_with_lineage.xlsx")

