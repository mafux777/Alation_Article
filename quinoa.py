import uuid
import random
from sudoku.bento import AlationInstance
from sudoku.alationutil import log_me
import config
import pandas as pd
from datetime import datetime, timezone


if __name__ == "__main__":

    # --- Log into the source instance
    alation_1 = AlationInstance(config.args['host'],
                                config.args['username'],
                                config.args['password'],
                                config.args['refresh_token'],
                                config.args['user_id'],
                                )

    my_art = alation_1.find_article_by_title("Charles De Gaulle (CDG)", 44)
    alation_1.modify_single_field_on_article(my_art, 44, 'My Rich Text', 'CDG is the biggest airport in Paris')
    # cols = ['External Object ID', 'Rule ID', 'Compliant records',
    #        'Non-Compliant Records', 'Total Records', 'Compliance %',
    #        'DQ rule owner', 'Rule description', 'Template', 'Article',
    #        'Table Reference', 'Column Reference']

    # df = pd.read_excel("./sampledata/quinoa.xlsx")
    test_table = 897
    table = alation_1.get_fully_qualified_name("table", test_table)
    policy_template_id = 54
    policy_group_id = 4


    # Let's come up with N silly rules and test values
    inputs = []
    syllables = ['ka', 'di', 'ra', 'po', 'song', 'tung', 'ly', 'do', 'tip', 'ly', 'ding', 'dread']
    verb = ['needs to be', 'cannot be', 'has to always', 'should never', 'exceeds', 'occasionally mismatches']
    qual = ['GOOD', 'WARNING', 'ALERT']
    for i in range(3):
        x = random.randrange(100)
        u = str(uuid.uuid4())[0:8]
        my_test_rule = {
            'External Object ID': f'{u}',
            'Rule ID' : f'Rule {i:05}',
            'Compliant records' : x,
            'Non-Compliant Records': 100-x,
            'Total Records' : 100,
            'Compliance %': f'{x/100:%}',
            'status': f'{random.choice(qual)}',
            'DQ rule owner':'matthias.funke@alation.com',
            'Rule description':f'{"".join(random.choices(syllables, k=random.randint(1,3)))}'
                               f' {random.choice(verb)} '
                               f'{"".join(random.choices(syllables, k=random.randint(1,3)))}',
            'Template': 'Data Policy Article',
            'Article': f'Data Policy {u}/{i:05}',
            'Table Reference': table,
            'Column Reference': None,
            'policy_id': int(alation_1.create_new_policy(policy_template_id, policy_group_id)),
        }
        inputs.append(my_test_rule)
    inputs_df = pd.DataFrame(inputs)

    # create a new policy for each of the rules
    remap = {
        'policy_id' : 'id',
        'Article': 'title',
        'Rule description': 'description',
        'Table Reference': 'Object Under Policy:table',
    }
    logical_metadata_for_policies =  inputs_df.loc[:, list(remap)].rename(columns=remap)
    logical_metadata_for_policies['otype'] = 'business_policy'
    validated = alation_1.validate_headers(logical_metadata_for_policies.columns)
    alation_1.upload_lms(logical_metadata_for_policies, validated)

    # Prepare payload for Data Quality API
    payload = {}

    renaming_fields = {
        'External Object ID': 'field_key',
        'Rule ID': 'name',
        'Rule description': 'description',
    }
    fields_df = inputs_df.loc[:, list(renaming_fields)].rename(columns=renaming_fields)
    fields_df['type'] = 'STRING'
    payload['fields'] = list(fields_df.apply(lambda x: dict(x), axis=1))

    renaming_values = {
        'External Object ID': 'field_key',
        'Table Reference': 'object_key',
        'Compliance %': 'value',
        'status': 'status'
    }
    renaming_values_df = inputs_df.loc[:, list(renaming_values)].rename(columns=renaming_values)
    renaming_values_df['object_type'] = 'TABLE'
    renaming_values_df['url'] = logical_metadata_for_policies.id.apply(lambda id: f"/policy/{id}/")
    payload['values'] = list(renaming_values_df.apply(lambda x: dict(x), axis=1))

    # post for the first time
    alation_1.post_data_health(payload)

    # post for the second time
    del payload['fields']
    alation_1.post_data_health(payload)

    print(f"Check out {alation_1.host}/table/{test_table}/health")




