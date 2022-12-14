import json
import pandas as pd
from pathlib import Path
import yaml
from yaml.loader import SafeLoader
import sys

tableType = {
    0: "TABLE",
    1: "VIEW",
    3: "SYNONYM",
}

columnType = {
    0: "OTHER",
    1: "BOOL",
    2: "DATE",
    3: "INT",
    4: "FLOAT",
    5: "STRING",
    6: "TIME",
    7: "TIMESTAMP"
}

objects = {
    'catalog': {"files":('databaseCatalog.txt', 'DatabaseCatalog.dump')},
    'schema': {"files":('schema.txt', 'Schema.dump')},
    'table': {"files":('table.txt', 'Table.dump'), "conversion": tableType},
    'column': {"files":('column.txt', 'Attribute.dump'), "conversion": columnType},
}

def recursive_grab(text, conversion):
    if isinstance(text, dict):
        my_dict = {}
        for k, v in text.items():
            if k.endswith("_"):
                v = recursive_grab(v, conversion)
                if k=="normalized_":
                    if isinstance(v, int):
                        # for some keys, we need to look up the text value
                        my_dict[k[0:len(k) - 1]] = conversion.get(v)
                else:
                    my_dict[k[0:len(k) - 1]] = v
        return my_dict
    else:
        return text


df = {}
# Assume the worst case: database has a catalog (Alation schema with dots)
catalog = True

def concat_schema(row, catalog, type):
    if catalog:
        if type=="catalog":
            return
        elif type=="schema":
            return f"{row['id.databaseCatalog.original']}.{row['id.name.original']}".lower()
        else:
            return f"{row['id.databaseCatalog.original']}.{row['id.schema.original']}".lower()
    else:
        if type=="schema":
            return f"{row['id.name.original']}".lower()
        else:
            return f"{row['id.schema.original']}".lower()



# Open the file and load the file
with open(sys.argv[1], "r") as f:
    my_config = yaml.load(f, Loader=SafeLoader)

blacklist = my_config.get('blacklist', [])

for type, my_object in objects.items():
    input, output = my_object["files"]

    # Check if this database type provides a file for catalog. If not, remember catalog=False
    if not Path(input).exists():
        catalog = False
        continue

    # Open the input files and convert the format as needed
    conversion = my_object.get("conversion", {})
    with open(input) as f:
        info = [recursive_grab(json.loads(r), conversion) for r in f.readlines()]

    # Blacklist certain IDs by checking the fully qualified schema name
    df[type] = pd.json_normalize(info)
    df[type]['lower_schema'] = df[type].apply(concat_schema, axis=1, catalog=catalog, type=type)
    s = df[type]['blacklisted'] = df[type]['lower_schema'].isin(blacklist)
    blacklisted_ids = list(s[s].index)

    with open(output, "w") as output_file:
        for i, t in enumerate(info):
            if i not in blacklisted_ids:
                output_file.write(f"{json.dumps(t)}\n")
            else:
                print(f"{type} {t['id']} is blacklisted")

# Create CSV file for manual upload (if so desired...)
ds_id = my_config.get('ds_id')

my_schema = df['schema'].loc[~df['schema']['blacklisted'], ['lower_schema']]
my_schema['key'] = my_schema['lower_schema'].apply(lambda x: f"{x}")
# my_schema['key'] = my_schema['lower_schema'].apply(lambda x: f"{ds_id}.{x}")
del my_schema['lower_schema']

my_table = df['table'].loc[~df['table']['blacklisted'], ['lower_schema', 'id.name.original', 'type.normalized']]
my_table['key'] = my_table.apply(lambda r: f"{r['lower_schema']}.{r['id.name.original'].lower()}", axis=1)
# my_table['key'] = my_table.apply(lambda r: f"{ds_id}.{r['lower_schema']}.{r['id.name.original'].lower()}", axis=1)
my_table = my_table.rename(columns={"type.normalized":"table_type"}).loc[:, ['key', 'table_type']]

my_column = df['column'].loc[~df['column']['blacklisted'], ['lower_schema', 'id.table.original', 'id.name.original', 'type.normalized']]
my_column['sanitized'] = my_column['id.name.original'].apply(lambda s: s.replace(".", "_"))
my_column['key'] = my_column.apply(lambda r: f"{r['lower_schema']}.{r['id.table.original'].lower()}.{r['sanitized'].lower()}", axis=1)
# my_column['key'] = my_column.apply(lambda r: f"{ds_id}.{r['lower_schema']}.{r['id.table.original'].lower()}.{r['id.name.original'].lower()}", axis=1)
my_column = my_column.rename(columns={"type.normalized":"column_type"}).loc[:, ['key', 'column_type']]

my_amalgam = pd.concat([my_schema, my_table, my_column]).fillna("")
my_file_name = my_config.get("csv_file_name")
if my_file_name:
    my_amalgam.to_csv(my_file_name)

print(f"All done.")