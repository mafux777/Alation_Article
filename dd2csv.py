from string import lower
import pandas as pd
import sys

def split_schema_table(statement):
    words = statement.split()
    if lower(words[0])=="create" and lower(words[1])=="table":
        components = words[2].split(".")
        return components[0].rstrip('"').lstrip('"'), components[1].rstrip('"').lstrip('"')
    else:
        return

def dd2pd(statement):
    result = []
    lines = statement.split("\n")
    for line in lines:
        if not line:
            continue
        if split_schema_table(line):
            schema, table = split_schema_table(line)
            result.append(dict(key=schema))
            result.append(dict(key=schema+"."+table, table_type="table"))
            continue
        components = line.split()
        if len(components)>1:
            col = components[0].rstrip('"').lstrip('"')
            if lower(col)=="primary" and lower(components[1])=="key":
                n=dict(key=schema+"."+table+".index1",
                       index_type='primary',
                       column_names=[s.rstrip(',').rstrip(')').lstrip('(') for s in components[2:]])
            else:
                n = dict(key=schema+"."+table+"."+col,
                      table_type=None,
                      column_type=components[1].rstrip(','))
            result.append(n)
    return pd.DataFrame(result, columns=['key', 'table_type', 'column_type'])

if __name__ == "__main__":
    args = sys.argv
    with open(args[1]) as f:
        df = dd2pd(f.read())
        df.to_csv(args[2], index=False)
