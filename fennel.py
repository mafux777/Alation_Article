import argparse
import pandas as pd
import psycopg2
import time

# - provide a time stamped log message
def log_me(txt):
    try:
        print ("{} {}".format(
            time.strftime("%Y-%b-%d %H:%M:%S", time.localtime()),
            txt))
    except:
        print ("{} Formatting issue with a log message.".format(
            time.strftime("%Y-%b-%d %H:%M:%S", time.localtime())))

# main class for this code
class DatabaseProxy(object):
    def __init__(self, credentials):
        self.connection = psycopg2.connect(
            host=credentials['HOST'],
            port=credentials['PORT'],
            user=credentials['USER'],
            password=credentials['PASSWORD'],
            dbname=credentials['NAME']
        )

    def get_conn(self):
        return self.connection

    # get data from the rosemeta instance of the source
    # includes name and ID for each schema, table, column=attribute
    def get_data_from_rosemeta(self):
        log_me('fetching data...')
        df = pd.read_sql( 
            '''
            WITH "datasource" as (SELECT
                   ds.id as ds_id, 
                   ds.title as ds_title, 
                   ds.dbtype as ds_type
          FROM public.rosemeta_datasource ds where ds.deleted is false),
"schema"
     AS ( SELECT   s.id AS schema_id,
                   s.name AS schema_name,
                   s.title AS schema_title,
                   s.description AS schema_description,
                   s.ds_id AS ds_id
          FROM     public.rosemeta_schema s
          INNER JOIN     "datasource" using (ds_id)
          WHERE    s.excluded = FALSE 
),
"table"
     AS ( SELECT   s.ds_id AS ds_id,
                   t."schema" AS schema_name,
                   t.id AS table_id,
                   t.title AS table_title,
                   t.name AS table_name,
                   t.description AS table_description,
                   s.id AS schema_id
          FROM     public.rosemeta_table t
                   JOIN public.rosemeta_schema s
                   ON s.id=t.schema_obj_id
          WHERE    t.excluded = FALSE and s.id in (select schema_id from "schema")
          ORDER BY ds_id,
                   schema_id,
                   t.id )
SELECT   a.ds_id AS ds_id,
                   ds_title,
                   ds_type, 
                   a.schema_id AS schema_id,
                   s.name AS schema_name,
                   a.table_id AS table_id,
                   t.name AS table_name,
                   a.id AS attribute_id,
                   a.title AS attribute_title,
                   a.description AS attribute_description,
                   a.name AS attribute_name,
                   a.data_type AS attribute_type,
                   a.position as position,
                   a.nullable as nullable,
                   a.is_primary_key as is_primary_key
          FROM     public.rosemeta_attribute a
                   JOIN public.rosemeta_table t
                   ON a.table_id=t.id
                   JOIN public.rosemeta_schema s
                   ON a.schema_id=s.id
                   JOIN "datasource" on "datasource".ds_id=a.ds_id
          WHERE    a.excluded = FALSE
                   and table_id IN (SELECT table_id
                                FROM   "table")
          ORDER BY ds_id,
                   schema_id,
                   a.table_id,
                   a.position;
            ''',
            con=self.connection
        )
        return df

    def send_statement(self, statement, final=False):
        try:
            #log_me(statement)
            cursor = self.connection.cursor()
            if not cursor:
                raise("Connection error")
            cursor.execute(statement)
            #psql_instructions.append(statement)
            self.connection.commit()
            cursor.close()
        except (Exception, psycopg2.DatabaseError) as error:
            print(f"Error while executing {statement}", error)
            cursor.close()
            self.connection.rollback()
        finally:
            # closing database connection.
            if final:
                cursor.close()
                self.connection.close()
                print("PostgreSQL connection is closed")


if __name__ == "__main__":
    desc = "Copies physical metadata from rosemeta to another pgSQL"
    defaults = {}
    # try:
    #     from alation_conf import conf
    #     import alation_util.pgsql_util as pgsql_util
    #
    #     prefix = 'pgsql.config.'
    #     defaults['HOST'] = conf[prefix + 'host']
    #     defaults['PORT'] = conf[prefix + 'port']
    #     defaults['USER'] = conf[prefix + 'user']
    #     defaults['PASSWORD'] = conf.get_decrypted(prefix + 'password')
    # except ImportError:
    #     log_me('Alation config does not exist.'
    #           'Run this script the help (-h) option for info on manually providing credentials.')

    # parser = argparse.ArgumentParser(description=desc)
    # parser.add_argument("-H", "--hostname", default=defaults.get('HOST', 'localhost'),
    #                     help="Host name of the machine on which PostgreSQL is running.")
    # parser.add_argument("-U", "--username", default=defaults.get('USER', 'alation'),
    #                     help="Database user to be used for authentication.")
    # parser.add_argument("-w", "--password", default=defaults.get('PASSWORD', ''),
    #                     help="Database password.")
    # parser.add_argument("-d", "--dbname", default='rosemeta',
    #                     help="name of database instance to connect to.")
    # parser.add_argument("-s", "--schema", default='public',
    #                     help="name of database instance to connect to. "
    #                          "Defaults to PostreSQL's 'public' schema")
    # parser.add_argument("-p", "--port", default=defaults.get('PORT', 5432),
    #                     help="Port number. Defaults to PostgreSQL's default of 5432.")
    # args = parser.parse_args()
    #
    # dbname = args.dbname
    # credentials = {
    #     'HOST': args.hostname,
    #     'PORT': args.port,
    #     'USER': args.username,
    #     'PASSWORD': args.password,
    #     'NAME': args.dbname
    # }
    # schema = args.schema

    #exporter1 = DatabaseSchemaExporter(credentials, schema)
    #data = exporter1.get_data_from_rosemeta()
    #data.to_pickle("rosemeta.gzip")
    #exporter1.connection.close()

    data = pd.read_pickle("rosemeta.gzip")
    import re

    substitutions={
        r'\(\d+\)': '',
        r'datetime': 'timestamp',
        r'timestamp_ltz': 'timestamp with time zone',
        r'timestamp_ntz': 'timestamp',
        r'string': 'text',
        r'longtext' : 'text',
        r'number': 'numeric',
        r'double': 'numeric',
        r'tinyint': 'integer',
        r'bytes': 'text',
        r'geography' : 'polygon',
        r'na' : 'text'
    }

    def modify_attribute(attr):
        for pattern, replacement in substitutions.items():
            attr = re.sub(pattern, replacement, attr, flags=re.IGNORECASE)
        return attr

    # data['create_schema']    = 'CREATE SCHEMA IF NOT EXISTS "' + data['schema_name'] + '";\n'
    # data['create_table']     = 'CREATE TABLE IF NOT EXISTS "'  + data['schema_name'] + '"' + '."' + data['table_name'] + '"'
    # data['create_attribute'] = '"' + data['attribute_name'] + '" ' + data['attribute_type'].apply(modify_attribute)

    def format_attribute(attr):
        attr_name = attr['attribute_name']
        attr_type = modify_attribute(attr['attribute_type'])
        attr_null = "NOT NULL" if not attr['nullable'] else ""
        attr_prmk = "PRIMARY KEY" if attr['is_primary_key'] else ""
        return f'"{attr_name}" {attr_type} {attr_null} {attr_prmk}'

    data['create_attribute'] = data.apply(format_attribute, axis=1)

    # grouped = data.groupby(['schema_name', 'table_name', 'attribute_name'])
    grouped = data.groupby(['schema_name', 'table_name'])

    credentials2 = {
        'HOST': 'fennel2.cluster-cingqyuv6npc.us-east-2.rds.amazonaws.com',
        'PORT': 5433,
        'USER': 'postgres',
        'PASSWORD': 'alation123',
        'NAME': 'monday'
    }
    exporter2 = DatabaseProxy(credentials2)
    # for schema in all_schemas:
    # for schema in set(data['schema_name']):
    #     statement = f'CREATE SCHEMA IF NOT EXISTS "{schema}"'
    #     exporter2.send_statement(statement)

    for name, group in grouped:
        all_attributes = ",".join(set(group['create_attribute']))
        table = f'CREATE TABLE IF NOT EXISTS "{name[0]}"."{name[1]}"({all_attributes})'
        exporter2.send_statement(table)
        table_id = group.iloc[0, 5]
        table_comment = f'<p>Original source: <a href="https://demo-sales.alationcatalog.com/table/{table_id}/" rel="noopener noreferrer" target="_blank">https://demo-sales.alationcatalog.com/table/{table_id}/</a></p>'
        exporter2.send_statement(f'COMMENT ON TABLE "{name[0]}"."{name[1]}"' + f" IS '{table_comment}'")

    # # create all databases
    # database_name = ...
    # """create database {database_name}"""
    exporter2.connection.close()


