#!/usr/bin/env python
import argparse
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from collections import namedtuple
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
            WITH "schema"
     AS ( SELECT   s.id AS schema_id,
                   s.name AS schema_name,
                   s.title AS schema_title,
                   s.description AS schema_description,
                   s.ds_id AS ds_id
          FROM     public.rosemeta_schema s
          WHERE    s.excluded = FALSE
          ORDER BY ds_id,
                   schema_id ),
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
    # a convenience function to correct the data types and eliminate NaN values
    def write_line(self, row):
        # convert all IDs and position to int
        ds_id        = int(row.ds_id)
        schema_id    = int(row.schema_id)
        table_id     = int(row.table_id)
        attribute_id = int(row.attribute_id)
        position     = int(row.position) if pd.notna(row.position) else None

        # Eliminate NaN values
        attribute_title = row.attribute_title if pd.notna(row.attribute_title) else None
        nullable        = row.nullable        if pd.notna(row.nullable)        else None
        is_primary_key  = row.is_primary_key  if pd.notna(row.is_primary_key)  else None
        attribute_type  = row.attribute_type  if pd.notna(row.attribute_type)  else 'default'

        database_column = namedtuple('database_column', ['ds_id',
                    'schema_id',
                    'schema_name',
                    'table_id',
                    'table_name',
                    'attribute_id',
                    'attribute_title',
                    'attribute_description',
                    'attribute_name',
                    'attribute_type',
                    'position',
                    'nullable',
                    'is_primary_key'])
        my_tuple = database_column(ds_id,
                    schema_id,
                    row.schema_name,
                    table_id,
                    row.table_name,
                    attribute_id,
                    attribute_title,
                    row.attribute_description,
                    row.attribute_name,
                    attribute_type,
                    position,
                    nullable,
                    is_primary_key)

        return my_tuple
    # put all the data into one table on the target
    def upload_to_remote(self, data, table_name):
        cur = self.connection.cursor()
        insert_query = f'insert into {table_name} (ds_id,schema_id,schema_name,table_id,table_name,' \
            f'attribute_id,attribute_title,attribute_description,attribute_name,attribute_type,position,' \
            f'nullable,is_primary_key) values %s'
        execute_values(cur, insert_query, data)
        self.connection.commit()
        cur.close()

    def send_statement(self, statement, final=False):
        try:
            log_me(statement)
            cursor = self.connection.cursor()
            if not cursor:
                raise("Connection error")
            cursor.execute(statement)
            psql_instructions.append(statement)
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
        r'string': 'text'
    }

    def modify_attribute(attr):
        for k, v in substitutions.items():
            attr = re.sub(k, v, attr)
        return attr
    data['create_schema']    = 'CREATE SCHEMA IF NOT EXISTS "' + data['schema_name'] + '";\n'
    data['create_table']     = 'CREATE TABLE IF NOT EXISTS "'  + data['schema_name'] + '"' + '."' + data['table_name'] + '"'
    data['create_attribute'] = '"' + data['attribute_name'] + '" ' + data['attribute_type'].apply(modify_attribute)

    #schemas = data.groupby(['create_schema']).size().reset_index().iloc[0,:]#.aggregate(lambda x: ",".join(x))
    schemas = data['create_schema'].unique()
    tables  = data.groupby(['create_table'])['create_attribute'].aggregate(lambda x: ",".join(x)).reset_index()

    #psql_1 = psql_0.reset_index()
    tables['create_attribute'] = "(" + tables['create_attribute'] + ");\n"
    tables=tables.apply(lambda x: " ".join(dict(x).values()), axis=1)
    # psql_3=tables.sum()
    # with open("fennel.sql", "w") as psql_4:
    #     psql_4.write(psql_3)



    credentials2 = {
        'HOST': 'fennel2.cluster-cingqyuv6npc.us-east-2.rds.amazonaws.com',
        'PORT': 5433,
        'USER': 'matthias.funke',
        'PASSWORD': 'alation123',
        'NAME': 'fennel'
    }

    exporter2 = DatabaseProxy(credentials2)
    #data = [exporter2.write_line(t) for t in data.itertuples()]

    ds_created={}
    schemas_created={}
    tables_created={}
    columns_created={}

    psql_instructions=[]


    # for schema in all_schemas:
    #     """create schema {schema}"""
    for statement in list(schemas):
        exporter2.send_statement(statement)
    # # create all databases
    # database_name = ...
    # """create database {database_name}"""
    # # create all schemas
    # # create all tables
    for statement in list(tables):
        exporter2.send_statement(statement)
    # for table in all_tables:
    #     """create table {schema}.{table}("""
    #     for column in all_columns:
    #     """{column} {attribute_type}"""
    for item in data: # a list of tuples...
        # --- Data Source ---
        if item.ds_id not in ds_created:
            credentials = {
                'HOST': 'fennel2.cluster-cingqyuv6npc.us-east-2.rds.amazonaws.com',
                'PORT': 5433,
                'USER': 'postgres',
                'PASSWORD': 'alation123',
                'NAME': 'formula1'}
            ds_created[item.ds_id] = dict(
                statement=f"CREATE DATABASE database_{item.ds_id};",
                connection=psycopg2.connect(
            host=credentials['HOST'],
            port=credentials['PORT'],
            user=credentials['USER'],
            password=credentials['PASSWORD'],
            dbname=credentials['NAME'])#DatabaseProxy(credentials)
            )
            # send_statement(ds_created[item.ds_id]['connection'],
            #                ds_created[item.ds_id]['statement'])
        # --- Schema ---
        if item.schema_id not in schemas_created:
            schemas_created[item.schema_id] = dict(statement=f'CREATE SCHEMA IF NOT EXISTS "{item.schema_name}";')
            send_statement(ds_created[item.ds_id]['connection'], schemas_created[item.schema_id]['statement'])
        # --- Table ---
        if item.table_id not in tables_created:
            tables_created[item.table_id] = dict(
                statement=f'CREATE TABLE IF NOT EXISTS "{item.schema_name}"."{item.table_name}" (dummy text);')
            send_statement(ds_created[item.ds_id]['connection'], tables_created[item.table_id]['statement'])
        # --- Column ---
        if item.attribute_id not in columns_created:
            columns_created[item.attribute_id] = f"{item.attribute_name} {item.attribute_type}"
            if item.nullable:
                columns_created[item.attribute_id] += ' NULL'
            if item.is_primary_key:
                columns_created[item.attribute_id] += ' PRIMARY KEY'

    with open("my_psql.sql", "w") as psql_file:
        psql_file.write(psql_instructions.join('\n'))

    exporter2.upload_to_remote(data, "okra")
    exporter2.connection.close()


