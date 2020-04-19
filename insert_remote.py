#!/usr/bin/env python
import argparse
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
import pickle


class DatabaseSchemaExporter(object):
    def __init__(self, credentials, schema):
        self.schema = schema
        self.connection = psycopg2.connect(
            host=credentials['HOST'],
            port=credentials['PORT'],
            user=credentials['USER'],
            password=credentials['PASSWORD'],
            dbname=credentials['NAME']
        )

    def get_data_from_rosemeta(self):
        cur = self.connection.cursor()

        print('fetching data...')
        cur.execute(
            '''
            SELECT
                (ds_id ,
                schema_name,
                table_name,
                attribute_name,
                table_type,
                column_type) from public.fennel;
            ''',
            (self.schema,)
        )

        data = cur.fetchall()
        cur.close()
        return data

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

        my_tuple = (ds_id,
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

    def upload_to_remote(self, data):
        cur = self.connection.cursor()
        insert_query = 'insert into gherkin (ds_id,schema_id,schema_name,table_id,table_name,attribute_id,attribute_title,attribute_description,attribute_name,attribute_type,position,nullable,is_primary_key) values {}'.format('%s')
        execute_values(cur, insert_query, data)
        self.connection.commit()
        cur.close()

if __name__ == "__main__":
    desc = "Copies physical metadata from rosemeta to another pgSQL"
    defaults = {}
    try:
        from alation_conf import conf
        import alation_util.pgsql_util as pgsql_util

        prefix = 'pgsql.config.'
        defaults['HOST'] = conf[prefix + 'host']
        defaults['PORT'] = conf[prefix + 'port']
        defaults['USER'] = conf[prefix + 'user']
        defaults['PASSWORD'] = conf.get_decrypted(prefix + 'password')
    except ImportError:
        print('Alation config does not exist.'
              'Run this script the help (-h) option for info on manually providing credentials.')

    parser = argparse.ArgumentParser(description=desc)
    parser.add_argument("-H", "--hostname", default=defaults.get('HOST', 'localhost'),
                        help="Host name of the machine on which PostgreSQL is running.")
    parser.add_argument("-U", "--username", default=defaults.get('USER', 'alation'),
                        help="Database user to be used for authentication.")
    parser.add_argument("-w", "--password", default=defaults.get('PASSWORD', ''),
                        help="Database password.")
    parser.add_argument("-d", "--dbname", default='rosemeta',
                        help="name of database instance to connect to.")
    parser.add_argument("-s", "--schema", default='public',
                        help="name of database instance to connect to. "
                             "Defaults to PostreSQL's 'public' schema")
    parser.add_argument("-p", "--port", default=defaults.get('PORT', 5432),
                        help="Port number. Defaults to PostgreSQL's default of 5432.")
    args = parser.parse_args()

    dbname = args.dbname
    credentials = {
        'HOST': args.hostname,
        'PORT': args.port,
        'USER': args.username,
        'PASSWORD': args.password,
        'NAME': args.dbname
    }
    schema = args.schema

    exporter1 = DatabaseSchemaExporter(credentials, schema)
    # data = exporter1.get_data_from_rosemeta()
    # exporter1.connection.close()
    d = pd.read_csv('data_dictionary.out', sep='|', header=0,
                        names=["ds_id", "schema_id", "schema_name", "table_id", "table_name", "attribute_id",
                               "attribute_title", "attribute_description", "attribute_name", "attribute_type",
                               "position", "nullable", "is_primary_key"])
    d = d[d.ds_id.notna()]

    credentials2 = {
        'HOST': 'fennel2.cluster-cingqyuv6npc.us-east-2.rds.amazonaws.com',
        'PORT': 5433,
        'USER': 'matthias.funke',
        'PASSWORD': 'alation123',
        'NAME': 'fennel'
    }

    exporter2 = DatabaseSchemaExporter(credentials2, schema)

    data = [exporter2.write_line(t) for t in d.itertuples()]
    exporter2.upload_to_remote(data)
    exporter2.connection.close()
