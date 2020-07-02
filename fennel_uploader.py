import pandas as pd
import re
from psycopg2 import connect, extensions, sql, DatabaseError
from AlationInstance import AlationInstance
from alationutil import log_me
import config

# Configuration: base url of the on-prem instance, to preserve links for debug purposes
base_url = "https://demo-sales.alationcatalog.com"

# Arguments for the DataSource API
"""
dbtype	Yes	Currently the only supported types are MySQL and Oracle.
host	Yes*	The host of the data source.
port	Yes*	The port of the data source.
deployment_setup_complete	No	If the deployment setup is complete. When set to true, complete data source information is required, else, only partial information is required. Defaults to True.
title	Yes**	The title of the data source.
db_username	Yes***	The service account username.
db_password	No	The service account password.
private	No	If the data source is private. Defaults to false.
is_virtual	No	If the data source is virtual. Defaults to false.
description	No	The description of the data source.
dbname	No	The database name of the data source
"""

# Create a DataSource in Alation
def create_datasource(alation_instance, host, port, title, db_username, db_password, description, dbname):
    body=dict(
        dbtype='postgresql',
        host=host,
        port=port,
        deployment_setup_complete=True,
        title=title,
        db_username=db_username,
        db_password=db_password,
        private=False,
        is_virtual=False,
        description=description,
        dbname=dbname
    )
    # Call the DataSource creation API
    ds = alation_instance.generic_api_post(api='/integration/v1/datasource/', body=body, official=True)
    return ds

# Update the DataSource with a list of schemas to extract
def update_datasource(alation_instance, ds_id, schemas):
    # / integration / v1 / datasource / < ds_id > / sync_configuration / metadata_extraction /
    """
    cron_extraction	Yes	The extraction schedule in crontab format (minute, hour, day of month, month of year, day of week)
    disable_auto_extraction	No	True if the extraction schedule should not be executed, false to run extraction according to cron_extraction
    limit_schemas	Yes	Schemas to include.
    exclude_schemas	Yes	Schemas to exclude.
    remove_filtered_schemas	Yes	Whether to remove filtered schemas.
    """
    params=dict(force_refresh=True)
    log_me("Running MDE")
    # /data/57/list_schemas/
    mde = alation_instance.generic_api_post(api=f'/data/{ds_id}/list_schemas/')
    mde = alation_instance.generic_api_get(api=f'/integration/v1/datasource/{ds_id}/available_schemas/',
                                           params=params, official=True)
    body=dict(cron_extraction="0 4 * * *",
              disable_auto_extraction=False,
              limit_schemas=schemas,
              exclude_schemas=[],
              remove_filtered_schemas=True
              )
    sync = alation_instance.generic_api_put(api=f'/integration/v1/datasource/{ds_id}/sync_configuration/metadata_extraction/',
                                           body=body, official=True)
    mde = alation_instance.generic_api_post(api=f'/data/{ds_id}/extract_now/',
                                           params=params, official=False)
    return mde

# main class for this code
class DatabaseProxy(object):
    # Construct a DatabaseProxy with the credentials provided.
    # We are using a Postgres database in the cloud to serve as clones for the "real" databases on premise
    def __init__(self, credentials, isolation_level=extensions.ISOLATION_LEVEL_AUTOCOMMIT):
        self.connection = connect(
            host=credentials['HOST'],
            port=credentials['PORT'],
            user=credentials['USER'],
            password=credentials['PASSWORD'],
            dbname=credentials['NAME']
        )
        self.connection.set_isolation_level(isolation_level)
    # Send a SQL statement to the cloud database
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
        except (Exception, DatabaseError) as error:
            log_me(f"Error while executing {statement}:\n{error}")
            # Remember all the types that are not supported by Postgres
            # by saving the type name in dict missing
            match = re.search(r'type "(\w+)" does not exist', error.pgerror)
            if match:
                if match.group(1) in missing:
                    missing[match.group(1)] +=1
                else:
                    log_me(error)
                    missing[match.group(1)] = 1
            cursor.close()
            self.connection.rollback()
        finally:
            # closing database connection.
            if final:
                cursor.close()
                self.connection.close()
                # log_me("PostgreSQL connection is closed")

    # Add a source comment which will be available in Alation, too
    def add_comment(self, otype, id, part1, part2=None):
        mapping=dict(table='table',
                     schema='schema',
                     column='attribute')

        href=f"{base_url}/{mapping[otype]}/{id}/"
        comment_text = f'<p>Original source: <a href="{href}" rel="noopener noreferrer" target="_blank">{href}</a></p>'
        if part2:
            statement=f'COMMENT ON {otype} "{part1}"."{part2}"' + f" IS '{comment_text}'"
        else:
            statement = f'COMMENT ON {otype} "{part1}"' + f" IS '{comment_text}'"
        self.send_statement(statement)

# Main program
if __name__ == "__main__":
    desc = "Copies physical metadata from rosemeta to another pgSQL"
    log_me("Reading data from pickle file")
    data = pd.read_pickle("rosemeta.gzip")

    # Replace non-postgres types with equivalent postgres types
    substitutions={
        r'^datetime': 'timestamp',
        r'^timestamp_ltz': 'timestamp with time zone',
        r'^(timestamp_ntz|smalltimestamp)': 'timestamp',
        r'^string': 'text',
        r'^(long|medium|short)?text(\(\d+\))?' : 'text',
        r'^(number|double|float|numeric)(\(\d+\))?': 'numeric',
        r'^integer(\d)?' : 'integer',
        r'^(big|small)?integer(\d)?' : 'integer',
        r'^(big|small)?integer(\(\d+\))?' : 'integer',
        r'^(big|small)?_integer(\d)?' : 'integer[]',
        # int with (digit)
        r'^(tiny|byte|big)?int(\(\d+\))?': 'integer',
        # int with digit
        r'^(tiny|byte|big)?int\d?': 'integer',
        r'^bytes$': 'text',
        r'^geography' : 'polygon',
        r'^(varchar|nvarchar)(\(\d+\))?' : 'text',
        r'^(na|hstore|id|picklist|reference|textarea|url|address|email|clob)(\(\d+\))?' : 'text',
        r'^(num|variant|enum|measures|uniqueidentifier|accountnumeric)(\(\d+\))?': 'numeric',
    }

    missing = {}
    seen = {}

    # Ensure the attribute type is postgres compatible
    def modify_attribute(attr):
        for pattern, replacement in substitutions.items():
            match = re.search(pattern, attr, flags=re.IGNORECASE)
            if match:
                # Remember the substition and print it for debuggin
                if not match.group(0) in seen:
                    log_me(f'{match.group(0)} -> {replacement}')
                    seen[match.group(0)] = True
                attr = re.sub(pattern, replacement, attr, flags=re.IGNORECASE)
                break
        return attr
    # Format the attribute so that it can be included in the CREATE TABLE statement
    def format_attribute(attr):
        attr_name = attr['attribute_name']
        attr_type = modify_attribute(attr['attribute_type'])
        attr_null = "NOT NULL" if not attr['nullable'] else ""
        # Postgres only supports one primary key, so commenting out (does not make sense to have only one)
        # attr_prmk = "PRIMARY KEY" if attr['is_primary_key'] else ""
        return f'"{attr_name}" {attr_type} {attr_null}'# {attr_prmk}'

    log_me("Preparing & converting attributes")
    data['create_attribute'] = data.apply(format_attribute, axis=1)

    log_me("Logging in to Alation Instance")
    alation = AlationInstance(config.args['host'], config.args['username'], config.args['password'], verify=False)
    all_datasources={}

    # create data source, if necessary
    all_ds = data.groupby(['ds_id', 'ds_title', 'ds_type'])
    # Iterate through all data sources, identified by their ID, title, and type
    for ds, schema_df in all_ds:
        # generate a unique name for the data source
        # substitute _ for brackets and spaces
        ds_name = re.sub(r'[[\]\s]', '_', f"005_{ds[1]}_{ds[2]}_{ds[0]}")
        # keep track of all names and their ds_ids on the target
        all_datasources[ds_name] = alation.look_up_ds_by_name(ds_name)
        log_me(f"Working on {ds_name} in the cloud database")
        # Hard coded credentials for the cloud database
        credentials = {
            'HOST': 'fennel2.cluster-cingqyuv6npc.us-east-2.rds.amazonaws.com',
            'PORT': 5433,
            'USER': 'postgres',
            'PASSWORD': 'alation123',
            'NAME': ds_name
        }
        try:
            # See if we can just connect to the database (i.e. it already exists)
            exporter2 = DatabaseProxy(credentials)
            if not all_datasources[ds_name]:
                # If it's not already known in Alation, create it there
                new_ds = create_datasource(alation, host=credentials['HOST'],
                                           port=credentials['PORT'],
                                           title=ds_name,
                                           description="This datasource was created by Fennel",
                                           db_username=credentials['USER'],
                                           db_password=credentials['PASSWORD'],
                                           dbname=ds_name)
                log_me(f"Created data source: {config.args['host']}/data/{new_ds['id']}/")
                all_datasources[ds_name] = new_ds['id']

        except:
            # The database does not exist in the cloud already.
            # Log in using the "default" database
            credentials['NAME'] = 'fennel'
            exporter2 = DatabaseProxy(credentials, extensions.ISOLATION_LEVEL_AUTOCOMMIT)

            # set the isolation level for the connection's cursors
            # will raise ActiveSqlTransaction exception otherwise
            log_me(f"Creating database {ds_name}")
            exporter2.send_statement(f'CREATE DATABASE "{ds_name}"', final=True) # include ds_id

            # Now that the database exists, we can connect to it
            credentials['NAME'] = ds_name

            # Create the data source in Alation
            new_ds = create_datasource(alation, host=credentials['HOST'],
                              port=credentials['PORT'],
                              title=ds_name,
                              description="This datasource was created by Fennel",
                              db_username=credentials['USER'],
                              db_password=credentials['PASSWORD'],
                              dbname=ds_name)
            log_me(f"Created data source: {config.args['host']}/data/{new_ds['id']}/")
            # Remember that this datasource exists
            all_datasources[ds_name] = new_ds['id']
            exporter2 = DatabaseProxy(credentials)

        list_of_schemas=[]
        # Create all the schemas
        log_me("Creating schemas (if not exists)")
        for schema, _ in schema_df.groupby(['schema_name', 'schema_id']):
            statement = f'CREATE SCHEMA IF NOT EXISTS "{schema[0]}"'
            exporter2.send_statement(statement)
            exporter2.add_comment("schema", schema[1], schema[0])
            list_of_schemas.append(schema[0])
        # Create all the tables
        log_me("Creating tables (if not exists)")
        for table, attribute_df in schema_df.groupby(['schema_name', 'table_name']):
            all_attributes = ",".join(set(attribute_df['create_attribute']))
            table_statement = f'CREATE TABLE IF NOT EXISTS "{table[0]}"."{table[1]}"({all_attributes})'
            table_id = attribute_df.iloc[0, 5]
            exporter2.send_statement(table_statement)
            exporter2.add_comment("table", table_id, table[0], table[1])
        exporter2.connection.close()
        # Now that the database in the cloud has been hydrated with schemas, tables, and columns...
        # We can run MDE
        update_datasource(alation, all_datasources[ds_name], list_of_schemas)

    log_me(f'These types were not handled: {missing}')
