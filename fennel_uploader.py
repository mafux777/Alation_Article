import pandas as pd
import re
from psycopg2 import connect, extensions, sql, DatabaseError
from AlationInstance import AlationInstance
from alationutil import log_me
import config
import random

# Configuration: base url of the on-prem instance, to preserve links for debug purposes
base_url = "https://demo-sales.alationcatalog.com"
DEFAULT = 'fennel'
CREDS = {
            'HOST': 'fennel2.cluster-cingqyuv6npc.us-east-2.rds.amazonaws.com',
            'PORT': 5433,
            'USER': 'postgres',
            'PASSWORD': 'alation123',
            'NAME': DEFAULT
        }


# Arguments for the DataSource API
"""
dbtype	Yes	
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
    return alation_instance.generic_api_post(api='/integration/v1/datasource/', body=body, official=True)

# Update the DataSource with a list of schemas to extract (or not extract)
def update_datasource(alation_instance, ds_id, schemas, warnings=None):
    if warnings:
        # get all flags to see if we need to append our warnings
        flags_raw = alation_instance.generic_api_get(api=f"/integration/flag/?oid={ds_id}&otype=data", official=True)
        if flags_raw:
            existing_warning_text = ""
            existing_warning_id = None
            for flag in flags_raw:
                # there can be at most one warning
                if flag.get('flag_type')=='WARNING':
                    existing_warning_text = flag.get('flag_reason')
                    existing_warning_id = flag.get('id')
            new_warning_text = existing_warning_text + "Missing tables: " +", ".join(warnings)
            # There is a warning already -- just append and hope the admin will take action before the warning
            # gets too long to display in Alation
            if existing_warning_id:
                update_flag = alation_instance.generic_api_put(api=f"/integration/flag/{existing_warning_id}/",
                                                               body=dict(flag_reason=new_warning_text),
                                                               official=True)
            else: # create a new warning flag
                new_flag = alation_instance.generic_api_post(api=f"/integration/flag/",
                                                               body=dict(flag_type="WARNING",
                                                                         subject=dict(id=int(ds_id), otype="data"),
                                                                         flag_reason=new_warning_text),
                                                               official=True)
        else: # create the very first flag, namely the warning
            new_flag = alation_instance.generic_api_post(api=f"/integration/flag/",
                                                         body=dict(flag_type="WARNING",
                                                                   subject=dict(id=int(ds_id), otype="data"),
                                                                   flag_reason="Missing tables: " +", ".join(warnings)),
                                                         official=True)

    """
    cron_extraction	Yes	The extraction schedule in crontab format (minute, hour, day of month, month of year, day of week)
    disable_auto_extraction	No	True if the extraction schedule should not be executed, false to run extraction according to cron_extraction
    limit_schemas	Yes	Schemas to include.
    exclude_schemas	Yes	Schemas to exclude.
    remove_filtered_schemas	Yes	Whether to remove filtered schemas.
    """
    params=dict(force_refresh=True)
    log_me("Running MDE")
    mde = alation_instance.generic_api_post(api=f'/data/{ds_id}/list_schemas/')
    mde = alation_instance.generic_api_get(api=f'/integration/v1/datasource/{ds_id}/available_schemas/',
                                           params=params, official=True)
    body=dict(cron_extraction="{r} 0 * * *".format(r=random.randint(0, 59)),
              disable_auto_extraction=False,
              limit_schemas=[],
              exclude_schemas=['pg_temp_1', 'pg_toast', 'pg_toast_temp_1', 'public'],
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
            self.connection.commit()
            cursor.close()
            return True
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
            return False
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
        comment_text = f'Original source: {href}'
        if part2:
            statement = f'COMMENT ON {otype} "{part1}"."{part2}"' + f" IS '{comment_text}'"
        else:
            statement = f'COMMENT ON {otype} "{part1}"' + f" IS '{comment_text}'"
        self.send_statement(statement)


    def get_existing_schemas(self):
        return pd.read_sql("SELECT catalog_name ,schema_name, schema_owner FROM information_schema.schemata",
                            con=self.connection)


# Main program
if __name__ == "__main__":
    desc = "Copies physical metadata from rosemeta to another pgSQL"
    log_me("Reading data from pickle file")
    data = pd.read_pickle("rosemeta.gzip")
    # Replace non-postgres types with equivalent postgres types
    substitutions={
        r'^(small)?datetime': 'timestamp',
        r'^timestamp_ltz': 'timestamp with time zone',
        r'^(timestamp_ntz|smalltimestamp)': 'timestamp',
        r'^string': 'text',
        r'^text\(max\)' : 'text',
        r'^(long|medium|short)?text(\(\d+\))?': 'text',
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
        r'^geography': 'polygon',
        r'^(varchar|nvarchar)\(max\)': 'text',
        r'^(varchar|nvarchar)(\(\d+\))?': 'text',
        r'^varbinary\(max\)': 'bytea',
        r'^binary\(max\)': 'bytea',
        r'^(var)?binary(\(\d+\))?': 'bytea',
        r'^(na|hstore|id|picklist|reference|textarea|url|address|email|clob)(\(\d+\))?': 'text',
        r'^(num|variant|enum|measures|uniqueidentifier|accountnumeric)(\(\d+\))?': 'numeric',
    }

    missing = {}
    seen = {}

    # Ensure the attribute type is postgres compatible
    def modify_attribute(attr):
        for pattern, replacement in substitutions.items():
            match = re.search(pattern, attr, flags=re.IGNORECASE)
            if match:
                # Remember the substitution and print it for debugging
                if not match.group(0) in seen:
                    log_me(f'{match.group(0)} -> {replacement}')
                    seen[match.group(0)] = True
                attr = replacement
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
    all_datasources = {}
    credentials = CREDS  # configuration is now on top of the file

    # create data source, if necessary
    all_ds = data.groupby(['ds_id', 'ds_title', 'ds_type'])
    # Iterate through all data sources, identified by their ID, title, and type
    for ds, schema_df in all_ds:
        # generate a unique name for the data source
        # substitute _ for brackets and spaces
        #ds_name = re.sub(r'[[\]\s]', '_', f"020_{ds[1]}_{ds[2]}_{ds[0]}")
        ds_name = re.sub(r'[[\]\s]', '_', ds[1])
        # keep track of all names and their ds_ids on the target
        all_datasources[ds_name] = alation.look_up_ds_by_name(ds_name)
        log_me(f"Working on {ds_name} in the cloud database")
        credentials['NAME'] = ds_name
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
        except DatabaseError as my_exception:
            log_me(my_exception)
            # The database does not exist in the cloud already.
            # Log in using the "default" database
            credentials['NAME'] = DEFAULT

            # set the isolation level for the connection's cursors
            # will raise ActiveSqlTransaction exception otherwise
            exporter2 = DatabaseProxy(credentials, extensions.ISOLATION_LEVEL_AUTOCOMMIT)
            log_me(f"Creating database {ds_name}")
            exporter2.send_statement(f'CREATE DATABASE "{ds_name}"', final=True) # include ds_id

            # Now that the database exists, we can set up the data source in Alation
            credentials['NAME'] = ds_name
            new_ds = create_datasource(alation, host=credentials['HOST'],
                              port=credentials['PORT'],
                              title=ds_name,
                              description="This datasource was created by Fennel",
                              db_username=credentials['USER'],
                              db_password=credentials['PASSWORD'],
                              dbname=ds_name)
            log_me(f"Created data source: {config.args['host']}/data/{new_ds['id']}/")
            # Remember that this datasource exists and the ID
            all_datasources[ds_name] = new_ds['id']
            # create a new connection to the newly created database
            exporter2 = DatabaseProxy(credentials)

        # Find out which schemas already exist so we can drop them (note the Alation catalog page will stay)
        existing_schemas = exporter2.get_existing_schemas()
        for schema in existing_schemas.loc[existing_schemas.schema_owner==credentials['USER']].itertuples():
            cat = schema.catalog_name
            sch = schema.schema_name
            log_me(f"Dropping {sch}")
            statement = f'DROP SCHEMA "{sch}" CASCADE'
            exporter2.send_statement(statement)

        # Create all the schemas and remember them
        log_me("Creating schemas (if not exists)")
        list_of_schemas=[]
        for schema, _ in schema_df.groupby(['schema_name', 'schema_id']):
            statement = f'CREATE SCHEMA IF NOT EXISTS "{schema[0]}"'
            if exporter2.send_statement(statement):
                exporter2.add_comment("schema", schema[1], schema[0])
                list_of_schemas.append(schema[0])
            else:
                # could not create the schema
                # this must be investigated so abort fatally
                raise Exception(f'Fatal error creating schema {schema[0]}')

        # Create all the tables
        log_me("Creating tables (if not exists)")
        table_warnings = list()
        for table, attribute_df in schema_df.groupby(['schema_name', 'table_name']):
            all_attributes = ",".join(list(attribute_df['create_attribute']))
            table_statement = f'CREATE TABLE IF NOT EXISTS "{table[0]}"."{table[1]}"({all_attributes})'
            table_id = attribute_df.iloc[0, 5]
            if exporter2.send_statement(table_statement):
                exporter2.add_comment("table", table_id, table[0], table[1])
            else:
                # could not create the table
                table_warnings.append(f"{table[0]}.{table[1]}")
        exporter2.connection.close()
        # Now that the database in the cloud has been hydrated with schemas, tables, and columns...
        # We can run MDE
        update_datasource(alation, all_datasources[ds_name], list_of_schemas, table_warnings)

    log_me(f'These types were not handled: {missing}')
    log_me("All done.")


