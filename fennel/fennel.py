import pandas as pd
import time
from psycopg2 import connect, extensions, DatabaseError

# - provide a time stamped log message
def log_me(txt):
    try:
        print("{} {}".format(
            time.strftime("%Y-%b-%d %H:%M:%S", time.localtime()),
            txt))
    except:
        print("{} Formatting issue with a log message.".format(
            time.strftime("%Y-%b-%d %H:%M:%S", time.localtime())))

# main class for this code
class DatabaseProxy(object):
    # Constructor: make a connection to the database using the credentials provided
    def __init__(self, credentials, isolation_level=extensions.ISOLATION_LEVEL_AUTOCOMMIT):
        self.connection = connect(
            host=credentials['HOST'],
            port=credentials['PORT'],
            user=credentials['USER'],
            password=credentials['PASSWORD'],
            dbname=credentials['NAME']
        )

    # get data from the rosemeta instance of the source
    # includes name and ID for each schema, table, column=attribute
    def get_data_from_rosemeta(self):
        log_me('Fetching data from rosemeta')
        # use the Pandas library to read the table into a DataFrame
        df = pd.read_sql(
            '''
            WITH "datasource" as (SELECT
                   ds.id as ds_id, 
                   ds.title as ds_title, 
                   ds.dbtype as ds_type
          FROM public.rosemeta_datasource ds where ds.deleted is false),
"schema"
     AS ( SELECT   s.id AS schema_id,
                   s.original_name AS schema_name,
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
                   t.original_name AS table_name,
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
                   s.original_name AS schema_name,
                   a.table_id AS table_id,
                   t.original_name AS table_name,
                   a.id AS attribute_id,
                   a.title AS attribute_title,
                   a.description AS attribute_description,
                   a.original_name AS attribute_name,
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


# The main program starts here
# This script exports rosemeta to a pickle file
if __name__ == "__main__":
    desc = "Copies physical metadata from rosemeta to another pgSQL"
    defaults = {}
    # Hard coded access to standard ports of rosemeta, user "alation", no password
    exporter1 = DatabaseProxy(credentials=dict(
        HOST='/tmp',
        PORT=5432,
        USER='alation',
        PASSWORD=None,
        NAME='rosemeta'
    ))
    # Download the DataFrame
    data = exporter1.get_data_from_rosemeta()
    # Save the pickle file in the working directory
    log_me("Saving file")
    data.to_pickle("rosemeta.gzip")
    # Close the database connection
    exporter1.connection.close()
    log_me("All done!")
