with
"schema" as
(
  SELECT
    S.id as schema_id,
    S.name as schema_name,
    S.title as schema_title,
    S.description as schema_description,
    S.ds_id as ds_id
  FROM public.rosemeta_schema S
  order by ds_id, schema_id
),
"table" as
(
  SELECT
    S.ds_id as ds_id,
    T."schema" as schema_name,
    T.id as table_id,
    T.title as table_title,
    T.name as table_name,
    T.description as table_description,
    S.id as schema_id
    FROM public.rosemeta_table T
    JOIN public.rosemeta_schema S on S.id=T.schema_obj_id
    where T.excluded = False /* Exclude tables marked as non-browsable */
  order by ds_id, schema_id, T.id
),
"attribute" as
(
  SELECT
    A.ds_id as ds_id,
    A.schema_id as schema_id,
    S.name as schema_name,
    A.table_id as table_id,
    T.name as table_name,
    A.id as attribute_id,
    A.title as attribute_title,
    A.description as attribute_description,
    A.name as attribute_name,
    A.data_type as attribute_type
    FROM public.rosemeta_attribute A
    JOIN public.rosemeta_table T on T.id=A.table_id
    JOIN public.rosemeta_schema S on S.id=A.schema_id
    where table_id in (select table_id from "table") /* only keep browsable tables */
  order by ds_id, schema_id, A.table_id, A.position
),
logical_metadata as
(
    -- SCHEMA --
    select
      ds_id || '.' || schema_name as key,
      schema_title as title,
      schema_description as description
    from "schema"
    UNION ALL
    -- TABLE --
    select
      ds_id || '.' || schema_name || '.' || table_name as key,
      table_title as title,
      table_description as description
    from "table"

    UNION ALL
    -- ATTRIBUTE --
    select
      ds_id || '.' || schema_name || '.' || table_name || '.' || attribute_name as key,
      attribute_title as title,
      attribute_description as description
    from "attribute"
),
physical_metadata as
(
    -- SCHEMA --
    select ds_id, schema_id,
      --ds_id || '.' || schema_name as key,
      schema_name as key,
      NULL as table_type,
      NULL as column_type
    from "schema"
    UNION ALL
    -- TABLE --
    select ds_id, schema_id,
      --ds_id || '.' || schema_name || '.' || table_name as key,
      schema_name || '.' || table_name as key,
      'table' as table_type,
      NULL as column_type
    from "table"

    UNION ALL
    -- ATTRIBUTE --
    select ds_id, schema_id,
      --ds_id || '.' || schema_name || '.' || table_name || '.' || attribute_name as key,
      schema_name || '.' || table_name || '.' || attribute_name as key,
      NULL as table_type,
      attribute_type as column_type
    from "attribute"
)


select key, table_type, column_type from physical_metadata
    where ds_id=4 /* only one data source */
--select * from logical_metadata;