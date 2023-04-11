sql_create_catalog_cat2 = """
CREATE CATALOG cat2 WITH (
  'type'='iceberg',
  'catalog-type'='hive',
  'uri'='thrift://hms.dormi.io:9083',
  'clients'='5',
  'property-version'='1',
  'warehouse'='s3a://warehouse/'
);
""".strip()

sql_create_db_cat2_db2 = """
create database if not exists cat2.cat2_db2;
""".strip()

sql_drop_table_cat2_db2_t2 = """drop table if exists cat2.cat2_db2.t2""".strip()

sql_create_table_cat2_db2_t2 = """
CREATE TABLE `cat2`.`cat2_db2`.`t2` (
  `id` INT,
  `age` INT
);
""".strip()

sql_insert_table_cat2_db2_t2_with_hint = """
/** 'job_name'='group3:sql_insert_table_cat2_db2_t2_with_hint' 'test_query' */

insert into `cat2`.`cat2_db2`.`t2`
    (`id`, `age`)
values
    (1, 1),
    (2, 2)
/** */
""".strip()

sql_select_table_cat2_db2_t2 = """
/** 'job_name'='group4:sql_select_table_cat2_db2_t2' */

select * from `cat2`.`cat2_db2`.`t2`
""".strip()

sql_drop_table_cat2_db2_t2_mirror = """drop table if exists cat2.cat2_db2.t2_mirror""".strip()

sql_ctas_cat2_db2_t2_mirror = """
/** 'job_name'='group5:sql_ctas_cat2_db2_t2_mirror' */

CREATE TABLE `cat2`.`cat2_db2`.`t2_mirror`
as
select * from cat2.cat2_db2.t2
""".strip()

sql_create_cat2_db2_t2_mirror = """
CREATE TABLE `cat2`.`cat2_db2`.`t2_mirror` (
  `id` INT,
  `age` INT
);
""".strip()

sql_table_create_datagen = """
CREATE TABLE datagen (
    id BIGINT,
    price        DECIMAL(32,2),
    -- buyer        ROW<first_name STRING, last_name STRING>,
    order_time   TIMESTAMP(3)
) WITH (
    'connector' = 'datagen',
    -- 'number-of-rows' = '10',
    'rows-per-second' = '10',
    'fields.id.kind'='sequence',
    'fields.id.start'='1',
    'fields.id.end'='100'
);

""".strip()

sql_table_create_blackhole = """
CREATE TABLE default_catalog.default_database.blackhole
WITH ('connector' = 'blackhole')
LIKE default_catalog.default_database.datagen (EXCLUDING ALL);
""".strip()

sql_datagen_2_blackhole = """
/** 'job_name'='group6:sql_datagen_2_blackhole' */

INSERT INTO default_catalog.default_database.blackhole SELECT * from default_catalog.default_database.datagen;
""".strip()
