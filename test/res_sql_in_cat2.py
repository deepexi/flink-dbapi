sql_create_catalog_cat2 = """
CREATE CATALOG cat2 WITH (
  'type'='iceberg',
  'catalog-type'='hive',
  'uri'='thrift://hms.dormi.io:9083',
  'clients'='5',
  'property-version'='1',
  'warehouse'='s3a://warehouse/'
);
"""

sql_create_db_cat2_db2 = """
create database if not exists cat2.cat2_db2;
"""

sql_drop_table_cat2_db2_t2 = """drop table if exists cat2.cat2_db2.t2"""

sql_create_table_cat2_db2_t2 = """
CREATE TABLE `cat2`.`cat2_db2`.`t2` (
  `id` INT,
  `age` INT
); 
"""

sql_drop_table_cat2_db2_t2_mirror = """drop table if exists cat2.cat2_db2.t2_mirror"""

sql_ctas_cat2_db2_t2_mirror = """
CREATE TABLE `cat2`.`cat2_db2`.`t2_mirror`
as
select * from cat2.cat2_db2.t2
"""

sql_create_cat2_db2_t2_mirror = """
CREATE TABLE `cat2`.`cat2_db2`.`t2_mirror` (
  `id` INT,
  `age` INT
);
"""
