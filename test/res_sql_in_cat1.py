sql_create_catalog_cat1 = """
CREATE CATALOG cat1 WITH ('type'='hive', 'hive-conf-dir'='/opt/flink/conf')
""".strip()

sql_create_cat1_db1 = """
create database if not exists cat1.cat1_db1
""".strip()

sql_drop_cat1_db1_topic01 = """drop table if exists cat1.cat1_db1.topic01"""

sql_create_cat1_db1_topic01 = """
CREATE TABLE cat1.cat1_db1.topic01 (
  id   INTEGER,
  name STRING,
  ts   timestamp
) WITH (
  'connector' = 'kafka',
  'topic' = 'topic01',
  'properties.bootstrap.servers' = '10.201.0.82:9092,10.201.0.83:9092,10.201.0.84:9092',
  'properties.group.id' = 'testGroup',
  'scan.startup.mode' = 'earliest-offset',
  'format' = 'csv'
)
""".strip()

sql_insert_cat1_db1_topic01 = """
/** 'job_name'='group1:sql_insert_cat1_db1_topic01' */
insert into cat1.cat1_db1.topic01
(id, name, ts)
values
(1, 'xxx', timestamp '2020-01-01 00:00:01'),
(2, 'yyy', timestamp '2020-01-01 00:00:01')
""".strip()

sql_select_cat1_db1_topic01 = """
/** 'job_name'='group2:sql_select_cat1_db1_topic01'*/

select * from cat1.cat1_db1.topic01
""".strip()

sql_set_mode_stream = "set execution.runtime-mode = 'streaming'"

sql_set_mode_batch = "set execution.runtime-mode = 'batch'"

sql_drop_cat1_db1_table01 = """drop table if exists cat1.cat1_db1.table01"""

sql_create_cat1_db1_table01 = """
create table cat1.cat1_db1.table01
(
    id      int,
    name    string,
    ts      timestamp
)with (
    'connector' = 'filesystem',
    'path' = 's3a://warehouse/cat1_db1.db/table01',
    'format' = 'json'
)
""".strip()

sql_insert_cat1_db1_table01 = """
insert into cat1.cat1_db1.table01
(id, name, ts)
values
(1, 'aaa', timestamp '2020-01-01 00:00:01'),
(2, 'bbb', timestamp '2020-01-01 00:00:01')
""".strip()

sql_select_cat1_db1_table01 = """
select * from cat1.cat1_db1.table01
""".strip()
