import unittest

from flink_api.flink_ddl_utils import FlinkDdlUtils

ddl_stream_1 = """
CREATE TABLE `cat1`.`cat1_db1`.`topic01` (
  `id` INT,
  `name` VARCHAR(2147483647),
  `ts` TIMESTAMP(6)
) WITH (
  'format' = 'csv',
  'scan.startup.mode' = 'earliest-offset',
  'properties.bootstrap.servers' = '10.201.0.82:9092,10.201.0.83:9092,10.201.0.84:9092',
  'connector' = 'kafka',
  'topic' = 'topic01',
  'properties.group.id' = 'testGroup'
)
"""

ddl_bound_1 = """
CREATE TABLE `cat1`.`cat1_db1`.`topic02` (
  `id` INT,
  `name` VARCHAR(2147483647),
  `ts` TIMESTAMP(6)
) WITH (
  'format' = 'csv',
  'scan.startup.mode' = 'earliest-offset',
  'bounded' = 'true',
  'properties.bootstrap.servers' = '10.201.0.82:9092,10.201.0.83:9092,10.201.0.84:9092',
  'connector' = 'kafka',
  'topic' = 'topic01',
  'properties.group.id' = 'testGroup'
)
"""

ddl_bound_2 = """
CREATE TABLE `cat2`.`cat2_db2`.`t4` (
  `id` INT,
  `age` INT
)
"""

ddl_bound_3 = """
CREATE TABLE `cat1`.`cat1_db1`.`table01` (
  `id` INT,
  `name` VARCHAR(2147483647),
  `ts` TIMESTAMP(6)
) WITH (
  'path' = 's3a://warehouse/cat1_db1.db/table01',
  'connector' = 'filesystem',
  'format' = 'json'
)
"""


class TestFlinkUtils(unittest.TestCase):
    def test_is_table_streaming_by_ddl_1(self):
        self.assertTrue(FlinkDdlUtils.is_table_streaming_by_ddl(ddl_stream_1))

    def test_is_table_streaming_by_ddl_2(self):
        self.assertFalse(FlinkDdlUtils.is_table_streaming_by_ddl(ddl_bound_1))

    def test_is_table_streaming_by_ddl_3(self):
        self.assertFalse(FlinkDdlUtils.is_table_streaming_by_ddl(ddl_bound_2))

    def test_is_table_streaming_by_ddl_4(self):
        self.assertFalse(FlinkDdlUtils.is_table_streaming_by_ddl(ddl_bound_3))
