import unittest

from flink_api.flink_utils import FlinkUtils

ddl_should_stream = """
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
);
"""

ddl_not_stream = """
CREATE TABLE `cat2`.`cat2_db2`.`t4` (
  `id` INT,
  `age` INT
)
"""


class TestFlinkUtils(unittest.TestCase):
    def test_is_table_streaming_by_ddl(self):
        self.assertTrue(FlinkUtils.is_table_streaming_by_ddl(ddl_should_stream))
        self.assertFalse(FlinkUtils.is_table_streaming_by_ddl(ddl_not_stream))
