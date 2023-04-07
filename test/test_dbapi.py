import unittest

from flink_api.dbapi import Connection
from flink_api.flink_operation import FlinkConfig, FlinkOperation
from flink_api.sql_gateway.session import SqlGatewaySession
from test.sql_gateway.test_sql import sql_create_catalog_cat1, sql_create_cat1_db1, sql_drop_cat1_db1_topic01, \
    sql_create_cat1_db1_topic01


def _test_config():
    session = SqlGatewaySession._test_session()
    config = FlinkConfig.from_session("127.0.0.1:8081", session)
    return config


def _setup():
    config = _test_config()
    FlinkOperation(config).execute_statement(sql_create_catalog_cat1)
    FlinkOperation(config).execute_statement(sql_create_cat1_db1)
    FlinkOperation(config).execute_statement(sql_drop_cat1_db1_topic01)
    FlinkOperation(config).execute_statement(sql_create_cat1_db1_topic01)
    return config


class TestDbapi(unittest.TestCase):

    def test_execute(self):
        conf: FlinkConfig = _test_config()

        conn = Connection(conf.flink_api_host_port, conf.sql_gw_api_host_port, conf.sql_gw_session_handle)
        cursor = conn.cursor()
        cursor.execute("select 9981 as f1, 9982 as f1")
        result_set = cursor.fetchall()
        print(result_set)
        self.assertIsNotNone(result_set)
        self.assertEquals(len(result_set), 1)
        self.assertEquals(result_set[0], [9981, 9982])
        cursor.close()
        conn.close()
