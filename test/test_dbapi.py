import unittest

from flink_api.dbapi import Connection, Cursor
from flink_api.flink_operation import FlinkConfig, FlinkOperation
from flink_api.sql_gateway.session import SqlGatewaySession
from test.res_sql_in_cat1 import (
    sql_create_catalog_cat1,
    sql_create_cat1_db1,
    sql_drop_cat1_db1_topic01,
    sql_create_cat1_db1_topic01,
)
from test.res_sql_in_cat2 import (
    sql_create_catalog_cat2,
    sql_create_db_cat2_db2,
    sql_drop_table_cat2_db2_t2,
    sql_create_table_cat2_db2_t2,
    sql_insert_table_cat2_db2_t2_with_hint,
    sql_select_table_cat2_db2_t2,
)


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
    def test_execute_simple(self):
        conf: FlinkConfig = _test_config()
        conn = Connection(conf.flink_api_host_port, conf.sql_gw_api_host_port, conf.sql_gw_session_handle)
        cursor: Cursor = conn.cursor()
        cursor.execute("select 9981 as f1, 9982 as f1")
        result_set = cursor.fetchall()
        print(result_set)
        self.assertIsNotNone(result_set)
        self.assertEquals(len(result_set), 1)
        self.assertEquals(result_set[0], [9981, 9982])
        cursor.close()
        conn.close()

    def test_execute_with_hint_job_name(self):
        conf: FlinkConfig = _test_config()
        conn = Connection(conf.flink_api_host_port, conf.sql_gw_api_host_port, conf.sql_gw_session_handle)
        cursor: Cursor = conn.cursor()
        #
        cursor.execute(sql_create_catalog_cat2)
        cursor.execute(sql_create_db_cat2_db2)
        cursor.execute(sql_drop_table_cat2_db2_t2)
        cursor.execute(sql_create_table_cat2_db2_t2)
        cursor.execute(sql_insert_table_cat2_db2_t2_with_hint)  # [1, 1], [2, 2]
        #
        cursor.execute(sql_select_table_cat2_db2_t2)
        rows = cursor.fetchall()
        self.assertEquals(len(rows), 2)
        self.assertEquals(sorted(rows), sorted([[1, 1], [2, 2]]))
        # print(rows)
