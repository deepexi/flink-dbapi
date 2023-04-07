import time
import unittest
import uuid

from flink_api.flink_operation import FlinkConfig, FlinkOperation
from flink_api.sql_gateway.session import SqlGatewaySession
from test.sql_gateway.test_sql import sql_create_catalog_cat1, sql_create_cat1_db1, sql_create_cat1_db1_topic01, \
    sql_insert_cat1_db1_topic01, sql_select_cat1_db1_topic01, sql_drop_cat1_db1_topic01


def uuid_name(pre: str = None):
    return f"{pre}_{str(uuid.uuid4())}"


class TestFlinkOperation(unittest.TestCase):

    def test_execute_statement(self):
        session: SqlGatewaySession = SqlGatewaySession._test_session("127.0.0.1:8083")
        config = FlinkConfig.from_session("127.0.0.1:8081", session)
        #
        FlinkOperation(config).execute_statement(sql_create_catalog_cat1)
        FlinkOperation(config).execute_statement(sql_create_cat1_db1)
        FlinkOperation(config).execute_statement(sql_drop_cat1_db1_topic01)
        FlinkOperation(config).execute_statement(sql_create_cat1_db1_topic01)
        op_insert = FlinkOperation(config).execute_statement(sql_insert_cat1_db1_topic01, uuid_name("insert"))
        op_insert.wait_job_end()

        op_select = FlinkOperation(config).execute_statement(sql_select_cat1_db1_topic01, uuid_name("select"))
        while True:
            op_select.fetch_next_result()
            if len(op_select.data_rows) >= 2:  # wait
                break
            time.sleep(0.5)
        op_select.cancel_job()
        op_select.wait_job_end()

        self.assertEquals(sorted(op_select.column_names), sorted(["id", "name", "ts"]))
        self.assertEquals(len(op_select.data_rows), 2)
        self.assertEquals(sorted(op_select.data_rows), sorted([
            [1, 'xxx', '2020-01-01T00:00:01'],
            [2, 'yyy', '2020-01-01T00:00:01']
        ]))
