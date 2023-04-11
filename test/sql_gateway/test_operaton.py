import time
import unittest
from datetime import datetime
from typing import List, Any

from flink_api.flink_rest.flink_rest_client import FlinkRestClient
from flink_api.sql_gateway.helper import SqlGatewayHelper
from flink_api.sql_gateway.operation import SqlGatewayOperation
from flink_api.sql_gateway.session import SqlGatewaySession
from test.res_sql_in_cat1 import (
    sql_create_catalog_cat1,
    sql_create_cat1_db1,
    sql_create_cat1_db1_topic01,
    sql_set_mode_batch,
    sql_create_cat1_db1_table01,
    sql_insert_cat1_db1_table01,
    sql_select_cat1_db1_table01,
    sql_drop_cat1_db1_table01,
    sql_set_mode_stream,
    sql_drop_cat1_db1_topic01,
    sql_insert_cat1_db1_topic01,
    sql_select_cat1_db1_topic01,
)


class TestSqlGatewayOperation(unittest.TestCase):
    def test_execute_statement(self):
        session = SqlGatewaySession._test_session()
        sql = "select 9981 as id"
        operation = SqlGatewayOperation.submit_sql_and_wait_submit_finished(session, sql)
        rows: List[Any] = operation.fetch_next_result()  # may not contain data
        self.assertEquals(operation.last_payload.result_type, "PAYLOAD")

        rows: List[Any] = operation.fetch_all_result()
        self.assertEquals(len(rows), 1)
        self.assertEquals(rows[0][0], 9981)
        self.assertEquals(operation.last_payload.result_type, "EOS")
        self.assertEquals(operation.has_next(), False)

    def test_get_settings(self):
        session = SqlGatewaySession._test_session()
        SqlGatewayOperation.submit_sql_and_wait_submit_finished(session, "set 'execution.runtime-mode' = 'streaming'")
        settings = SqlGatewayHelper.get_settings(session)
        self.assertEquals(settings["execution.runtime-mode"], "streaming")
        pass

    def test_batch_select(self):
        session = SqlGatewaySession._test_session()
        SqlGatewayHelper.sequential_execute_many(
            session,
            [
                sql_create_catalog_cat1,
                sql_create_cat1_db1,
                sql_drop_cat1_db1_table01,
                sql_create_cat1_db1_table01,
                sql_set_mode_batch,
                sql_insert_cat1_db1_table01,
            ],
        )
        time.sleep(3)  # manual wait insert job finished
        last_operation = SqlGatewayOperation.submit_sql_and_wait_submit_finished(session, sql_select_cat1_db1_table01)
        result = last_operation.fetch_all_result()
        column_names = last_operation.column_names
        self.assertEquals(sorted(column_names), sorted(["id", "name", "ts"]))
        self.assertEquals(len(result), 2)
        self.assertEquals(
            sorted(result),
            sorted([[1, "aaa", "2020-01-01T00:00:01"], [2, "bbb", "2020-01-01T00:00:01"]]),
        )

    def test_streaming(self):
        job_name = f"test_{datetime.now().strftime('%Y_%m_%d_%H_%M_%S')}"
        session = SqlGatewaySession._test_session()
        SqlGatewayHelper.sequential_execute_many(
            session,
            [
                sql_create_catalog_cat1,
                sql_create_cat1_db1,
                sql_drop_cat1_db1_topic01,
                sql_create_cat1_db1_topic01,
                sql_insert_cat1_db1_topic01,
                sql_set_mode_stream,
                f"set 'pipeline.name'='{job_name}'",
            ],
        )
        select_op1 = SqlGatewayOperation.submit_sql_and_wait_submit_finished(session, sql_select_cat1_db1_topic01)
        while select_op1.has_next():
            select_op1.fetch_next_result()
            if len(select_op1.data_rows) >= 2:  # wait
                break
            time.sleep(1)
        #
        self.assertEquals(sorted(select_op1.column_names), sorted(["id", "name", "ts"]))
        # self.assertEquals(len(select_op1.data_rows), 2)
        # self.assertEquals(sorted(select_op1.data_rows), sorted([
        #     [1, 'xxx', '2020-01-01T00:00:01'],
        #     [2, 'yyy', '2020-01-01T00:00:01']
        # ]))
        # because kafka topic may have other data
        self.assertTrue(len(select_op1.data_rows) >= 2)
        self.assertTrue([1, "xxx", "2020-01-01T00:00:01"] in select_op1.data_rows)
        self.assertTrue([2, "yyy", "2020-01-01T00:00:01"] in select_op1.data_rows)
        # clean up
        client = FlinkRestClient("127.0.0.1:8081")
        client.cancel_job_by_name_if_possible(job_name)
        # client.wait_job_complete(job_name)
        select_op1.close()
