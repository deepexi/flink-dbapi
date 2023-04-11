import time
import unittest
import uuid

from flink_api.flink_operation import FlinkConfig, FlinkOperation, FlinkJobUtils
from flink_api.sql_gateway.session import SqlGatewaySession
from test.res_sql_in_cat1 import (
    sql_create_catalog_cat1,
    sql_create_cat1_db1,
    sql_create_cat1_db1_topic01,
    sql_insert_cat1_db1_topic01,
    sql_select_cat1_db1_topic01,
    sql_drop_cat1_db1_topic01,
)
from test.res_sql_in_cat2 import (
    sql_create_catalog_cat2,
    sql_create_db_cat2_db2,
    sql_drop_table_cat2_db2_t2,
    sql_drop_table_cat2_db2_t2_mirror,
    sql_ctas_cat2_db2_t2_mirror,
    sql_create_table_cat2_db2_t2,
    sql_table_create_datagen,
    sql_table_create_blackhole,
    sql_datagen_2_blackhole,
)


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
        FlinkOperation(config).execute_statement(sql_insert_cat1_db1_topic01)

        op_select = FlinkOperation(config).execute_statement(sql_select_cat1_db1_topic01)
        while True:
            op_select.fetch_next_result()
            if len(op_select.data_rows) >= 2:  # wait
                break
            time.sleep(0.5)

        self.assertEquals(sorted(op_select.column_names), sorted(["id", "name", "ts"]))
        self.assertTrue(len(op_select.data_rows) >= 2)
        self.assertTrue([1, "xxx", "2020-01-01T00:00:01"] in op_select.data_rows)
        self.assertTrue([2, "yyy", "2020-01-01T00:00:01"] in op_select.data_rows)
        # clean up
        op_select.cancel_job()
        op_select.close()

    def test_is_flink_job_mode_streaming__kafka_2_iceberg(self):
        session: SqlGatewaySession = SqlGatewaySession._test_session("127.0.0.1:8083")
        config = FlinkConfig.from_session("127.0.0.1:8081", session)
        FlinkOperation(config).execute_statement(sql_create_catalog_cat1)
        FlinkOperation(config).execute_statement(sql_create_cat1_db1)
        FlinkOperation(config).execute_statement(sql_drop_cat1_db1_topic01)
        FlinkOperation(config).execute_statement(sql_create_cat1_db1_topic01)
        FlinkOperation(config).execute_statement(sql_insert_cat1_db1_topic01)
        op_select = FlinkOperation(config).execute_statement(sql_select_cat1_db1_topic01)
        self.assertTrue(op_select.last_flink_job_stream)
        op_select.cancel_job()
        op_select.close()

    def test_is_flink_job_mode_streaming__iceberg_ctas(self):
        session: SqlGatewaySession = SqlGatewaySession._test_session("127.0.0.1:8083")
        config = FlinkConfig.from_session("127.0.0.1:8081", session)

        FlinkOperation(config).execute_statement(sql_create_catalog_cat2)
        FlinkOperation(config).execute_statement(sql_create_db_cat2_db2)
        FlinkOperation(config).execute_statement(sql_drop_table_cat2_db2_t2)
        FlinkOperation(config).execute_statement(sql_create_table_cat2_db2_t2)
        FlinkOperation(config).execute_statement(sql_drop_table_cat2_db2_t2_mirror)

        op_ctas = FlinkOperation(config).execute_statement(sql_ctas_cat2_db2_t2_mirror)
        self.assertFalse(op_ctas.last_flink_job_stream)
        op_ctas.cancel_job()
        op_ctas.close()

    def test_idle_ratio_(self):
        session: SqlGatewaySession = SqlGatewaySession._test_session("127.0.0.1:8083")
        config = FlinkConfig.from_session("127.0.0.1:8081", session)
        #
        # FlinkOperation(config).execute_statement("use default_catalog.default_database")
        FlinkOperation(config).execute_statement(sql_table_create_datagen)
        FlinkOperation(config).execute_statement(sql_table_create_blackhole)
        op_insert = FlinkOperation(config).execute_statement(sql_datagen_2_blackhole)
        client = op_insert.flink_rest_client
        job_id = op_insert.last_flink_job_id

        FlinkJobUtils.wait_job_source_idle(client, job_id)

        op_insert.cancel_job()
