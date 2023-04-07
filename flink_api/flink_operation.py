from dataclasses import dataclass
from typing import List, Any

from flink_api.flink_rest.flink_rest_client import FlinkRestClient
from flink_api.flink_rest.response_models import FlinkJobDetail, FlinkJob, Relation
from flink_api.flink_sql_parser import FlinkSqlParseHelper
from flink_api.sql_gateway.operation import SqlGatewayOperation
from flink_api.sql_gateway.session import SqlGatewaySession

FLINK_SQL_SET_JOB_NAME = "set 'pipeline.name'='{}'"
FLINK_SQL_RESET_JOB_NAME = "reset 'pipeline.name'"


@dataclass
class FlinkConfig:
    #
    flink_api_host_port: str
    #
    sql_gw_api_host_port: str
    sql_gw_session_handle: str

    @staticmethod
    def from_session(flink_api_host_port, session: SqlGatewaySession):
        return FlinkConfig(flink_api_host_port, session.host_port, session.session_handle)


class FlinkOperation:
    def __init__(self, config: FlinkConfig):
        self.flink_rest_client = FlinkRestClient(config.flink_api_host_port)
        self.session: SqlGatewaySession = SqlGatewaySession(
            config.sql_gw_api_host_port, config.sql_gw_session_handle
        )
        self.last_operation = None
        self.last_operation_job_name = None

    def execute_dbt_hint_sql(self, dbt_hint_sql: str):
        """sql_with_dbt_hint contains /* __dbt_key=value */"""
        if FlinkSqlParseHelper.sql_without_flink_job(dbt_hint_sql):
            self._execute_statement_with_job_name(dbt_hint_sql, job_name=None)
        else:
            # get name from hint
            hints = FlinkSqlParseHelper.extract_dbt_hint(dbt_hint_sql)
            job_name = hints.get("__dbt_job_name")
            self._execute_statement_with_job_name(dbt_hint_sql, job_name)
        return self

    def execute_statement(self, sql: str, job_name: str = None):
        if FlinkSqlParseHelper.sql_without_flink_job(sql):
            self._execute_statement_with_job_name(sql, job_name=None)
        else:
            self._execute_statement_with_job_name(sql, job_name)
        return self

    def _execute_statement_with_job_name(self, sql: str, job_name: str = None):
        if job_name:
            self.last_operation_job_name = job_name
            sql_list = [FLINK_SQL_SET_JOB_NAME.format(job_name), sql]
            self.last_operation = SqlGatewayOperation.execute_many(self.session, sql_list)
            SqlGatewayOperation.execute_many(self.session, [FLINK_SQL_RESET_JOB_NAME])
        else:
            self.last_operation = SqlGatewayOperation.execute_many(self.session, [sql])

    def _inner_query_not_record(self, sql: str) -> List[Any]:
        inner_op = SqlGatewayOperation.execute_statement_wait_finish(self.session, sql)
        rows = inner_op.fetch_all_result()
        return rows

    def is_job_mode_streaming(self):
        """
        判断一个 job是否是 streaming job,
        - 1. 未completed, 否则 batch
        - 2. plan.node.type=streaming, 否则 batch (type=streaming 是 streaming mode的必要条件)
        """
        if self.last_operation_job_name is None:
            return False
        jobs: List[FlinkJob] = self.flink_rest_client.get_job_by_name(self.last_operation_job_name)
        if len(jobs) == 0:
            return False

        assert len(jobs) <= 1, f"many job have same name {self.last_operation_job_name}"

        if not jobs[0].is_not_finished():
            return False

        job_detail: FlinkJobDetail = self.flink_rest_client.job_detail(jobs[0].jid)

        # this job has a node hint: streaming=true
        if job_detail.node_has_hint_streaming():
            return True

        scan_tables: List[Relation] = job_detail.list_running_table_source_scan_node()
        # check if one of these scan_table is streaming table
        # by `show create table xxx.xxx.xxx`
        for relation in scan_tables:
            _sql_show_create_table = "show create table {}".format(relation)
            rows = self._inner_query_not_record(_sql_show_create_table)
            if len(rows) == 0:
                continue
            print(rows[0])
        raise "TODO"

    def close(self):
        self.last_operation.close()
        self.last_operation = None
        self.last_operation_job_name = None

    def wait_job_complete(self):
        if self.last_operation_job_name:
            self.flink_rest_client.wait_job_complete(self.last_operation_job_name)

    def cancel_job(self):
        if self.last_operation_job_name:
            self.flink_rest_client.cancel_job_by_name_if_possible(self.last_operation_job_name)

    def has_next(self):
        self.last_operation.has_next()

    def fetch_next_result(self):
        return self.last_operation.fetch_next_result()

    def fetch_all_result(self):
        return self.last_operation.fetch_all_result()

    @property
    def column_names(self):
        return self.last_operation.column_names

    @property
    def data_rows(self):
        return self.last_operation.data_rows
