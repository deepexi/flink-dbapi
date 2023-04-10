import time
from dataclasses import dataclass
from typing import List, Any

from flink_api.flink_ddl_utils import FlinkDdlUtils
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
        self._flink_rest_client = FlinkRestClient(config.flink_api_host_port)
        self.session: SqlGatewaySession = SqlGatewaySession(
            config.sql_gw_api_host_port, config.sql_gw_session_handle
        )
        self._last_operation = None
        self._last_flink_job_name = None

    def execute_dbt_hint_sql(self, dbt_hint_sql: str):
        """sql_with_dbt_hint contains /* __dbt_key=value */"""
        if FlinkSqlParseHelper.is_sql_require_flink_job(dbt_hint_sql):
            self._execute_statement_with_job_name(dbt_hint_sql, job_name=None)
        else:
            # get name from hint
            hints = FlinkSqlParseHelper.extract_dbt_hint(dbt_hint_sql)
            job_name = hints.get("__dbt_job_name")
            self._execute_statement_with_job_name(dbt_hint_sql, job_name)
        return self

    def execute_statement(self, sql: str, job_name: str = None):
        if FlinkSqlParseHelper.is_sql_require_flink_job(sql):
            self._execute_statement_with_job_name(sql, job_name)
        else:
            self._execute_statement_with_job_name(sql, job_name=None)
        return self

    def _execute_statement_with_job_name(self, sql: str, job_name: str = None) -> None:
        """
        execute statement and
        if not flink job => wait till submit Finished
        if flink job:
           if batch     => wait job finished
           if streaming => wait job running
        """
        if job_name:
            self._last_flink_job_name = job_name
            sql_list = [FLINK_SQL_SET_JOB_NAME.format(job_name), sql]
            self._last_operation = SqlGatewayOperation.submit_sql_and_wait_submit_finished(
                self.session, sql_list
            )
            SqlGatewayOperation.submit_sql_and_wait_submit_finished(
                self.session, FLINK_SQL_RESET_JOB_NAME
            )
            # wait job finished(batch) / running(stream)
            is_streaming = self.is_flink_job_mode_streaming()
            if is_streaming:
                self._wait_job_running()
            else:
                self._wait_job_complete()
        else:
            self._last_operation = SqlGatewayOperation.submit_sql_and_wait_submit_finished(
                self.session, sql
            )

    def _inner_query_not_record(self, sql: str) -> List[Any]:
        inner_op = SqlGatewayOperation.submit_sql_and_wait_submit_finished(self.session, sql)
        rows = inner_op.fetch_all_result()
        return rows

    def is_flink_job_mode_streaming(self):
        """check if a RUNNING flink job is streaming job or batch job"""
        if self._last_flink_job_name is None:
            raise Exception("last_operation_job_name is None")
            # return False
        job: FlinkJob = self._flink_rest_client.get_latest_start_job_by_name(
            self._last_flink_job_name
        )
        if job is None:
            # return False
            raise Exception(
                f"No such job name={self._last_flink_job_name}, maybe finished too long"
            )

        if job.is_finished():
            return False

        job_detail: FlinkJobDetail = self._flink_rest_client.job_detail(job.jid)

        if job_detail.is_plan_type_batch():
            return False

        # job has a flink hint: streaming=true => streaming job
        if job_detail.plan_node_has_hint_streaming():
            return True

        # check if one of these scan_table is streaming table, by `show create table xxx.xxx.xxx`
        scan_tables: List[Relation] = job_detail.list_running_table_source_scan_node()
        if len(scan_tables) == 0:
            return True
        for relation in scan_tables:
            _sql_show_create_table = "show create table {}".format(relation)
            rows = self._inner_query_not_record(_sql_show_create_table)
            if len(rows) == 0:
                continue
            print(rows[0])
            _ddl = rows[0][0]
            is_stream = FlinkDdlUtils.is_table_streaming_by_ddl(_ddl)
            if is_stream:
                return True

    def close(self):
        self._last_operation.close()
        self._last_operation = None
        self._last_flink_job_name = None

    def _wait_job_complete(self):
        if self._last_flink_job_name:
            self._flink_rest_client.wait_job_complete(self._last_flink_job_name)

    def _wait_job_running(self):
        if self._last_flink_job_name:
            self._flink_rest_client.wait_job_running(self._last_flink_job_name)

    def cancel_job(self):
        if self._last_flink_job_name:
            self._flink_rest_client.cancel_job_by_name_if_possible(self._last_flink_job_name)

    def has_next(self):
        self._last_operation.has_next()

    def fetch_next_result(self):
        return self._last_operation.fetch_next_result()

    def fetch_all_result(self):
        return self._last_operation.fetch_all_result()

    @property
    def column_names(self):
        return self._last_operation.column_names

    @property
    def data_rows(self):
        return self._last_operation.data_rows
