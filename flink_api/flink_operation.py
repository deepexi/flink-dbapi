from dataclasses import dataclass

from flink_api.flink_rest.flink_rest_client import FlinkRestClient
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
        self.session: SqlGatewaySession = SqlGatewaySession(config.sql_gw_api_host_port, config.sql_gw_session_handle)
        self.last_operation = None
        self.last_operation_job_name = None

    def execute_dbt_hint_sql(self, dbt_hint_sql: str):
        """ sql_with_dbt_hint contains /* __dbt_key=value */ """
        if FlinkSqlParseHelper.sql_without_flink_job(dbt_hint_sql):
            self._execute_statement_with_job_name(dbt_hint_sql, job_name=None, reset_job_name=False)
        else:
            # get name from hint
            hints = FlinkSqlParseHelper.extract_dbt_hint(dbt_hint_sql)
            job_name = hints.get("__dbt_job_name")
            self._execute_statement_with_job_name(dbt_hint_sql, job_name, reset_job_name=True)
        return self

    def execute_statement(self, sql: str, job_name: str = None):
        if FlinkSqlParseHelper.sql_without_flink_job(sql):
            self._execute_statement_with_job_name(sql, job_name=None, reset_job_name=False)
        else:
            self._execute_statement_with_job_name(sql, job_name, reset_job_name=True)
        return self

    def _execute_statement_with_job_name(self, sql: str, job_name: str = None, reset_job_name: bool = True):
        """
        sql cases:
           1. not a flink job        => just execute it
           2. job not specify a name => reset job_name + execute sql (flink auto create a name for this job)
           3. job specify a name     => set job_name + execute sql
        """
        if job_name:
            self.last_operation_job_name = job_name
            SqlGatewayOperation.execute_statement_wait_finish(self.session, FLINK_SQL_SET_JOB_NAME.format(job_name))
        elif reset_job_name:
            SqlGatewayOperation.execute_statement_wait_finish(self.session, FLINK_SQL_RESET_JOB_NAME)

        self.last_operation = SqlGatewayOperation.execute_statement_wait_finish(self.session, sql)

    def close(self):
        self.last_operation.close()
        self.last_operation = None
        self.last_operation_job_name = None

    def wait_job_end(self):
        if self.last_operation_job_name:
            self.flink_rest_client.wait_job_end(self.last_operation_job_name)

    def is_job_mode_streaming(self):
        raise Exception("TODO")

    def cancel_job(self):
        if self.last_operation_job_name:
            self.flink_rest_client.cancel_job_by_name_if_possible(self.last_operation_job_name)
        self.close()

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
