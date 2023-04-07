from typing import List

from flink_api.sql_gateway.operation import SqlGatewayOperation
from flink_api.sql_gateway.session import SqlGatewaySession


class SqlGatewayHelper:

    @staticmethod
    def sequential_execute_many(session: SqlGatewaySession, sql_list: List[str]) -> SqlGatewayOperation:
        operation = None
        for sql in sql_list:
            operation = SqlGatewayOperation.execute_statement_wait_finish(session, sql)
        return operation

    # @staticmethod
    # def execute_sql_with_pipeline_name(session: SqlGatewaySession, sql, pipe_name):
    #     sql_list = [
    #         f"set 'pipeline.name'='{pipe_name}'",
    #         sql,
    #         f"reset 'pipeline.name'"
    #     ]
    #     return SqlGatewayHelper.sequential_execute_many(session=session, sql_list=sql_list)

    @staticmethod
    def get_settings(session: SqlGatewaySession):
        operation = SqlGatewayOperation.execute_statement_wait_finish(session, "set")
        operation.fetch_all_result()
        settings = {}
        for kv in operation.data_rows:
            key = kv[0].lower()
            value = kv[1]
            settings[key] = value
        return settings

    @staticmethod
    def is_execution_runtime_mode_streaming(session: SqlGatewaySession):
        settings = SqlGatewayHelper.get_settings(session)
        key = "execution.runtime-mode"
        value = "streaming"
        return settings[key] == value
