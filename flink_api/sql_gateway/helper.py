from typing import List

from flink_api.sql_gateway.operation import SqlGatewayOperation
from flink_api.sql_gateway.session import SqlGatewaySession


class SqlGatewayHelper:
    @staticmethod
    def sequential_execute_many(session: SqlGatewaySession, sql_list: List[str]) -> SqlGatewayOperation:
        operation = None
        for sql in sql_list:
            operation = SqlGatewayOperation.submit_sql_and_wait_submit_finished(session, sql)
        return operation

    @staticmethod
    def get_settings(session: SqlGatewaySession):
        operation = SqlGatewayOperation.submit_sql_and_wait_submit_finished(session, "set")
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
