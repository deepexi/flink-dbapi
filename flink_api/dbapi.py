from typing import Any, List, Optional

from flink_api.exceptions import NotSupportedError
from flink_api.flink_operation import FlinkConfig, FlinkOperation

apilevel = "2.0"
threadsafety = 2
paramstyle = "qmark"


class Cursor:
    def __init__(self, flink_op: FlinkOperation):
        self.name = flink_op.session.session_handle
        self.type_code = None
        # self.display_size = None
        # self.internal_size = None
        # self.precision = None
        # self.scale = None
        # self.null_ok = None
        self.flink_op = flink_op
        self.last_operation = None

    @property
    def rowcount(self):
        """read-only attribute specifies the number of rows that the last .execute*() produced"""
        raise NotSupportedError()

    def callproc(self):
        raise NotSupportedError()

    def close(self):
        pass

    def execute(self, sql: str, params=None):
        if params:
            raise NotSupportedError("TODO not support params")
        self.last_operation = self.flink_op.execute_statement(sql)
        return self

    def executemany(self, sql: str, seq_of_params):
        for parameters in seq_of_params[:-1]:
            self.execute(sql, parameters)
            self.fetchall()
        if seq_of_params:
            self.last_operation = self.execute(sql, seq_of_params[-1])
        else:
            self.last_operation = self.execute(sql)
        return self

    def fetchone(self) -> Optional[List[Any]]:
        return self.last_operation.fetch_next_result()

    def fetchmany(self):
        raise NotSupportedError()

    def fetchall(self):
        self.last_operation.fetch_all_result()
        return self.last_operation.data_rows

    def nextset(self):
        raise NotSupportedError()

    def arraysize(self):
        raise NotSupportedError()

    def setinputsizes(self, sizes):
        raise NotSupportedError()

    def setoutputsize(self, size):
        raise NotSupportedError()


class Connection(object):
    def __init__(
        self,
        flink_rest_api_host_port: str,
        flink_sql_gw_host_port: str,
        flink_sql_gw_session_handle: str,
    ):
        config = FlinkConfig(flink_rest_api_host_port, flink_sql_gw_host_port, flink_sql_gw_session_handle)
        self.flink_operation = FlinkOperation(config)

    def close(self):
        """Close the connection now"""
        pass

    def commit(self):
        """Commit any pending transaction to the database."""
        pass

    def rollback(self):
        """optional since not all databases provide transaction support."""
        pass
        raise NotSupportedError("flink dbapi not support rollback")

    def cursor(self) -> Cursor:
        return Cursor(self.flink_operation)
