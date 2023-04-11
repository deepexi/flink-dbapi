import json
from dataclasses import dataclass
import time
from typing import Any, List

import requests

from flink_api.sql_gateway.payload_parser import SqlGatewayPayload
from flink_api.sql_gateway.session import SqlGatewaySession

from flink_api.log import get_logger

logger = get_logger("SqlGatewayOperation")

_SLEEP_INTERVAL_S = 0.1


# @dataclass
# class SqlGatewayResult:
#     column_names: List[str]
#     rows: List[List[Any]]
#     has_next: bool
#     next_result_uri: str
#
#     @staticmethod
#     def from_payload(payload: SqlGatewayPayload):
#         column_names = [x.name for x in payload.columns]
#         rows = [x.fields for x in payload.data]
#         has_next = payload.has_next()
#         next_result_uri = payload.next_result_uri
#         return SqlGatewayResult(column_names, rows, has_next, next_result_uri)


@dataclass
class SqlGatewayOperation:
    def __init__(self, session: SqlGatewaySession, sql):
        self.session = session
        self.sql = sql
        #
        self.closed = False
        self.operation_handle = None
        # result
        self.last_payload = None
        self.column_names = None
        self.data_rows: List[Any] = []

    @staticmethod
    def submit_sql_and_wait_submit_finished(session: SqlGatewaySession, sql_or_list) -> "SqlGatewayOperation":
        """execute statement and wait finish
        here finish means: accepted by flink cluster
        """
        sql_list = []
        if isinstance(sql_or_list, list):
            sql_list = sql_or_list
        else:
            sql_list.append(sql_or_list)

        op = None
        for sql in sql_list:
            op = SqlGatewayOperation(session, sql)
            op.start()
            op._wait_submit_finished()
        return op

    def start(self) -> None:
        logger.info(f"execute {self.sql}")
        response = requests.post(
            url=f"{self.session.session_endpoint()}/statements",
            data=json.dumps({"statement": self.sql}),
            headers={"Content-Type": "application/json"},
        )
        logger.debug(f"SQL gateway response: {json.dumps(response.json())}")
        if response.status_code != 200:
            raise Exception("SQL gateway error: ", response.status_code)
        self.operation_handle = response.json()["operationHandle"]

    def statement_endpoint(self) -> str:
        return f"{self.session.session_endpoint()}/operations/{self.operation_handle}"

    def get_status(self) -> str:
        if self.closed:
            return "CLOSED"

        response = requests.get(
            url=f"{self.statement_endpoint()}/status", headers={"Content-Type": "application/json"}
        )
        if response.status_code != 200:
            raise Exception("SQL gateway error: ", response.status_code)
        return response.json()["status"]

    def _wait_submit_finished(self) -> None:
        status = self.get_status()
        # FINISHED = submitted to flink successfully, not job has been done
        while status != "FINISHED":
            if status == "ERROR":
                raise Exception(f"execute statement status=ERROR, {self.sql} ")
            time.sleep(_SLEEP_INTERVAL_S)
            status = self.get_status()

    def cancel(self) -> str:
        """
        flink 1.16.1 cancel not works as expect
        it only cancel if job hasn't submitted to cluster
        DO not use this method
        """
        response = requests.post(
            url=f"{self.statement_endpoint()}/cancel",
            headers={
                "Content-Type": "application/json",
            },
        )
        if response.status_code != 200:
            raise Exception("SQL gateway error: ", response.status_code)
        return response.json()["status"]

    def close(self) -> str:
        response = requests.delete(
            url=f"{self.statement_endpoint()}/close",
            headers={"Content-Type": "application/json"},
        )
        if response.status_code != 200:
            raise Exception("SQL gateway error: ", response.status_code)
        status = response.json()["status"]
        if status == "CLOSED":
            self.closed = True
        return status

    def has_next(self) -> bool:
        return (self.last_payload is None) or (self.last_payload.has_next())

    def fetch_next_result(self) -> List[Any]:
        if self.last_payload is None:
            self._wait_submit_finished()
            _url = f"{self.statement_endpoint()}/result/0"
        else:
            if not self.last_payload.has_next():
                raise Exception(f"operation {self.operation_handle} no more results")
            _url = f"{self.session.schema_host_port()}{self.last_payload.next_result_uri}"

        response = requests.get(
            url=_url,
            headers={"Content-Type": "application/json"},
        )
        if response.status_code != 200:
            raise Exception("SQL gateway error: ", response.reason)
        payload: SqlGatewayPayload = SqlGatewayPayload.parse(response.json())
        if self.column_names is None:
            self.column_names = [x.name for x in payload.columns]
        rows = [x.fields for x in payload.data]
        self.data_rows.extend(rows)
        self.last_payload = payload
        return rows

    def fetch_all_result(self) -> List[Any]:
        while self.has_next():
            self.fetch_next_result()
        return self.data_rows
