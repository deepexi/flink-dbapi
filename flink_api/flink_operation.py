from dataclasses import dataclass
from typing import List, Any
import time
from flink_api.log import get_logger

from flink_api.flink_ddl_utils import FlinkDdlUtils
from flink_api.flink_rest.flink_rest_client import FlinkRestClient
from flink_api.flink_rest.response_models import FlinkJobDetail, FlinkJob, Relation
from flink_api.flink_sql_parser import FlinkSqlParseHelper
from flink_api.sql_gateway.operation import SqlGatewayOperation
from flink_api.sql_gateway.session import SqlGatewaySession

FLINK_SQL_SET_JOB_NAME = "set 'pipeline.name'='{}'"
FLINK_SQL_RESET_JOB_NAME = "reset 'pipeline.name'"

__FLINK_VERSION__ = "1.16"

_SLEEP_INTERVAL_S = 0.1
logger = get_logger("FlinkOperation")

SUPPORT_DBT_HINT_KEY = ["job_name", "test_query"]


def assure_hint_legal(hint_kv: dict) -> None:
    for key in hint_kv.keys():
        if key not in SUPPORT_DBT_HINT_KEY:
            logger.warn(f"hint {key} not support yet")


@dataclass
class FlinkJobName:
    group: str
    ts: str


@dataclass
class FlinkConfig:
    # flink rest api host port, like 127.0.0.1:8081
    flink_api_host_port: str
    # flink sql-gateway api host port, like 127.0.0.1:8083
    sql_gw_api_host_port: str
    # session_handle from flink sql-gateway
    sql_gw_session_handle: str

    @staticmethod
    def from_session(flink_api_host_port, session: SqlGatewaySession):
        return FlinkConfig(flink_api_host_port, session.host_port, session.session_handle)


class FlinkOperation:
    def __init__(self, config: FlinkConfig):
        self.flink_rest_client = FlinkRestClient(config.flink_api_host_port)
        self.session: SqlGatewaySession = SqlGatewaySession(config.sql_gw_api_host_port, config.sql_gw_session_handle)
        self.last_operation = None
        # self.last_flink_job_name = None
        self.last_flink_job_id = None
        self.last_flink_job_stream: bool = False
        self.sql_hints = None

    def execute_statement(self, sql: str):
        """sql_with_dbt_hint contains /* __dbt_key=value */"""
        hints = FlinkSqlParseHelper.extract_dbt_hint(sql)
        assure_hint_legal(hints)
        self.sql_hints = hints

        # some sql does not run flink job, for these job, job_name is not used
        if FlinkSqlParseHelper.is_sql_require_flink_job(sql):
            # self.last_flink_job_name = hints.get("job_name")
            self._execute_statement_with_job_name(sql, hints.get("job_name"))
        else:
            self._execute_statement_with_job_name(sql, job_name=None)
        return self

    def _cancel_job_of_same_group(self, job_name: str):
        group_ts = job_name.split(":")
        assert len(group_ts) == 2, "job_name not legal, must be format = group_name:ts"
        group_name = group_ts[0]
        self.flink_rest_client.cancel_job_by_name_prefix(group_name)

    def _is_hint_test(self):
        return self.sql_hints and "test_query" in self.sql_hints.keys()

    def _execute_statement_with_job_name(self, sql: str, job_name: str = None) -> None:
        """
        execute statement and
        if it does not require flink job => wait till submit Finished
        if it requires flink job:
           if batch     => wait job finished
           if streaming => wait job running
        """
        if job_name:
            # these sql will start a flink job, typically dml
            # we need do further work on flink job to make sure sql works as expect
            # 1
            # if it has same group jobs, cancel them all,
            # for eg, run same sql twice, but first job still running
            self._cancel_job_of_same_group(job_name)

            # run job with job_name
            sql_list = [FLINK_SQL_SET_JOB_NAME.format(job_name), sql]
            self.last_operation = SqlGatewayOperation.submit_sql_and_wait_submit_finished(self.session, sql_list)
            SqlGatewayOperation.submit_sql_and_wait_submit_finished(self.session, FLINK_SQL_RESET_JOB_NAME)

            _job = self.flink_rest_client.get_latest_start_job_by_name(job_name)
            self.last_flink_job_id = _job.jid
            self.last_flink_job_stream = self._is_flink_job_mode_streaming()
            if self._is_hint_test():
                if self.last_flink_job_stream:
                    print(f"_execute_statement_with_job_name job_id={_job.jid}, test + stream")
                    # for test query + stream job, we should return when streaming reach current
                    FlinkJobUtils.wait_job_source_idle(self.flink_rest_client, self.last_flink_job_id)
                else:
                    print(f"_execute_statement_with_job_name job_id={_job.jid}, test + batch")
                    self._wait_job_complete()
            else:
                # wait job finished(batch) / running(stream)
                if self.last_flink_job_stream:
                    print(f"_execute_statement_with_job_name job_id={_job.jid}, stream")
                    self._wait_job_running()
                else:
                    print(f"_execute_statement_with_job_name job_id={_job.jid}, batch")
                    self._wait_job_complete()
        else:
            # sql here will not start a flink job, its runs fast, typically ddl statement
            # therefore we needn't do further work
            self.last_operation = SqlGatewayOperation.submit_sql_and_wait_submit_finished(self.session, sql)

    def _inner_query_not_record(self, sql: str) -> List[Any]:
        inner_op = SqlGatewayOperation.submit_sql_and_wait_submit_finished(self.session, sql)
        rows = inner_op.fetch_all_result()
        return rows

    def _is_flink_job_mode_streaming(self):
        if self.last_flink_job_id is None:
            raise Exception("last_flink_job_id is None")
            # return False

        job: FlinkJobDetail = self.flink_rest_client.job_detail(self.last_flink_job_id)
        if job is None:
            # return False
            raise Exception(f"No such job job_id={self.last_flink_job_id}, maybe finished too long")

        if job.is_finished():
            return False

        # job_detail: FlinkJobDetail = self.flink_rest_client.job_detail(job.jid)

        if job.is_plan_type_batch():
            return False

        # job has a flink hint: streaming=true => streaming job
        if job.plan_node_has_hint_streaming():
            return True

        # check if one of these scan_table is streaming table, by `show create table xxx.xxx.xxx`
        scan_tables: List[Relation] = job.list_running_table_source_scan_node()
        if len(scan_tables) == 0:
            return False  # no streaming source
        for relation in scan_tables:
            _sql_show_create_table = "show create table {}".format(relation)
            rows = self._inner_query_not_record(_sql_show_create_table)
            if len(rows) == 0:
                continue
            _ddl = rows[0][0]
            return FlinkDdlUtils.is_table_streaming_by_ddl(_ddl)

    def close(self):
        self.last_operation.close()
        self.last_operation = None
        # self.last_flink_job_name = None
        self.last_flink_job_id = None

    def _wait_job_complete(self):
        if not self.last_flink_job_id:
            return
        while True:
            job = self.flink_rest_client.job_detail(self.last_flink_job_id)
            if job is not None and job.is_finished():
                return
            time.sleep(_SLEEP_INTERVAL_S)

    def _wait_job_running(self):
        if not self.last_flink_job_id:
            return
        while True:
            job = self.flink_rest_client.job_detail(self.last_flink_job_id)
            if (job is not None) and (job.is_running() or job.is_finished()):
                return
            time.sleep(_SLEEP_INTERVAL_S)

    def cancel_job(self):
        if not self.last_flink_job_id:
            return
        job_detail = self.flink_rest_client.job_detail(self.last_flink_job_id)
        if job_detail.is_not_finished():
            self.flink_rest_client.cancel_job_by_id(self.last_flink_job_id)

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


class FlinkJobUtils:
    _CHECK_INTERVAL_ = 0.5
    _IDLE_RATIO_ = 0.5

    @staticmethod
    def wait_job_source_idle(client: FlinkRestClient, job_id: str):
        """
        this method may fail return if source streaming always busy
        TODO: how to handle if source always busy
        """
        detail_pre: FlinkJobDetail = client.job_detail(job_id)
        time.sleep(FlinkJobUtils._CHECK_INTERVAL_)

        while True:
            detail_curr: FlinkJobDetail = client.job_detail(job_id)
            if detail_curr.is_finished():
                return
            if FlinkJobUtils._is_idle_(detail_pre, detail_curr):
                return
            detail_pre = detail_curr

    @staticmethod
    def _idle_ratio_(detail_pre: FlinkJobDetail, detail_curr: FlinkJobDetail) -> float:
        ratio = 100000  # big enough
        for node_curr in detail_curr.plan_nodes:
            if node_curr.is_node_finished:
                continue
            node_id = node_curr.id
            node_pre = list(filter(lambda node: node.id == node_id, detail_pre.plan_nodes))[0]
            d_busy_time = node_curr.metrics_accumulated_busy_time - node_pre.metrics_accumulated_busy_time
            d_idle_time = node_curr.metrics_accumulated_idle_time - node_pre.metrics_accumulated_idle_time
            d_backpressured_time = (
                node_curr.metrics_accumulated_backpressured_time - node_pre.metrics_accumulated_backpressured_time
            )
            logger.info(
                f"""interval={FlinkJobUtils._CHECK_INTERVAL_},
                         d_busy_time={d_busy_time} ms,
                         d_idle_time={d_idle_time} ms,
                d_backpressured_time={d_backpressured_time} ms"""
            )

            # simply policy
            if (d_busy_time + d_backpressured_time) == 0:
                continue
            _ration = d_idle_time / (d_busy_time + d_backpressured_time)
            ratio = ratio if ratio < _ration else _ration
        return ratio

    @staticmethod
    def _is_idle_(detail_pre: FlinkJobDetail, detail_curr: FlinkJobDetail):
        return FlinkJobUtils._idle_ratio_(detail_pre, detail_curr) > FlinkJobUtils._IDLE_RATIO_
