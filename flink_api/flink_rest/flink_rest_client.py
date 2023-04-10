import time
from typing import List, Optional

import requests

from flink_api.flink_rest.response_models import FlinkJob
from flink_api.flink_rest.rest_client_utils import RestClientUtils

_SCHEMA_HTTP = "http"
_SLEEP_INTERVAL_S = 0.1

JOB_STATUS = [
    "INITIALIZING",
    "CREATED",
    "RUNNING",
    "FAILING",
    "FAILED",
    "CANCELLING",
    "CANCELED",
    "FINISHED",
    "RESTARTING",
    "SUSPENDED",
    "RECONCILING",
]


class FlinkRestClient:
    __FLINK_VERSION__ = "1.16"

    def __init__(self, host_port):
        self.host_port = host_port

    def job_list(self) -> List[FlinkJob]:
        url = f"{_SCHEMA_HTTP}://{self.host_port}/jobs/overview"
        response = requests.get(url=url)
        if response.status_code != 200:
            raise Exception("flink api error: ", response.status_code)
        jobs = response.json()["jobs"]
        return [FlinkJob(j["jid"], j["name"], j["state"], j["start-time"]) for j in jobs]

    def job_detail(self, job_id):
        url = f"{_SCHEMA_HTTP}://{self.host_port}/jobs/{job_id}"
        response = requests.get(url=url)
        if response.status_code != 200:
            raise Exception("flink api error: ", response.status_code)
        job_detail = response.json()
        return RestClientUtils.parse_job_detail(job_detail)

    def cancel_job_by_id(self, job_id):
        url = f"{_SCHEMA_HTTP}://{self.host_port}/jobs/{job_id}?mode=cancel"
        response = requests.patch(url=url)
        if response.status_code != 202:  # accepted
            raise Exception("flink api error: ", response.status_code)

    def _is_job_streaming(self, job_id):
        """cannot implement"""
        raise Exception("TODO: don't know how to implement it here")

    def list_job_by_name(self, name: str) -> List[FlinkJob]:
        jobs = self.job_list()
        return list(filter(lambda job: (job.name == name), jobs))

    def get_latest_start_job_by_name(self, name: str) -> Optional[FlinkJob]:
        jobs = self.list_job_by_name(name)
        if len(jobs) == 0:
            return None
        latest_start_job = jobs[0]
        for i in range(1, len(jobs)):
            if jobs[i].start_time > latest_start_job.start_time:
                latest_start_job = jobs[i]
        return latest_start_job

    def cancel_job_by_name_if_possible(self, name: str):
        jobs = self.list_job_by_name(name)
        for j in jobs:
            if j.is_not_finished():
                self.cancel_job_by_id(j.jid)

    def wait_job_complete(
        self,
        name: str,
    ):
        while True:
            job = self.get_latest_start_job_by_name(name)
            if job is not None and job.is_finished():
                return
            time.sleep(_SLEEP_INTERVAL_S)

    def wait_job_running(self, name: str):
        while True:
            job = self.get_latest_start_job_by_name(name)
            if (job is not None) and (job.is_running()):
                return
            time.sleep(_SLEEP_INTERVAL_S)
