import time
from dataclasses import dataclass
from typing import List

import requests

_SCHEMA_HTTP = "http"
_SLEEP_INTERVAL_S = 0.1
# @dataclass
# class FlinkRestClientConfig:
#     host: str
#     port: int
#
#     @staticmethod
#     def _test_config():
#         return FlinkRestClientConfig("localhost", 8081)

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
    "RECONCILING"
]


@dataclass
class FlinkJob:
    jid: str
    name: str
    state: str

    def _is_running(self):
        return self.state == "RUNNING"

    def can_cancel(self):
        """TODO not very sure"""
        return self.state in ["RUNNING", "INITIALIZING", "CREATED", "SUSPENDED", "RESTARTING", "RECONCILING"]

    def not_ended(self):
        return self.state in ["CANCELED", "FINISHED"]


class FlinkRestClient:
    def __init__(self, host_port):
        self.host_port = host_port

    def _endpoint(self):
        return

    def job_list(self) -> List[FlinkJob]:
        url = f"{_SCHEMA_HTTP}://{self.host_port}/jobs/overview"
        response = requests.get(url=url)
        if response.status_code != 200:
            raise Exception("flink api error: ", response.status_code)
        jobs = response.json()["jobs"]
        return [FlinkJob(j["jid"], j["name"], j["state"]) for j in jobs]

    def get_job_by_name(self, name: str) -> List[FlinkJob]:
        jobs = self.job_list()
        return list(filter(lambda job: (job.name == name), jobs))

    # def get_job_by_name_ensure_one(self, name: str):
    #     target = self.get_job_by_name(name)
    #     if len(target) != 1:
    #         raise Exception(f"job name {name} has multiple job")
    #     return target[0]

    def _cancel_job_by_id(self, job_id):
        """
        doc say     = PATCH /jobs/:jobId?mode=cancel
        while on ui = GET   /jobs/jobsId/yarn-cancel
        """
        url = f"{_SCHEMA_HTTP}://{self.host_port}/jobs/{job_id}?mode=cancel"
        response = requests.patch(url=url)
        if response.status_code != 202:  # accepted
            raise Exception("flink api error: ", response.status_code)

    def cancel_job_by_name_if_possible(self, name: str):
        jobs = self.get_job_by_name(name)
        for j in jobs:
            if not j.not_ended():
                self._cancel_job_by_id(j.jid)

    def wait_job_end(self, name: str, ):
        while True:
            all_name_jobs = self.get_job_by_name(name)
            not_end_jobs = list(filter(lambda job: job.not_ended(), all_name_jobs))
            if len(not_end_jobs) == 0:
                return
            time.sleep(_SLEEP_INTERVAL_S)
