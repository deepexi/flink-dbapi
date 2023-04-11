from typing import List, Optional

import requests

from flink_api.flink_rest.response_models import FlinkJob, FlinkJobDetail
from flink_api.flink_rest.job_detail_parser import JobDetailParser

_SCHEMA_HTTP = "http"

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
    def __init__(self, host_port):
        self.host_port = host_port

    def job_list(self) -> List[FlinkJob]:
        url = f"{_SCHEMA_HTTP}://{self.host_port}/jobs/overview"
        response = requests.get(url=url)
        if response.status_code != 200:
            raise Exception("flink api error: ", response.status_code)
        jobs = response.json()["jobs"]
        return [FlinkJob(j["jid"], j["name"], j["state"], j["start-time"]) for j in jobs]

    def job_detail(self, job_id: str) -> FlinkJobDetail:
        url = f"{_SCHEMA_HTTP}://{self.host_port}/jobs/{job_id}"
        response = requests.get(url=url)
        if response.status_code != 200:
            raise Exception("flink api error: ", response.status_code)
        job_detail = response.json()
        return JobDetailParser.parse_job_detail(job_detail)

    def cancel_job_by_id(self, job_id):
        url = f"{_SCHEMA_HTTP}://{self.host_port}/jobs/{job_id}?mode=cancel"
        response = requests.patch(url=url)
        if response.status_code != 202:  # accepted
            raise Exception("flink api error: ", response.status_code)

    def _list_job_by_name(self, name: str) -> List[FlinkJob]:
        jobs = self.job_list()
        return list(filter(lambda job: (job.name == name), jobs))

    def cancel_job_by_name_prefix(self, name_prefix: str) -> None:
        jobs: List[FlinkJob] = self.job_list()
        for j in jobs:
            if j.name.startswith(name_prefix) and j.is_not_finished():
                self.cancel_job_by_id(j.jid)

    def get_latest_start_job_by_name(self, name: str) -> Optional[FlinkJob]:
        jobs = self._list_job_by_name(name)
        if len(jobs) == 0:
            return None
        latest_start_job = jobs[0]
        for i in range(1, len(jobs)):
            if jobs[i].start_time > latest_start_job.start_time:
                latest_start_job = jobs[i]
        return latest_start_job

    def cancel_job_by_name_if_possible(self, name: str):
        jobs = self._list_job_by_name(name)
        for j in jobs:
            if j.is_not_finished():
                self.cancel_job_by_id(j.jid)
