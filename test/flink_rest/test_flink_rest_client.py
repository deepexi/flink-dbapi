import unittest

from flink_api.flink_rest.flink_rest_client import FlinkRestClient


class TestFlinkRestClient(unittest.TestCase):
    def test_job_list(self):
        client = FlinkRestClient("localhost:8081")
        job_list = client.job_list()
        self.assertIsNotNone(job_list)
