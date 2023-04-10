import json
import unittest

from flink_api.flink_rest.response_models import Relation
from flink_api.flink_rest.rest_client_utils import RestClientUtils
from test.flink_rest.res_job_detail import (
    job_detail_response_1,
    job_detail_response_2,
    job_detail_response_3,
)


class TestRestClientUtils(unittest.TestCase):
    def test_parse_job_detail_1(self):
        job_detail_1 = RestClientUtils.parse_job_detail(json.loads(job_detail_response_1))
        self.assertEquals(job_detail_1.jid, "16aaf923d43413315e6b4b80b1c146c1")
        self.assertEquals(job_detail_1.state, "RUNNING")
        self.assertEquals(job_detail_1.plan_type, "STREAMING")
        self.assertEquals(len(job_detail_1.plan_nodes), 2)
        self.assertEquals(job_detail_1.plan_nodes[0].plan_node_status, "RUNNING")
        self.assertEquals(
            job_detail_1.plan_nodes[0].relation, Relation("cat1", "cat1_db1", "topic01")
        )
        self.assertEquals(job_detail_1.plan_nodes[1].plan_node_status, "FINISHED")
        self.assertEquals(job_detail_1.plan_nodes[1].relation, Relation("cat2", "cat2_db2", "t2"))

        job_detail_2 = RestClientUtils.parse_job_detail(json.loads(job_detail_response_2))
        self.assertEquals(job_detail_2.state, "FINISHED")
        self.assertEquals(job_detail_2.plan_type, "BATCH")
        self.assertEquals(len(job_detail_2.plan_nodes), 2)

    def test_parse_job_detail_2(self):
        job_detail_2 = RestClientUtils.parse_job_detail(json.loads(job_detail_response_2))
        self.assertEquals(job_detail_2.state, "FINISHED")
        self.assertEquals(job_detail_2.plan_type, "BATCH")
        self.assertEquals(len(job_detail_2.plan_nodes), 2)

    def test_parse_job_detail_3(self):
        job_detail_3 = RestClientUtils.parse_job_detail(json.loads(job_detail_response_3))
        self.assertEquals(job_detail_3.state, "RUNNING")
        self.assertEquals(job_detail_3.plan_type, "STREAMING")
        self.assertEquals(len(job_detail_3.plan_nodes), 1)
        self.assertEquals(job_detail_3.plan_nodes[0].plan_node_status, "RUNNING")
        self.assertEquals(job_detail_3.plan_nodes[0].relation, Relation("cat2", "cat2_db2", "t2"))
        pass
