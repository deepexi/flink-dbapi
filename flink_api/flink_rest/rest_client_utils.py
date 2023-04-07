import re
from dataclasses import dataclass
from typing import List

from flink_api.flink_rest.response_models import PlanNode, FlinkJobDetail, Relation

# [5]:TableSourceScan(table=[[cat1, cat1_db1, topic01]], fields=[id, name, ts])<br/>
re_table_source_scan = re.compile(r"(.*?)TableSourceScan\(table=\[\[(.*?)]](.*)\)")


class RestClientUtils:
    @staticmethod
    def parse_job_detail(j: dict):
        plan = j["plan"]

        plan_nodes = []
        for node in plan["nodes"]:
            description = node["description"]
            match = re.search(re_table_source_scan, description)
            if not match:  # NOT a TableSourceScan node
                continue
            relation = Relation.from_relation_str(match[2])
            jid = node["id"]
            plan_node_status = list(filter(lambda v: v["id"] == jid, j["vertices"]))[0]["status"]
            plan_nodes.append(PlanNode(jid, description, True, relation, plan_node_status))

        return FlinkJobDetail(j["jid"], j["name"], j["state"], plan_nodes, plan["type"])
