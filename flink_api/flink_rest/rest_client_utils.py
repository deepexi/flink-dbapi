import json
import re
from dataclasses import dataclass
from typing import List

from flink_api.flink_rest.response_models import PlanNode, FlinkJobDetail, Relation

# [5]:TableSourceScan(table=[[cat1, cat1_db1, topic01]], fields=[id, name, ts])<br/>
#     [1]:TableSourceScan(table=[[cat2, cat2_db2, t2]], fields=[id, age], hints=[[[OPTIONS options:{streaming=true, monitor-interval=1s}]]])<br/>+- IcebergStreamWriter<br/>
re_table_source_scan = re.compile(r"(.*?)TableSourceScan\(table=\[\[(.*?)]](.*)\)")


# re_flink_hints = re.compile(r"(.*?)TableSourceScan\((.*)hints=\[\[\[OPTIONS option:(.*?)]]](.*)\)")


class RestClientUtils:
    @staticmethod
    def parse_job_detail(j: dict) -> FlinkJobDetail:
        plan = j["plan"]

        plan_nodes = []
        for node in plan["nodes"]:
            description = node["description"]

            match_table_src_scan = re.search(re_table_source_scan, description)
            if not match_table_src_scan:  # NOT a TableSourceScan node
                continue

            flink_hint = {}
            re_flink_hints = re.compile(
                r"(.*?)TableSourceScan\((.*)hints=\[\[\[OPTIONS\s+options:\{(.*?)\}]]](.*)\)"
            )
            match_flink_hints = re.search(re_flink_hints, description)
            if match_flink_hints:
                hints_str = match_flink_hints[3]
                kvs = hints_str.split(",")
                for kv in kvs:
                    ele = kv.split("=")
                    flink_hint[ele[0].strip()] = ele[1].strip()

            relation = Relation.from_relation_str(match_table_src_scan[2])
            jid = node["id"]
            plan_node_status = list(filter(lambda v: v["id"] == jid, j["vertices"]))[0]["status"]
            plan_nodes.append(PlanNode(jid, description, flink_hint, relation, plan_node_status))

        return FlinkJobDetail(
            j["jid"], j["name"], j["state"], j["start-time"], plan_nodes, plan["type"]
        )
