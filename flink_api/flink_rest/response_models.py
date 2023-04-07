from dataclasses import dataclass
from typing import List


@dataclass
class Relation:
    SEPERATOR = "."

    catalog: str
    database: str
    table: str

    @staticmethod
    def from_relation_str(relation_str, seperator=","):
        parts = relation_str.split(seperator)
        if len(parts) > 3:
            parts = parts[0:3]
        while len(parts) < 3:
            raise Exception("Relation require 3 catalog.database.table")
        return Relation(parts[0].strip(), parts[1].strip(), parts[2].strip())

    def __repr__(self):
        return f"{self.catalog}{Relation.SEPERATOR}{self.database}{Relation.SEPERATOR}{self.table}"


@dataclass
class PlanNode:
    """
    description: "[12]:TableSourceScan(table=[[cat2, cat2_db2, t2, project=[id]]], fields=[id])<br/>"
    """

    id: str
    description: str
    is_table_source_scan: bool
    relation: Relation
    plan_node_status: str


@dataclass
class FlinkJob:
    jid: str
    name: str
    state: str

    def __init__(self, jid, name, state):
        self.jid = jid
        self.name = name
        self.state = state

    def _is_running(self):
        return self.state == "RUNNING"

    def can_cancel(self):
        """TODO not very sure"""
        return self.state in [
            "RUNNING",
            "INITIALIZING",
            "CREATED",
            "SUSPENDED",
            "RESTARTING",
            "RECONCILING",
        ]

    def is_not_finished(self):
        return not (self.state in ["CANCELED", "FINISHED"])


@dataclass
class FlinkJobDetail(FlinkJob):
    # plan_nodes only keep "TableSourceScan" node
    plan_nodes: List[PlanNode]
    plan_type: str

    def __init__(self, jid, name, state, plan_nodes, plan_type):
        FlinkJob.__init__(self, jid, name, state)
        self.plan_nodes = plan_nodes
        self.plan_type = plan_type

    def list_running_table_source_scan_node(self) -> List[Relation]:
        running_table_src_scan = []
        for _node in self.plan_nodes:
            if _node.is_table_source_scan and _node.plan_node_status == "RUNNING":
                running_table_src_scan.append(_node.relation)
        return running_table_src_scan


class ParsePolicy:
    nodes_type = "TableSourceScan"
