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
    from description + metrics
    "[12]:TableSourceScan(table=[[cat2, cat2_db2, t2, project=[id]]], fields=[id])<br/>"
    [1]:TableSourceScan(table=[[cat2, cat2_db2, t2]], fields=[id, age], hints=[[[OPTIONS options:{streaming=true, monitor-interval=1s}]]])<br/>+- IcebergStreamWriter<br/>
    """

    id: str
    description: str
    # is_table_source_scan: bool # always True
    flink_hints: dict
    relation: Relation
    plan_node_status: str  # RUNNING | FINISHED | ...
    # metrics
    metrics_accumulated_backpressured_time: int
    metrics_accumulated_busy_time: int
    metrics_accumulated_idle_time: int

    def is_hint_streaming(self):
        if self.flink_hints:
            return self.flink_hints.get("streaming") == "true"
        return False

    def is_node_finished(self):
        return self.plan_node_status == "FINISHED"


@dataclass
class FlinkJob:
    jid: str
    name: str
    state: str
    start_time: int

    def __init__(self, jid, name, state, start_time):
        self.jid = jid
        self.name = name
        self.state = state
        self.start_time = start_time

    def is_running(self):
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
        return not (self.is_finished())

    def is_finished(self):
        return self.state in ["CANCELED", "FINISHED"]


@dataclass
class FlinkJobDetail(FlinkJob):
    plan_nodes: List[PlanNode]  # plan_nodes only keep "TableSourceScan" node
    plan_type: str

    def __init__(self, jid, name, state, start_time, plan_nodes, plan_type):
        FlinkJob.__init__(self, jid, name, state, start_time)
        self.plan_nodes = plan_nodes
        self.plan_type = plan_type

    def is_plan_type_batch(self):
        return self.plan_type == "batch"

    def list_running_table_source_scan_node(self) -> List[Relation]:
        running_table_src_scan = []
        for _node in self.plan_nodes:
            if _node.plan_node_status == "RUNNING":
                running_table_src_scan.append(_node.relation)
        return running_table_src_scan

    def plan_node_has_hint_streaming(self):
        for node in self.plan_nodes:
            if node.is_hint_streaming():
                return True
        return False


class ParsePolicy:
    nodes_type = "TableSourceScan"
