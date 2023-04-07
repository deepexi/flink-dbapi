import json
from dataclasses import dataclass
from typing import List, Any


@dataclass
class DataItem:
    def __init__(self, kind: str, fields: List[Any]):
        self.kind = kind
        self.fields = fields


@dataclass
class Column:
    def __init__(self, name: str, logicalType: dict, comment: str = None):
        self.name: str = name
        self.logical_type = logicalType
        self.comment: str = comment


@dataclass
class SqlGatewayPayload:
    def __init__(self, columns, data, result_type, next_result_uri):
        self.columns = columns
        self.data = data
        self.result_type = result_type
        self.next_result_uri = next_result_uri

    def has_next(self):
        return self.result_type != "EOS"

    @staticmethod
    def parse(txt_or_dict):
        if isinstance(txt_or_dict, str):
            j = json.loads(txt_or_dict)
        else:
            j = txt_or_dict

        _columns = []
        for _dict in j["results"]["columns"]:
            column = Column(**_dict)
            _columns.append(column)

        _data = []
        for _dict in j["results"]["data"]:
            data_item = DataItem(**_dict)
            _data.append(data_item)
        return SqlGatewayPayload(_columns, _data, j["resultType"], j["nextResultUri"])
