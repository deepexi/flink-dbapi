import unittest

from flink_api.sql_gateway.payload_parser import SqlGatewayPayload

literal = """
{
    "results": {
        "columns": [
            {
                "name": "result",
                "logicalType": {
                    "type": "VARCHAR",
                    "nullable": true,
                    "length": 2147483647
                },
                "comment": null
            }
        ],
        "data": [
            {
                "kind": "INSERT",
                "fields": [
                    "OK"
                ]
            }
        ]
    },
    "resultType": "PAYLOAD",
    "nextResultUri": "/v1/sessions/_session/operations/_operation/result/1"
}
"""


class TestSqlGatewayPayload(unittest.TestCase):
    def test_parse(self):
        r = SqlGatewayPayload.parse(literal)
        self.assertTrue(r.has_next())
        self.assertEquals(r.next_result_uri, "/v1/sessions/_session/operations/_operation/result/1")
        print(r)
