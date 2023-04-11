from flink_api.log import get_logger

import sqlglot

FLINK_STREAMING_CONNECTOR_LIST = []


class FlinkDdlUtils:
    """parse sql from flink-sql: show create table xx.xx.xx
    to get information if this table is streaming source
    """

    @staticmethod
    def _sql_pre_process(sql_raw):
        sql = sql_raw.replace("\n", " ").strip().lower()
        # backtick replace to '
        sql = sql.replace("`", "'")
        return sql.strip()

    @staticmethod
    def is_table_streaming_by_ddl(sql_ddl: str) -> bool:
        """check a table is a streaming src or not"""
        sql = FlinkDdlUtils._sql_pre_process(sql_ddl)
        parsed_sql = sqlglot.parse_one(sql)
        args = parsed_sql.args
        if not args:  # this table has not with properties
            return False
        properties = args["properties"]
        if not properties:
            return False
        if not properties.args["expressions"]:
            return False

        _properties = properties.args["expressions"]
        kv_pair = FlinkDdlUtils.properties_to_dic(_properties)

        # case: has connector
        if kv_pair.get("connector"):  # no connector
            return FlinkDdlUtils.is_connector_stream_src(kv_pair, parsed_sql)

        # no connector
        return False

    @staticmethod
    def _extract_with_properties_from_ddl(_ddl: str) -> dict:
        pass

    @staticmethod
    def properties_to_dic(properties) -> dict:
        kv_pair = {}
        for p in properties:
            kv = str(p).split("=")
            _key = kv[0].lower()
            _val = kv[1].lower()[1:-1]  # remove single quote
            kv_pair[_key] = _val
        return kv_pair

    @staticmethod
    def is_connector_stream_src(kv_pair, parsed_sql) -> bool:
        connector = kv_pair.get("connector")
        if connector == "kafka":
            return not FlinkDdlUtils._is_kafka_bounded(connector, kv_pair)
        if connector == "filesystem":
            return False
        if connector == "datagen":
            return FlinkDdlUtils._is_datagen_bounded(connector, kv_pair, parsed_sql)
        raise Exception(f"TODO connector={connector} not recognized, should refine this method")

    @staticmethod
    def _is_kafka_bounded(connector, kv_pair) -> bool:
        assert connector == "kafka", "connector must be kafka"
        is_bounded = kv_pair.get("bounded")
        return is_bounded == "true"

    @staticmethod
    def _is_datagen_bounded(connector, kv_pair, parsed_sql):
        assert connector == "datagen", "connector must be datagen"
        columns_expr = parsed_sql.args["this"].args["expressions"]
        columns_name = []
        for c in columns_expr:
            columns_name.append(c.alias_or_name)
        for c in columns_name:
            _key = f"fields.{c}.end"
            if _key in kv_pair.keys():
                return False
        return True
