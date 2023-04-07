FLINK_STREAMING_CONNECTOR_LIST = []


class FlinkUtils:
    @staticmethod
    def is_table_streaming_by_ddl(sql_ddl: str) -> bool:
        """check a table is a streaming src or not"""
        return False

    @staticmethod
    def _extract_with_properties_from_ddl(_ddl: str) -> dict:
        pass
