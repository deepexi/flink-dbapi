import re
from flink_api.log import get_logger

import sqlglot

# comment in sql /* .. */
re_remove_comment = re.compile(r"^(.*?)(/\*.*?\*/)(.*)")

# /** 'key1'='value1' 'key2'='value2' */       /*_dbt_hint_ 'key1'='value1' */
re_dbt_hint = re.compile(r"(.*?)(/\*\*(.*?)\*/)(.*)")
char_space = " "
char_equal = "="

re_create_db = re.compile(r"create\s+database\s+(\w+)")
re_create_catalog = re.compile(r"create\s+catalog\s+(\w+)")
re_create_table = re.compile(r"create[\s\n\r]+table")
re_create_view = re.compile(r"create[\s\n\r]+view")

FLINK_SQL_TYPE_UNKNOWN = "FLINK_SQL_TYPE_UNKNOWN"
FLINK_SQL_TYPE_SET = "FLINK_SQL_TYPE_SET"
FLINK_SQL_TYPE_USE = "FLINK_SQL_TYPE_USE"
FLINK_SQL_TYPE_SHOW = "FLINK_SQL_TYPE_SHOW"
FLINK_SQL_TYPE_DESCRIBE = "FLINK_SQL_TYPE_DESCRIBE"
FLINK_SQL_TYPE_DROP = "FLINK_SQL_TYPE_DROP"
FLINK_SQL_TYPE_CREATE_CATALOG = "FLINK_SQL_TYPE_CREATE_CATALOG"
FLINK_SQL_TYPE_CREATE_DATABASE = "FLINK_SQL_TYPE_CREATE_DATABASE"
FLINK_SQL_TYPE_CREATE_VIEW = "FLINK_SQL_TYPE_CREATE_VIEW"
FLINK_SQL_TYPE_CREATE_TABLE = "FLINK_SQL_TYPE_CREATE_TABLE"
# FLINK_SQL_TYPE_CTAS = "FLINK_SQL_TYPE_CTAS"
FLINK_SQL_TYPE_SELECT = "FLINK_SQL_TYPE_SELECT"
FLINK_SQL_TYPE_INSERT = "FLINK_SQL_TYPE_INSERT"

NO_JOB_REQUIRED = [
    FLINK_SQL_TYPE_SET,
    FLINK_SQL_TYPE_USE,
    FLINK_SQL_TYPE_SHOW,
    FLINK_SQL_TYPE_DROP,
    FLINK_SQL_TYPE_CREATE_CATALOG,
    FLINK_SQL_TYPE_CREATE_DATABASE,
    FLINK_SQL_TYPE_CREATE_VIEW,
]

logger = get_logger("FlinkSqlParseHelper")

SUPPORT_DBT_HINT_KEY = [
    "pipeline.name"
    "execution.runtime-mode"
]


class FlinkSqlParseHelper:

    @staticmethod
    def sql_without_flink_job(sql):
        sql_type = FlinkSqlParseHelper.sql_type_verdict(sql)
        return sql_type in NO_JOB_REQUIRED

    @staticmethod
    def sql_pre_process(sql_raw, remove_comment=True):
        # remove \n \r
        sql = sql_raw.replace("\n", " ").strip().lower()

        # remove comment
        if remove_comment:
            while match := re.search(re_remove_comment, sql):
                if match[2]:
                    sql = f"{match[1]}{match[3]}"
                    print(f"sql={sql}")
                else:
                    break
        return sql.strip()

    @staticmethod
    def sql_type_verdict(sql_raw: str):
        """check sql type"""
        sql = FlinkSqlParseHelper.sql_pre_process(sql_raw)

        if sql.startswith("set"):
            return FLINK_SQL_TYPE_SET
        if sql.startswith("use"):
            return FLINK_SQL_TYPE_USE
        if sql.startswith("show"):
            return FLINK_SQL_TYPE_SHOW
        if sql.startswith("describe"):
            return FLINK_SQL_TYPE_DESCRIBE
        if sql.startswith("drop"):
            return FLINK_SQL_TYPE_DROP
        if re.search(re_create_catalog, sql):
            return FLINK_SQL_TYPE_CREATE_CATALOG
        if re.search(re_create_db, sql):
            return FLINK_SQL_TYPE_CREATE_DATABASE

        try:
            parsed_sql = sqlglot.parse_one(sql)
            if parsed_sql.key == "create":
                if re.search(re_create_table, sql):
                    return FLINK_SQL_TYPE_CREATE_TABLE
                elif re.search(re_create_view, sql):
                    return FLINK_SQL_TYPE_CREATE_VIEW
                else:
                    return FLINK_SQL_TYPE_UNKNOWN
            elif parsed_sql.key == "select":
                return FLINK_SQL_TYPE_SELECT
            elif parsed_sql.key == "insert":
                return FLINK_SQL_TYPE_INSERT
            else:
                logger.warn("!!!unknown sql type, should refine catalog!!!")
                return FLINK_SQL_TYPE_UNKNOWN
        except Exception as ex:
            logger.error(f"sqlglot cannot parse {sql_raw}, {ex}")
            return FLINK_SQL_TYPE_UNKNOWN

    @staticmethod
    def extract_dbt_hint(sql_raw: str) -> dict:
        def _process_hint_kv(multi_kv: str, _hints: dict):
            for kv in multi_kv.strip().split(' '):
                key_value = kv.strip().split('=')
                _hints[key_value[0]] = key_value[1]

        #
        sql = FlinkSqlParseHelper.sql_pre_process(sql_raw, remove_comment=False)
        hints = {}
        while match := re.search(re_dbt_hint, sql):
            if match[3]:
                sql = f"{match[1]}{match[4]}"
                _process_hint_kv(match[3], hints)
        return hints
