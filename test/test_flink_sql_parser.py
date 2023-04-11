import unittest

from flink_api.flink_sql_parser import (
    FlinkSqlParseHelper,
    FLINK_SQL_TYPE_SET,
    FLINK_SQL_TYPE_USE,
    FLINK_SQL_TYPE_SHOW,
    FLINK_SQL_TYPE_DROP,
    FLINK_SQL_TYPE_CREATE_CATALOG,
    FLINK_SQL_TYPE_CREATE_DATABASE,
    FLINK_SQL_TYPE_CREATE_TABLE,
    FLINK_SQL_TYPE_CREATE_VIEW,
    FLINK_SQL_TYPE_SELECT,
    FLINK_SQL_TYPE_DESCRIBE,
    FLINK_SQL_TYPE_INSERT,
    FLINK_SQL_TYPE_UNKNOWN,
    FLINK_SQL_TYPE_CTAS,
)


class TestFlinkSqlTypeHelper(unittest.TestCase):
    def test_sql_pre_process(self):
        _sql = " {} select * from aa {} *///***"
        sql = _sql.format("/* __dbt_'pipeline.name'='foo' */", "/* should_be_removed */")
        self.assertEquals(_sql.format("", "").strip(), FlinkSqlParseHelper._sql_pre_process(sql))

    def test_extract_dbt_hint(self):
        # _sql = " {} "
        _sql = " {} select * from aa {} /* 'foo'='bar' */ {}"
        sql = _sql.format(
            "/** 'pipeline.name'='foo' */",
            "/** 'key1'='value1' 'test' */",
            "/** 'key3'='value3' */",
        )
        expect_hint = {"pipeline.name": "foo", "key1": "value1", "test": None, "key3": "value3"}
        hints = FlinkSqlParseHelper.extract_dbt_hint(sql)
        self.assertEquals(len(hints), 4)
        self.assertEquals(hints, expect_hint)

    def test_sql_type_verdict_simple(self):
        self.assertEquals(FLINK_SQL_TYPE_SET, FlinkSqlParseHelper.sql_type_verdict("set xx=yy"))
        #
        self.assertEquals(FLINK_SQL_TYPE_USE, FlinkSqlParseHelper.sql_type_verdict(" use catalog cat1"))
        self.assertEquals(FLINK_SQL_TYPE_USE, FlinkSqlParseHelper.sql_type_verdict("use db1 "))
        self.assertEquals(FLINK_SQL_TYPE_USE, FlinkSqlParseHelper.sql_type_verdict("use  cat1.db1"))
        #
        self.assertEquals(FLINK_SQL_TYPE_SHOW, FlinkSqlParseHelper.sql_type_verdict("show catalogs"))
        self.assertEquals(FLINK_SQL_TYPE_SHOW, FlinkSqlParseHelper.sql_type_verdict("show current catalog"))
        self.assertEquals(FLINK_SQL_TYPE_SHOW, FlinkSqlParseHelper.sql_type_verdict("show databases"))
        self.assertEquals(FLINK_SQL_TYPE_SHOW, FlinkSqlParseHelper.sql_type_verdict("show current database"))
        self.assertEquals(FLINK_SQL_TYPE_SHOW, FlinkSqlParseHelper.sql_type_verdict("show tables"))
        self.assertEquals(FLINK_SQL_TYPE_SHOW, FlinkSqlParseHelper.sql_type_verdict("show views"))
        #
        self.assertEquals(FLINK_SQL_TYPE_DESCRIBE, FlinkSqlParseHelper.sql_type_verdict("describe t1"))
        self.assertEquals(FLINK_SQL_TYPE_DESCRIBE, FlinkSqlParseHelper.sql_type_verdict("describe db1.t1"))
        self.assertEquals(FLINK_SQL_TYPE_DESCRIBE, FlinkSqlParseHelper.sql_type_verdict("describe c1.db1.t1"))
        #
        self.assertEquals(FLINK_SQL_TYPE_DROP, FlinkSqlParseHelper.sql_type_verdict("drop table xxx cascade "))
        self.assertEquals(FLINK_SQL_TYPE_DROP, FlinkSqlParseHelper.sql_type_verdict("drop database xxx cascade "))
        self.assertEquals(FLINK_SQL_TYPE_DROP, FlinkSqlParseHelper.sql_type_verdict("drop catalog xxx cascade "))
        #
        self.assertEquals(
            FLINK_SQL_TYPE_CREATE_CATALOG,
            FlinkSqlParseHelper.sql_type_verdict("create catalog cat1"),
        )
        self.assertEquals(
            FLINK_SQL_TYPE_CREATE_DATABASE,
            FlinkSqlParseHelper.sql_type_verdict("create database c1.db1"),
        )
        self.assertEquals(
            FLINK_SQL_TYPE_CREATE_DATABASE,
            FlinkSqlParseHelper.sql_type_verdict("create database db1"),
        )

    def test_sql_type_verdict_unknown(self):
        sql_list = [
            "this is not a sql",
        ]
        for sql in sql_list:
            self.assertEquals(FLINK_SQL_TYPE_UNKNOWN, FlinkSqlParseHelper.sql_type_verdict(sql))

    def test_sql_type_verdict_select(self):
        sql_list = [
            "select * from t2",
            "select c1, c2 from t2",
            "with foo as (select * from bar) select * from foo.bar",
            "with foo as (select * from bar), ggg as (select * from hhh) select foo.c1, ggg.c2 from foo join ggg",
        ]
        for sql in sql_list:
            self.assertEquals(FLINK_SQL_TYPE_SELECT, FlinkSqlParseHelper.sql_type_verdict(sql))

    def test_sql_type_verdict_insert(self):
        sql_list = [
            "insert into t1 values(1,1,1)",
            "insert into t1 select * from t2",
            "insert into t2_mirror select * from t2;",
        ]
        for sql in sql_list:
            self.assertEquals(FLINK_SQL_TYPE_INSERT, FlinkSqlParseHelper.sql_type_verdict(sql))

    def test_sql_type_verdict_verdict_ctas(self):
        sql_list = [
            "create table t1 as select * from t2",
            "create table `t1` as select * from t2",
            "create table t1 as select c1, c2 from t2",
            "with foo as (select * from bar) create table `a`.`b`.`t1` as select * from t2",
            "with foo as (select * from bar) create table a.b.t1 as select * from t2",
            "with foo as (select * from bar), ggg as (select * from hhh) create table t1 as select * from t2",
            "with foo as (select * from bar), ggg as (select * from hhh) create table `t1` as select * from t2",
            "with foo as (select * from bar), ggg as (select * from hhh) create table a.b.t1 as select * from t2",
            "with foo as (select * from bar), ggg as (select * from hhh) create table `a`.`b`.`t1` as select * from t2",
        ]
        for sql in sql_list:
            self.assertEquals(FLINK_SQL_TYPE_CTAS, FlinkSqlParseHelper.sql_type_verdict(sql))

    def test_sql_type_verdict_verdict_create_table(self):
        sql_list = [
            "create table t1 (c1 int, c2 timestamp)",
        ]
        for sql in sql_list:
            self.assertEquals(FLINK_SQL_TYPE_CREATE_TABLE, FlinkSqlParseHelper.sql_type_verdict(sql))

    def test_sql_type_verdict_verdict_create_view(self):
        sql_list = [
            "create view t1 as select * from t2",
            "create view t1 as select c1, c2 from t2",
            "with foo as (select * from bar) create view t1 as select * from t2",
            "with foo as (select * from bar), ggg as (select * from hhh) create view v1 as select * from v2",
        ]
        for sql in sql_list:
            self.assertEquals(FLINK_SQL_TYPE_CREATE_VIEW, FlinkSqlParseHelper.sql_type_verdict(sql))
