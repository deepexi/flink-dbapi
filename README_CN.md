# 这个代码库是什么

- 把 flink + catalog 作为一个整体, 视为 database, 这个 db 暴露出来的 api

# 有什么用

- 给 dbt-flink 提供支撑

# 使用 TODO

```python

```

# 支持的 hint

- 设置 flink job name `/** 'job_name'='group_name:timestamp' */`
  - group_name:timestamp 取值 _projectName_sessionName_modelName:20230101235959
  - group_name:timestamp 唯一
  - 相同group_name会认为是同一个任务的多次执行
- 标记此次 query 是dbt test `/** 'test_query' */`

- TODO

```sql
/** 'fetch_max.name'='111' */
/** 'fetch_timeout_ms'='100' */
/** 'mode'='batch' | 'streaming' */
```
