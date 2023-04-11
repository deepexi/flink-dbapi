job_detail_response_1 = """
{
    "jid": "16aaf923d43413315e6b4b80b1c146c1",
    "name": "insert-into_cat2.cat2_db2.t3",
    "isStoppable": false,
    "state": "RUNNING",
    "start-time": 1680836008669,
    "end-time": -1,
    "duration": 5044730,
    "maxParallelism": -1,
    "now": 1680841053399,
    "timestamps": {
        "CANCELLING": 0,
        "RUNNING": 1680836008785,
        "INITIALIZING": 1680836008669,
        "RECONCILING": 0,
        "RESTARTING": 0,
        "CANCELED": 0,
        "SUSPENDED": 0,
        "CREATED": 1680836008672,
        "FAILING": 0,
        "FAILED": 0,
        "FINISHED": 0
    },
    "vertices": [{
            "id": "bc764cd8ddf7a0cff126f51c16239658",
            "name": "Source: topic01[5]",
            "maxParallelism": 128,
            "parallelism": 1,
            "status": "RUNNING",
            "start-time": 1680836008862,
            "end-time": -1,
            "duration": 5044537,
            "tasks": {
                "RUNNING": 1,
                "DEPLOYING": 0,
                "FAILED": 0,
                "CANCELING": 0,
                "SCHEDULED": 0,
                "INITIALIZING": 0,
                "CREATED": 0,
                "FINISHED": 0,
                "CANCELED": 0,
                "RECONCILING": 0
            },
            "metrics": {
                "read-bytes": 0,
                "read-bytes-complete": true,
                "write-bytes": 1026,
                "write-bytes-complete": true,
                "read-records": 0,
                "read-records-complete": true,
                "write-records": 18,
                "write-records-complete": true,
                "accumulated-backpressured-time": 0,
                "accumulated-idle-time": 5130251,
                "accumulated-busy-time": 0
            }
        },
        {
            "id": "feca28aff5a3958840bee985ee7de4d3",
            "name": "Source: t2[7]",
            "maxParallelism": 128,
            "parallelism": 1,
            "status": "FINISHED",
            "start-time": 1680836008862,
            "end-time": 1680836018283,
            "duration": 9421,
            "tasks": {
                "RUNNING": 0,
                "DEPLOYING": 0,
                "FAILED": 0,
                "CANCELING": 0,
                "SCHEDULED": 0,
                "INITIALIZING": 0,
                "CREATED": 0,
                "FINISHED": 1,
                "CANCELED": 0,
                "RECONCILING": 0
            },
            "metrics": {
                "read-bytes": 0,
                "read-bytes-complete": true,
                "write-bytes": 79,
                "write-bytes-complete": true,
                "read-records": 0,
                "read-records-complete": true,
                "write-records": 2,
                "write-records-complete": true,
                "accumulated-backpressured-time": 0,
                "accumulated-idle-time": 0,
                "accumulated-busy-time": "NaN"
            }
        },
        {
            "id": "4bf7c1955ffe56e2106d666433eaf137",
            "name": "Join[9] -> Calc[10] -> IcebergStreamWriter",
            "maxParallelism": 128,
            "parallelism": 1,
            "status": "RUNNING",
            "start-time": 1680836008864,
            "end-time": -1,
            "duration": 5044535,
            "tasks": {
                "RUNNING": 1,
                "DEPLOYING": 0,
                "FAILED": 0,
                "CANCELING": 0,
                "SCHEDULED": 0,
                "INITIALIZING": 0,
                "CREATED": 0,
                "FINISHED": 0,
                "CANCELED": 0,
                "RECONCILING": 0
            },
            "metrics": {
                "read-bytes": 20274,
                "read-bytes-complete": true,
                "write-bytes": 59735,
                "write-bytes-complete": true,
                "read-records": 20,
                "read-records-complete": true,
                "write-records": 503,
                "write-records-complete": true,
                "accumulated-backpressured-time": 0,
                "accumulated-idle-time": 5406179,
                "accumulated-busy-time": 0
            }
        },
        {
            "id": "c2cd52d0ed10bbeee8315f931b0d8f43",
            "name": "IcebergFilesCommitter -> Sink: IcebergSink cat2.cat2_db2.t3",
            "maxParallelism": 1,
            "parallelism": 1,
            "status": "RUNNING",
            "start-time": 1680836008866,
            "end-time": -1,
            "duration": 5044533,
            "tasks": {
                "RUNNING": 1,
                "DEPLOYING": 0,
                "FAILED": 0,
                "CANCELING": 0,
                "SCHEDULED": 0,
                "INITIALIZING": 0,
                "CREATED": 0,
                "FINISHED": 0,
                "CANCELED": 0,
                "RECONCILING": 0
            },
            "metrics": {
                "read-bytes": 78853,
                "read-bytes-complete": true,
                "write-bytes": 0,
                "write-bytes-complete": true,
                "read-records": 503,
                "read-records-complete": true,
                "write-records": 0,
                "write-records-complete": true,
                "accumulated-backpressured-time": 0,
                "accumulated-idle-time": 5429094,
                "accumulated-busy-time": 0
            }
        }
    ],
    "status-counts": {
        "RUNNING": 3,
        "DEPLOYING": 0,
        "FAILED": 0,
        "CANCELING": 0,
        "SCHEDULED": 0,
        "INITIALIZING": 0,
        "CREATED": 0,
        "FINISHED": 1,
        "CANCELED": 0,
        "RECONCILING": 0
    },
    "plan": {
        "jid": "16aaf923d43413315e6b4b80b1c146c1",
        "name": "insert-into_cat2.cat2_db2.t3",
        "type": "STREAMING",
        "nodes": [{
                "id": "c2cd52d0ed10bbeee8315f931b0d8f43",
                "parallelism": 1,
                "operator": "",
                "operator_strategy": "",
                "description": "IcebergFilesCommitter<br/>+- Sink: IcebergSink cat2.cat2_db2.t3<br/>",
                "inputs": [{
                    "num": 0,
                    "id": "4bf7c1955ffe56e2106d666433eaf137",
                    "ship_strategy": "FORWARD",
                    "exchange": "pipelined_bounded"
                }],
                "optimizer_properties": {}
            },
            {
                "id": "4bf7c1955ffe56e2106d666433eaf137",
                "parallelism": 1,
                "operator": "",
                "operator_strategy": "",
                "description": "[9]:Join(joinType=[InnerJoin], where=[(id = id0)], select=[id, name, ts, id0, age], leftInputSpec=[NoUniqueKey], rightInputSpec=[NoUniqueKey])<br/>+- [10]:Calc(select=[id, name, ts, age])<br/>   +- IcebergStreamWriter<br/>",
                "inputs": [{
                        "num": 0,
                        "id": "bc764cd8ddf7a0cff126f51c16239658",
                        "ship_strategy": "HASH",
                        "exchange": "pipelined_bounded"
                    },
                    {
                        "num": 1,
                        "id": "feca28aff5a3958840bee985ee7de4d3",
                        "ship_strategy": "HASH",
                        "exchange": "pipelined_bounded"
                    }
                ],
                "optimizer_properties": {}
            },
            {
                "id": "bc764cd8ddf7a0cff126f51c16239658",
                "parallelism": 1,
                "operator": "",
                "operator_strategy": "",
                "description": "[5]:TableSourceScan(table=[[cat1, cat1_db1, topic01]], fields=[id, name, ts])<br/>",
                "optimizer_properties": {}
            },
            {
                "id": "feca28aff5a3958840bee985ee7de4d3",
                "parallelism": 1,
                "operator": "",
                "operator_strategy": "",
                "description": "[7]:TableSourceScan(table=[[cat2, cat2_db2, t2]], fields=[id, age])<br/>",
                "optimizer_properties": {}
            }
        ]
    }
}"""

job_detail_response_2 = """
{
    "jid": "5a9d043fa00fe3244421f7b06a299c4a",
    "name": "insert-into_cat2.cat2_db2.t4",
    "isStoppable": false,
    "state": "FINISHED",
    "start-time": 1680845329274,
    "end-time": 1680845330066,
    "duration": 792,
    "maxParallelism": -1,
    "now": 1680845345564,
    "timestamps": {
        "CANCELLING": 0,
        "RUNNING": 1680845329415,
        "INITIALIZING": 1680845329274,
        "RECONCILING": 0,
        "RESTARTING": 0,
        "CANCELED": 0,
        "SUSPENDED": 0,
        "CREATED": 1680845329277,
        "FAILING": 0,
        "FAILED": 0,
        "FINISHED": 1680845330066
    },
    "vertices": [{
            "id": "bc764cd8ddf7a0cff126f51c16239658",
            "name": "Source: t2[19]",
            "maxParallelism": 128,
            "parallelism": 1,
            "status": "FINISHED",
            "start-time": 1680845329496,
            "end-time": 1680845329768,
            "duration": 272,
            "tasks": {
                "RUNNING": 0,
                "DEPLOYING": 0,
                "FAILED": 0,
                "CANCELING": 0,
                "SCHEDULED": 0,
                "INITIALIZING": 0,
                "CREATED": 0,
                "FINISHED": 1,
                "CANCELED": 0,
                "RECONCILING": 0
            },
            "metrics": {
                "read-bytes": 0,
                "read-bytes-complete": true,
                "write-bytes": 72,
                "write-bytes-complete": true,
                "read-records": 0,
                "read-records-complete": true,
                "write-records": 2,
                "write-records-complete": true,
                "accumulated-backpressured-time": 0,
                "accumulated-idle-time": 0,
                "accumulated-busy-time": "NaN"
            }
        },
        {
            "id": "feca28aff5a3958840bee985ee7de4d3",
            "name": "Source: t2[21]",
            "maxParallelism": 128,
            "parallelism": 1,
            "status": "FINISHED",
            "start-time": 1680845329498,
            "end-time": 1680845329769,
            "duration": 271,
            "tasks": {
                "RUNNING": 0,
                "DEPLOYING": 0,
                "FAILED": 0,
                "CANCELING": 0,
                "SCHEDULED": 0,
                "INITIALIZING": 0,
                "CREATED": 0,
                "FINISHED": 1,
                "CANCELED": 0,
                "RECONCILING": 0
            },
            "metrics": {
                "read-bytes": 0,
                "read-bytes-complete": true,
                "write-bytes": 88,
                "write-bytes-complete": true,
                "read-records": 0,
                "read-records-complete": true,
                "write-records": 2,
                "write-records-complete": true,
                "accumulated-backpressured-time": 0,
                "accumulated-idle-time": 0,
                "accumulated-busy-time": "NaN"
            }
        },
        {
            "id": "4bf7c1955ffe56e2106d666433eaf137",
            "name": "HashJoin[23] -> Calc[24] -> IcebergStreamWriter",
            "maxParallelism": 128,
            "parallelism": 1,
            "status": "FINISHED",
            "start-time": 1680845329770,
            "end-time": 1680845329871,
            "duration": 101,
            "tasks": {
                "RUNNING": 0,
                "DEPLOYING": 0,
                "FAILED": 0,
                "CANCELING": 0,
                "SCHEDULED": 0,
                "INITIALIZING": 0,
                "CREATED": 0,
                "FINISHED": 1,
                "CANCELED": 0,
                "RECONCILING": 0
            },
            "metrics": {
                "read-bytes": 160,
                "read-bytes-complete": true,
                "write-bytes": 730,
                "write-bytes-complete": true,
                "read-records": 4,
                "read-records-complete": true,
                "write-records": 1,
                "write-records-complete": true,
                "accumulated-backpressured-time": 0,
                "accumulated-idle-time": 0,
                "accumulated-busy-time": 23
            }
        },
        {
            "id": "c2cd52d0ed10bbeee8315f931b0d8f43",
            "name": "IcebergFilesCommitter -> Sink: IcebergSink cat2.cat2_db2.t4",
            "maxParallelism": 1,
            "parallelism": 1,
            "status": "FINISHED",
            "start-time": 1680845329872,
            "end-time": 1680845330053,
            "duration": 181,
            "tasks": {
                "RUNNING": 0,
                "DEPLOYING": 0,
                "FAILED": 0,
                "CANCELING": 0,
                "SCHEDULED": 0,
                "INITIALIZING": 0,
                "CREATED": 0,
                "FINISHED": 1,
                "CANCELED": 0,
                "RECONCILING": 0
            },
            "metrics": {
                "read-bytes": 730,
                "read-bytes-complete": true,
                "write-bytes": 0,
                "write-bytes-complete": true,
                "read-records": 1,
                "read-records-complete": true,
                "write-records": 0,
                "write-records-complete": true,
                "accumulated-backpressured-time": 0,
                "accumulated-idle-time": 0,
                "accumulated-busy-time": 156
            }
        }
    ],
    "status-counts": {
        "RUNNING": 0,
        "DEPLOYING": 0,
        "FAILED": 0,
        "CANCELING": 0,
        "SCHEDULED": 0,
        "INITIALIZING": 0,
        "CREATED": 0,
        "FINISHED": 4,
        "CANCELED": 0,
        "RECONCILING": 0
    },
    "plan": {
        "jid": "5a9d043fa00fe3244421f7b06a299c4a",
        "name": "insert-into_cat2.cat2_db2.t4",
        "type": "BATCH",
        "nodes": [{
                "id": "c2cd52d0ed10bbeee8315f931b0d8f43",
                "parallelism": 1,
                "operator": "",
                "operator_strategy": "",
                "description": "IcebergFilesCommitter<br/>+- Sink: IcebergSink cat2.cat2_db2.t4<br/>",
                "inputs": [{
                    "num": 0,
                    "id": "4bf7c1955ffe56e2106d666433eaf137",
                    "ship_strategy": "FORWARD",
                    "exchange": "blocking"
                }],
                "optimizer_properties": {}
            },
            {
                "id": "4bf7c1955ffe56e2106d666433eaf137",
                "parallelism": 1,
                "operator": "",
                "operator_strategy": "",
                "description": "[23]:HashJoin(joinType=[InnerJoin], where=[(id = id0)], select=[id, id0, age], build=[left])<br/>+- [24]:Calc(select=[id, age])<br/>   +- IcebergStreamWriter<br/>",
                "inputs": [{
                        "num": 0,
                        "id": "bc764cd8ddf7a0cff126f51c16239658",
                        "ship_strategy": "HASH[id]",
                        "exchange": "blocking"
                    },
                    {
                        "num": 1,
                        "id": "feca28aff5a3958840bee985ee7de4d3",
                        "ship_strategy": "HASH[id]",
                        "exchange": "blocking"
                    }
                ],
                "optimizer_properties": {}
            },
            {
                "id": "bc764cd8ddf7a0cff126f51c16239658",
                "parallelism": 1,
                "operator": "",
                "operator_strategy": "",
                "description": "[19]:TableSourceScan(table=[[cat2, cat2_db2, t2, project=[id]]], fields=[id])<br/>",
                "optimizer_properties": {}
            },
            {
                "id": "feca28aff5a3958840bee985ee7de4d3",
                "parallelism": 1,
                "operator": "",
                "operator_strategy": "",
                "description": "[21]:TableSourceScan(table=[[cat2, cat2_db2, t2]], fields=[id, age])<br/>",
                "optimizer_properties": {}
            }
        ]
    }
}"""

job_detail_response_3 = """
{
    "jid": "b265be7e8a8b6ee08c8d9f168ee6e2b5",
    "name": "insert-into_cat2.cat2_db2.t2_mirror",
    "isStoppable": false,
    "state": "RUNNING",
    "start-time": 1680862396159,
    "end-time": -1,
    "duration": 27210,
    "maxParallelism": -1,
    "now": 1680862423369,
    "timestamps": {
        "FAILING": 0,
        "CREATED": 1680862396261,
        "FINISHED": 0,
        "RECONCILING": 0,
        "INITIALIZING": 1680862396159,
        "CANCELLING": 0,
        "RUNNING": 1680862396532,
        "CANCELED": 0,
        "SUSPENDED": 0,
        "FAILED": 0,
        "RESTARTING": 0
    },
    "vertices": [{
            "id": "bc764cd8ddf7a0cff126f51c16239658",
            "name": "Source: Iceberg table (cat2.cat2_db2.t2) monitor",
            "maxParallelism": 128,
            "parallelism": 1,
            "status": "RUNNING",
            "start-time": 1680862396751,
            "end-time": -1,
            "duration": 26618,
            "tasks": {
                "CANCELED": 0,
                "FAILED": 0,
                "INITIALIZING": 0,
                "CANCELING": 0,
                "CREATED": 0,
                "DEPLOYING": 0,
                "SCHEDULED": 0,
                "RUNNING": 1,
                "FINISHED": 0,
                "RECONCILING": 0
            },
            "metrics": {
                "read-bytes": 0,
                "read-bytes-complete": true,
                "write-bytes": 1044,
                "write-bytes-complete": true,
                "read-records": 0,
                "read-records-complete": true,
                "write-records": 1,
                "write-records-complete": true,
                "accumulated-backpressured-time": 0,
                "accumulated-idle-time": 0,
                "accumulated-busy-time": "NaN"
            }
        },
        {
            "id": "20ba6b65f97481d5570070de90e4e791",
            "name": "t2[1] -> IcebergStreamWriter",
            "maxParallelism": 128,
            "parallelism": 1,
            "status": "RUNNING",
            "start-time": 1680862396762,
            "end-time": -1,
            "duration": 26607,
            "tasks": {
                "CANCELED": 0,
                "FAILED": 0,
                "INITIALIZING": 0,
                "CANCELING": 0,
                "CREATED": 0,
                "DEPLOYING": 0,
                "SCHEDULED": 0,
                "RUNNING": 1,
                "FINISHED": 0,
                "RECONCILING": 0
            },
            "metrics": {
                "read-bytes": 1086,
                "read-bytes-complete": true,
                "write-bytes": 715,
                "write-bytes-complete": true,
                "read-records": 1,
                "read-records-complete": true,
                "write-records": 1,
                "write-records-complete": true,
                "accumulated-backpressured-time": 0,
                "accumulated-idle-time": 12620,
                "accumulated-busy-time": 660
            }
        },
        {
            "id": "b5c8d46f3e7b141acf271f12622e752b",
            "name": "IcebergFilesCommitter -> Sink: IcebergSink cat2.cat2_db2.t2_mirror",
            "maxParallelism": 1,
            "parallelism": 1,
            "status": "RUNNING",
            "start-time": 1680862396774,
            "end-time": -1,
            "duration": 26595,
            "tasks": {
                "CANCELED": 0,
                "FAILED": 0,
                "INITIALIZING": 0,
                "CANCELING": 0,
                "CREATED": 0,
                "DEPLOYING": 0,
                "SCHEDULED": 0,
                "RUNNING": 1,
                "FINISHED": 0,
                "RECONCILING": 0
            },
            "metrics": {
                "read-bytes": 757,
                "read-bytes-complete": true,
                "write-bytes": 0,
                "write-bytes-complete": true,
                "read-records": 1,
                "read-records-complete": true,
                "write-records": 0,
                "write-records-complete": true,
                "accumulated-backpressured-time": 0,
                "accumulated-idle-time": 12943,
                "accumulated-busy-time": 448
            }
        }
    ],
    "status-counts": {
        "CANCELED": 0,
        "FAILED": 0,
        "INITIALIZING": 0,
        "CANCELING": 0,
        "CREATED": 0,
        "DEPLOYING": 0,
        "SCHEDULED": 0,
        "RUNNING": 3,
        "FINISHED": 0,
        "RECONCILING": 0
    },
    "plan": {
        "jid": "b265be7e8a8b6ee08c8d9f168ee6e2b5",
        "name": "insert-into_cat2.cat2_db2.t2_mirror",
        "type": "STREAMING",
        "nodes": [{
                "id": "b5c8d46f3e7b141acf271f12622e752b",
                "parallelism": 1,
                "operator": "",
                "operator_strategy": "",
                "description": "IcebergFilesCommitter<br/>+- Sink: IcebergSink cat2.cat2_db2.t2_mirror<br/>",
                "inputs": [{
                    "num": 0,
                    "id": "20ba6b65f97481d5570070de90e4e791",
                    "ship_strategy": "FORWARD",
                    "exchange": "pipelined_bounded"
                }],
                "optimizer_properties": {}
            },
            {
                "id": "20ba6b65f97481d5570070de90e4e791",
                "parallelism": 1,
                "operator": "",
                "operator_strategy": "",
                "description": "[1]:TableSourceScan(table=[[cat2, cat2_db2, t2]], fields=[id, age], hints=[[[OPTIONS options:{streaming=true, monitor-interval=1s}]]])<br/>+- IcebergStreamWriter<br/>",
                "inputs": [{
                    "num": 0,
                    "id": "bc764cd8ddf7a0cff126f51c16239658",
                    "ship_strategy": "FORWARD",
                    "exchange": "pipelined_bounded"
                }],
                "optimizer_properties": {}
            },
            {
                "id": "bc764cd8ddf7a0cff126f51c16239658",
                "parallelism": 1,
                "operator": "",
                "operator_strategy": "",
                "description": "Source: Iceberg table (cat2.cat2_db2.t2) monitor<br/>",
                "optimizer_properties": {}
            }
        ]
    }
}
"""
