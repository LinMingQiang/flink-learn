# Flink Learn
---
|   Flink                 | scala version      |Kafka version   |
|:------------------:|:------------------:|:------------------:|
| **1.10.0**    | 2.11+             | 0.10+               |
---
```
Maven 模板 : https://www.mvnjar.com/org.apache.flink/flink-quickstart-scala/jar.html <br>
注意 ： 当在idea上调试时，先将 es-shade install。然后 ignore Project：es-shade ；否则有冲突问题
```
**Flink Submit**
```
1：单独资源： flink run -m yarn-cluster --yarnname _wc -p 5 -yn 4 -yjm 1024 -ytm 1024 -ys 2 WordCount.jar
2：共享空间： yarn-session.sh -n 5 -jm 1024 -tm 1024 -s 2 // 在yarn提前开辟一个资源空间 application_1567318548013_0001。 <br>
在开辟的空间上提交任务： flink run -m yarn-cluster --yarnname flink_wc -p 5 -yn 4 -yjm 1024 -ytm 1024 -ys 2 -yid application_1567318548013_0001 WordCount.jar <br>
```
**State Manager**
```
关于状态的TTL
  RocksDB ：rocksDBStateBackend.enableTtlCompactionFilter() // 启用ttl后台增量清除功能
  或者 ：flink-conf :  state.backend.rocksdb.ttl.compaction.filter.enabled: true
在 RichMapFunction中
val desc = new ValueStateDescriptor.....
desc.enableTimeToLive(    StateTtlConfig
      .newBuilder(Time.minutes(timeOut)) // 2个小时
      .updateTtlOnReadAndWrite() // 每次读取或者更新这个key的值的时候都对ttl做更新，所以清理的时间是 lastpdatetime + outtime
      .cleanupFullSnapshot() // 创建完整快照时清理
      .cleanupInBackground()
      .cleanupInRocksdbCompactFilter() // 达到100个过期就清理？
      .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
      .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
      .build();)
```
**Flink API**
```
[Table DataStream互转](https://ci.apache.org/projects/flink/flink-docs-stable/dev/table/common.html#convert-a-datastream-or-dataset-into-a-table)
StreamExecutionEnvironment : 流式相关。不能使用SQL的API。如果要在流里面用SQL，使用下面的
StreamTableEnvironment ： 流式SQL相关。可以使用 SQL的API。如果要用Stream相关的，需要将tableData.toRetractStream[Row]
```
---
