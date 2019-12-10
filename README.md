# flink-demo 
flink 1.9.0 <br>
kafka 0.10 <br>
scala 2.11 <br>
整体流程示意 <br>
kafka ->  flink (state管理) -> hbase(redis, es ) <br>

idea 创建 maven 模板 <br>
https://www.mvnjar.com/org.apache.flink/flink-quickstart-scala/jar.html <br>
注意 ： 当在idea上调试时，先将 es-shade install。然后 ignore Project：es-shade ；否则有冲突问题
```
flink 提交 <br>
* ./yarn-session.sh -n 5 -jm 1024 -tm 1024 -s 2 // 在yarn提前开辟一个资源空间。 <br>
* flink run ../examples/batch/WordCount.jar <br>
  或者 <br>
flink run -m yarn-cluster --yarnname flink_word_count -p 5 -yn 4 -yjm 1024 -ytm 1024 -ys 2 -yid application_1567318548013_186353 ../examples/batch/WordCount.jar <br>
 * ./flink run -m yarn-cluster --yarnname flink_word_count -p 5 -yn 4 -yjm 1024 -ytm 1024 -ys 2 ../examples/batch/WordCount.jar <br>
```
```$xslt
关于状态得TTL
  RocksDB ：rocksDBStateBackend.enableTtlCompactionFilter() // 启用ttl后台增量清除功能
   或者 ：flink-conf :  state.backend.rocksdb.ttl.compaction.filter.enabled: true
val ttlConfig = StateTtlConfig
    .newBuilder(Time.seconds(7200)) // 2个小时
    .cleanupInRocksdbCompactFilter(100)
    .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
    .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
    .build();
```
