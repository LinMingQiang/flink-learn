# flink-demo 
flink 1.8.0 <br>
kafka 0.10 <br>
整体流程示意 <br>
kafka ->  flink (state管理) -> hbase(redis) <br>

idea 创建 maven 模板 <br>
https://www.mvnjar.com/org.apache.flink/flink-quickstart-scala/jar.html <br>

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
    import org.apache.flink.api.common.state.StateTtlConfig;
    import org.apache.flink.api.common.state.ValueStateDescriptor;
    import org.apache.flink.api.common.time.Time;
 
    StateTtlConfig ttlConfig = StateTtlConfig
        .newBuilder(Time.seconds(1))
        .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
        .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
        .build();
      
    ValueStateDescriptor<String> stateDescriptor = new ValueStateDescriptor<>("text state", String.class);
    stateDescriptor.enableTimeToLive(ttlConfig);
状态得清理
    这意味着默认情况下，如果未读取过期状态，则不会将其删除，可能会导致状态不断增长。这可能会在将来的版本中更改。
    此外，您可以在拍摄完整状态快照时激活清除操作，这将减小其大小。在当前实现下不会清除本地状态，但是如果从先前的快照还原，则不会包括已删除的过期状态。可以在以下位置配置StateTtlConfig：
   StateTtlConfig ttlConfig = StateTtlConfig
    .newBuilder(Time.seconds(1))
    .cleanupFullSnapshot()
    .build();
后台清理
import org.apache.flink.api.common.state.StateTtlConfig;
StateTtlConfig ttlConfig = StateTtlConfig
    .newBuilder(Time.seconds(1))
    .cleanupInBackground()
    .build();
```
