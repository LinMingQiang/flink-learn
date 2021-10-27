package com.flink.sql.env;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.io.IOException;
import java.time.Duration;

public class FlinkEvnBuilder {
    public static StreamExecutionEnvironment streamEnv = null;
    public static StreamTableEnvironment tableEnv = null;

    public static void initEnv(ParameterTool parameters,
                               String checkpointPath,
                               Long checkPointInterval,
                               Duration stateTTL) throws IOException {
        streamEnv = FlinkEvnBuilder.buildStreamingEnv(parameters, checkpointPath,
                checkPointInterval);
        tableEnv = FlinkEvnBuilder.buildStreamTableEnv(
                streamEnv,
                stateTTL);
    }

    /**
     * @param parameters
     * @param checkpointPath
     * @param checkPointInterval
     * @return
     * @throws IOException
     */
    public static StreamExecutionEnvironment buildStreamingEnv(
            ParameterTool parameters,
            String checkpointPath,
            Long checkPointInterval) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setGlobalJobParameters(parameters); // 广播配置
        if (checkPointInterval > 0L) {
            env.enableCheckpointing(checkPointInterval); //更新offsets。每60s提交一次
            env.getCheckpointConfig().setMinPauseBetweenCheckpoints(checkPointInterval); //; 两个chk最小间隔
        }
        env.getConfig().setAutoWatermarkInterval(20L); // 设置 触发水位计算 间隔
        // 同一时间只允许进行一个检查点
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        // 不设置的话，任务cancle后会删除ckp
        env.getCheckpointConfig().enableExternalizedCheckpoints(
                CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        // 这两个一起用
        env.getCheckpointConfig().setCheckpointStorage(checkpointPath);
        // enableIncrementalCheckpointing设置为ture需要注意，checkpoint可能会比较大，或者一直增大，但是savepoint却小（实际也应该小）
        EmbeddedRocksDBStateBackend backend = new EmbeddedRocksDBStateBackend(false);

        // state.backend.rocksdb.ttl.compaction.filter.enabled ：这个配置现在无效了，需要在richfunc里面设置ttlconf或者sql的ttl
        // 说是存储在hdfs，看代码好像不支持 hdfs // rocksdb本地路径，默认在tm临时路径下
        // rocksDBStateBackend.setDbStoragePath(checkpointPath + "/rocksdbstorage");
        env.setStateBackend(backend);
        return env;
    }

    public static StreamTableEnvironment buildStreamTableEnv(StreamExecutionEnvironment streamEnv,
                                                             Duration stateTTL) throws IOException {
        EnvironmentSettings sett =
                EnvironmentSettings.newInstance().useBlinkPlanner().build();
        StreamTableEnvironment streamTableEnv = StreamTableEnvironment.create(streamEnv, sett);
        streamTableEnv.getConfig().setIdleStateRetention(stateTTL);
        return streamTableEnv;
    }
}
