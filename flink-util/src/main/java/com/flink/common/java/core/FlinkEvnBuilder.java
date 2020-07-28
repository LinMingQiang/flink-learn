package com.flink.common.java.core;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.java.StreamTableEnvironment;

import java.io.IOException;

public class FlinkEvnBuilder {

    /**
     *
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
        env.getConfig().setAutoWatermarkInterval(10000L); // 每10s触发一次 wtm
        env.enableCheckpointing(checkPointInterval); //更新offsets。每60s提交一次
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(checkPointInterval); //; 两个chk最小间隔

        //超时
        //env.getCheckpointConfig.setCheckpointTimeout(5000) // 默认10min
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.getConfig().setAutoWatermarkInterval(5000L); // 设置 触发水位计算 间隔

        // 同一时间只允许进行一个检查点
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        // 表示一旦Flink处理程序被cancel后，会保留Checkpoint数据，以便根据实际需要恢复到指定的Checkpoint
        env.getCheckpointConfig().enableExternalizedCheckpoints(
                CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        //env.setStateBackend(new FsStateBackend(checkpointPath))
        RocksDBStateBackend rocksDBStateBackend = null;
        try {
            rocksDBStateBackend = new RocksDBStateBackend(checkpointPath, true);
        } catch (IOException e) {
            e.printStackTrace();
        }
        // rocksDBStateBackend.setDbStoragePath("") // rocksdb本地路径，默认在tm临时路径下
        rocksDBStateBackend.enableTtlCompactionFilter(); // 启用ttl后台增量清除功能
        // println(rocksDBStateBackend.isIncrementalCheckpointsEnabled)
        // println(rocksDBStateBackend.isTtlCompactionFilterEnabled)
        // state.backend.rocksdb.ttl.compaction.filter.enabled
        // 说是存储在hdfs，看代码好像不支持 hdfs
        // rocksDBStateBackend.setDbStoragePath(checkpointPath + "/rocksdbstorage")
        env.setStateBackend(rocksDBStateBackend);
        return env;
    }


    /**
     *
     * @param parameters
     * @param checkpointPath
     * @param checkPointInterval
     * @param stateMinT
     * @param stateMaxT
     * @return
     * @throws IOException
     */
    public static StreamTableEnvironment buildStreamTableEnv(ParameterTool parameters,
                                                             String checkpointPath,
                                                             Long checkPointInterval,
                                                             Time stateMinT,
                                                             Time stateMaxT) throws IOException {
        StreamExecutionEnvironment streamEnv = buildStreamingEnv(parameters, checkpointPath, checkPointInterval);
        EnvironmentSettings sett =
                EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment streamTableEnv = StreamTableEnvironment.create(streamEnv, sett);
        streamTableEnv.getConfig()
                .setIdleStateRetentionTime(stateMinT, stateMaxT);
        return streamTableEnv;
    }


    public static StreamTableEnvironment buildStreamTableEnv(StreamExecutionEnvironment streamEnv ,
                                                             Time stateMinT,
                                                             Time stateMaxT) throws IOException {
        EnvironmentSettings sett =
                EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment streamTableEnv = StreamTableEnvironment.create(streamEnv, sett);
        streamTableEnv.getConfig()
                .setIdleStateRetentionTime(stateMinT, stateMaxT);
        return streamTableEnv;
    }
}
