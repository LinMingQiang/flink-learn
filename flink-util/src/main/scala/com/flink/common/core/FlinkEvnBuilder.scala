package com.flink.common.core

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend
import org.apache.flink.runtime.state.StateBackend
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

object FlinkEvnBuilder {

  /**
    * @desc 获取env
    * @param checkpointPath
    * @return
    */
  def buildFlinkEnv(
      parameters: ParameterTool,
      checkpointPath: String,
      checkPointInterval: Long = 6000): StreamExecutionEnvironment = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.getConfig.setGlobalJobParameters(parameters) // 广播配置
    env.enableCheckpointing(checkPointInterval) //更新offsets。每60s提交一次
    //超时
    //env.getCheckpointConfig.setCheckpointTimeout(5000)
    // 同一时间只允许进行一个检查点
    env.getCheckpointConfig.setMaxConcurrentCheckpoints(1);
    // 表示一旦Flink处理程序被cancel后，会保留Checkpoint数据，以便根据实际需要恢复到指定的Checkpoint
    env.getCheckpointConfig.enableExternalizedCheckpoints(
      ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
    //env.setStateBackend(new FsStateBackend(checkpointPath))
    val rocksDBStateBackend = new RocksDBStateBackend(checkpointPath)
    rocksDBStateBackend.enableTtlCompactionFilter() // 启用ttl后台增量清除功能
    // state.backend.rocksdb.ttl.compaction.filter.enabled
    // 说是存储在hdfs，看代码好像不支持 hdfs
    // rocksDBStateBackend.setDbStoragePath(checkpointPath + "/rocksdbstorage")
    env.setStateBackend(rocksDBStateBackend)
    env
  }
}
