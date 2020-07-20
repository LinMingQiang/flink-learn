package com.flink.common.core

import java.util.Date

import org.apache.commons.lang3.time.DateFormatUtils
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment}

object FlinkEvnBuilder {

  /**
    * @desc 获取env
    * @param checkpointPath
    * @return
    */
  def buildStreamTableEnv(
      parameters: ParameterTool,
      checkpointPath: String,
      checkPointInterval: Long = 6000): StreamExecutionEnvironment = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.getConfig.setGlobalJobParameters(parameters)
    env.enableCheckpointing(checkPointInterval)
    env.getCheckpointConfig.setMinPauseBetweenCheckpoints(checkPointInterval) // 两个chk最小间隔
    env.getCheckpointConfig.setCheckpointTimeout(1200000) // 默认10min
    env.getCheckpointConfig.setMaxConcurrentCheckpoints(1); // 同一时间只允许进行一个检查点
    env.getCheckpointConfig.enableExternalizedCheckpoints(
      ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
    val rocksDBStateBackend =
      new RocksDBStateBackend(
        s"$checkpointPath/${DateFormatUtils.format(new Date(), "yyyyMMddMM")}",
        true)
    rocksDBStateBackend.enableTtlCompactionFilter() // 启用ttl后台增量清除功能
    env.setStateBackend(rocksDBStateBackend)
    env
  }
}
