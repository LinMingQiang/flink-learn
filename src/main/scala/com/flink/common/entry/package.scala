package com.flink.common

import com.flink.common.param.EnvironmentalKey
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup

package object entry extends EnvironmentalKey {

  def getFlinkEnv() = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.enableCheckpointing(6000) //更新offsets。每60s提交一次
    env.getCheckpointConfig.enableExternalizedCheckpoints(ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)
    env.setStateBackend((new RocksDBStateBackend("file:///C:\\Users\\Administrator\\Desktop\\checkpoint")))
    env
  }
}