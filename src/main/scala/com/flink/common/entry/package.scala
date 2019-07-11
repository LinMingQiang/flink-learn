package com.flink.common

import com.flink.common.param.EnvironmentalKey
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup

package object entry extends EnvironmentalKey {

  def getFlinkEnv(checkpointPath :String) = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    env.enableCheckpointing(6000) //更新offsets。每60s提交一次
    //超时
    //env.getCheckpointConfig.setCheckpointTimeout(5000)
    // 同一时间只允许进行一个检查点
    env.getCheckpointConfig.setMaxConcurrentCheckpoints(1);
    // 表示一旦Flink处理程序被cancel后，会保留Checkpoint数据，以便根据实际需要恢复到指定的Checkpoint
    env.getCheckpointConfig.enableExternalizedCheckpoints(ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
    //env.setStateBackend(new FsStateBackend(("file:///C:\\Users\\Master\\Desktop\\fscheckpoint")))
    env.setStateBackend((new RocksDBStateBackend(checkpointPath)))
    env
  }
}