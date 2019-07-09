package com.flink.common

import com.flink.common.param.EnvironmentalKey
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend
package object entry extends EnvironmentalKey {

  def getFlinkEnv() = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(3)
    env.enableCheckpointing(6000) //更新offsets。每60s提交一次
    
    env.setStateBackend((new RocksDBStateBackend("file:///C:\\Users\\Administrator\\Desktop\\checkpoint")))
    env
  }
}