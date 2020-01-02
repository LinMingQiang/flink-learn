package com.flink.learn.entry

import com.flink.common.core.FlinkEvnBuilder
import com.flink.learn.param.PropertiesUtil
import org.apache.flink.streaming.api.TimeCharacteristic

object FlinkEventtimeWatermarkTest {

  def main(args: Array[String]): Unit = {
    val env = FlinkEvnBuilder.buildFlinkEnv(PropertiesUtil.param, cp, 60000) // 1 min
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime) // 时间设为eventime
    val source = env.socketTextStream("localhost", 9876)

  }
}
