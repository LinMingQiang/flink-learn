package com.flink.learn.entry

import com.flink.common.core.FlinkEvnBuilder
import com.flink.learn.entry.FlinkOperatorStateTest.checkpointPath
import com.flink.learn.param.PropertiesUtil

object DataSetTest {
  def main(args: Array[String]): Unit = {
    val env = FlinkEvnBuilder.buildEnv(checkpointPath) // 1 min




  }
}
