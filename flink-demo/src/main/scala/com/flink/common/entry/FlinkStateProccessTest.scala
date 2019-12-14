package com.flink.common.entry

import org.apache.flink.api.common.state.ValueState
import org.apache.flink.api.common.typeinfo.{TypeHint, TypeInformation}
import org.apache.flink.api.java.ExecutionEnvironment
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.configuration.Configuration
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend
import org.apache.flink.state.api.Savepoint
import org.apache.flink.state.api.functions.KeyedStateReaderFunction
import org.apache.flink.util.Collector

object FlinkStateProccessTest {
  case class Wordcount(w: java.lang.String, var c: java.lang.Long)
  def main(args: Array[String]): Unit = {
    val path =
      "file:///Users/eminem/workspace/flink/flink-learn/checkpoint"
    val savp = "file:///Users/eminem/workspace/flink/flink-learn/savepoint/savepoint-ad2e7e-40210d5f0e13"
    val bEnv = ExecutionEnvironment.getExecutionEnvironment
    val existSp = Savepoint.load(bEnv, savp, new RocksDBStateBackend(path))

    existSp
      .readKeyedState(
        "StatisticalIndic",
        new Keyreader,
        Types.STRING,
        Types.LONG
      )
      .print()
  }

  class Keyreader extends KeyedStateReaderFunction[java.lang.String, java.lang.Long] {
    var state: ValueState[java.lang.Long] = _;

    override def open(parameters: Configuration): Unit = {
      import org.apache.flink.api.common.state.ValueStateDescriptor
      val stateDescriptor =
        new ValueStateDescriptor[java.lang.Long]("state", Types.LONG)
      state = getRuntimeContext.getState(stateDescriptor)
    }
    override def readKey(key: java.lang.String,
                         context: KeyedStateReaderFunction.Context,
                         collector: Collector[java.lang.Long]): Unit = {
      val data = Wordcount(key, state.value())
      collector.collect(state.value());
    }
  }
}
