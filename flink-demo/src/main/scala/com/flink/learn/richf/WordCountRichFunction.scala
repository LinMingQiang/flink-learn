package com.flink.learn.richf
import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.runtime.state.{FunctionInitializationContext, FunctionSnapshotContext}
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction
import org.apache.flink.util.Collector

class WordCountRichFunction
    extends RichFlatMapFunction[(String, Int), (String, Int)] with CheckpointedFunction{
   var lastState: ValueState[Int] = _
  // 在这里定义的变量，一个task公用一个，lastState也是一个，但是它是类似client的东西，根据key去拿value
  override def flatMap(value: (String, Int),
                       out: Collector[(String, Int)]): Unit = {
    val ls = lastState.value()
    val nCount = ls + value._2
    lastState.update(nCount)
    out.collect((value._1, nCount))
  }
  override def open(parameters: Configuration): Unit = {
    val desc = new ValueStateDescriptor("count", classOf[Int], 0)
    lastState = getRuntimeContext().getState(desc)
  }

  override def snapshotState(context: FunctionSnapshotContext): Unit = {
    // println(">>>> " , lastState.value())

  }

  override def initializeState(context: FunctionInitializationContext): Unit = {
    // println("<<<<<<<<<")
  }
}
