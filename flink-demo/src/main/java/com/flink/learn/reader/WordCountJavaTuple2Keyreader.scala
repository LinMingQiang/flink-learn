package com.flink.learn.reader

import com.flink.learn.bean.{TranWordCountPoJo, WordCountPoJo}
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.state.api.functions.KeyedStateReaderFunction
import org.apache.flink.streaming.api.scala.createTypeInformation
import org.apache.flink.util.Collector
import org.apache.flink.api.java.tuple.Tuple2
class WordCountJavaTuple2Keyreader(stateName: String)
    extends KeyedStateReaderFunction[
      Tuple2[String, String],
      WordCountPoJo] {
  var lastState: ValueState[WordCountPoJo] = _
  override def open(parameters: Configuration): Unit = {
    val desc = new ValueStateDescriptor[WordCountPoJo](
      stateName,
      createTypeInformation[WordCountPoJo])
    lastState = getRuntimeContext().getState(desc)
  }

  override def readKey(
      key: Tuple2[String, String],
      context: KeyedStateReaderFunction.Context,
      collector: Collector[WordCountPoJo]): Unit = {
    val v = lastState.value()
    collector.collect(v)
  }
}
