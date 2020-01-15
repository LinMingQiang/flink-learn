package com.flink.learn.state.processor.entry

import com.flink.learn.bean.CaseClassUtil.Wordcount
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.state.api.functions.KeyedStateReaderFunction
import org.apache.flink.streaming.api.scala.createTypeInformation
import org.apache.flink.util.Collector

class WordCountKeyreader(stateName : String) extends KeyedStateReaderFunction[String, Wordcount] {
  var lastState: ValueState[Wordcount] = _;
  override def open(parameters: Configuration): Unit = {
    val desc = new ValueStateDescriptor[Wordcount](
      stateName,
      createTypeInformation[Wordcount])
    lastState = getRuntimeContext().getState(desc)
  }
  override def readKey(key: String,
                       context: KeyedStateReaderFunction.Context,
                       collector: Collector[Wordcount]): Unit = {
    collector.collect(lastState.value())
  }
}
