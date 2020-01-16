package com.flink.learn.reader

import java.util.Date

import com.flink.learn.bean.CaseClassUtil.Wordcount
import com.flink.learn.bean.CaseClassUtil.WordCountScalaPoJo
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.state.api.functions.KeyedStateReaderFunction
import org.apache.flink.streaming.api.scala.createTypeInformation
import org.apache.flink.util.Collector

class WordCounScalaKeyreader(stateName: String)
    extends KeyedStateReaderFunction[String, WordCountScalaPoJo] {
  var lastState: ValueState[Wordcount] = _;
  override def open(parameters: Configuration): Unit = {
    val desc = new ValueStateDescriptor[Wordcount](
      stateName,
      createTypeInformation[Wordcount])
    lastState = getRuntimeContext().getState(desc)
  }
  override def readKey(key: String,
                       context: KeyedStateReaderFunction.Context,
                       collector: Collector[WordCountScalaPoJo]): Unit = {
    val v = lastState.value()
    collector.collect(
      WordCountScalaPoJo(v.w, v.c, new Date(v.timestamp).toString))
  }
}
