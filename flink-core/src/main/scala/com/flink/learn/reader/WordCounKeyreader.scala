package com.flink.learn.reader

import java.util.Date

import com.flink.learn.bean.CaseClassUtil.Wordcount
import com.flink.learn.bean.CaseClassUtil.TransWordCount
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.configuration.Configuration
import org.apache.flink.state.api.functions.KeyedStateReaderFunction
import org.apache.flink.streaming.api.scala.createTypeInformation
import org.apache.flink.util.Collector

class WordCounKeyreader(stateName: String)
    extends KeyedStateReaderFunction[String, TransWordCount] {
  // 状态
  var lastState: ValueState[Wordcount] = _;

  /**
    *
    * @param parameters
    */
  override def open(parameters: Configuration): Unit = {
    val desc = new ValueStateDescriptor[Wordcount](
      stateName,
      createTypeInformation[Wordcount])
    lastState = getRuntimeContext().getState(desc)
  }

  /**
    *
    * @param key
    * @param context
    * @param collector
    */
  override def readKey(key: String,
                       context: KeyedStateReaderFunction.Context,
                       collector: Collector[TransWordCount]): Unit = {
    val v = lastState.value()
    val r = TransWordCount(v.word, v.count, v.timestamp)
    collector.collect(r)
  }
}
